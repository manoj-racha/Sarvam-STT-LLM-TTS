import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';
import { ConversationTurn, SentenceChunk } from '../models/voice.models';

export interface LLMStreamEvent {
  type: 'token' | 'sentence' | 'done' | 'error';
  content?: string;
  sentence?: SentenceChunk;
  error?: string;
}

@Injectable({ providedIn: 'root' })
export class SarvamLlmService {
  // Point to your backend proxy
  private readonly LLM_ENDPOINT = '/api/sarvam/llm/stream';

  /**
   * Sentence boundary regex — handles Hindi, English, and mixed text.
   *
   * Latency optimisation: also split on comma-clauses that are ≥ 30 chars long.
   * This lets TTS start on clause-length chunks rather than waiting for a full
   * sentence to complete, shaving hundreds of ms off perceived TTFT on long answers.
   *
   * Priority order (regex alternation):
   *   1. Hard sentence endings:  ।  .  !  ?  …  (with optional trailing whitespace)
   *   2. Soft clause boundary:   ,  followed by a space (dispatch when buffer ≥ 30 chars)
   */
  private readonly SENTENCE_REGEX = /[।.!?…]+[\s\n]*|,\s/;

  /**
   * Minimum buffer length before a comma-split is considered a valid clause.
   * Prevents dispatching tiny fragments like "Yes," or "Ok,".
   */
  private readonly MIN_CLAUSE_CHARS = 30;

  /**
   * Streams LLM tokens and emits complete sentences/clauses for immediate TTS dispatch.
   * This is the core latency optimisation: first sentence → TTS before full response.
   *
   * Latency settings applied:
   *   - max_tokens: 256 → 128  (voice needs shorter answers; model stops sooner)
   *   - temperature: 0.7 → 0.3 (more decisive; reaches punctuation boundaries faster)
   *   - reasoning_effort: null  (disabled; no chain-of-thought overhead)
   *   - console.log removed from the per-token hot path
   */
  streamResponse(
    history: ConversationTurn[],
    userMessage: string,
    abortSignal: AbortSignal
  ): Observable<LLMStreamEvent> {
    return new Observable(observer => {
      let tokenBuffer = '';
      let streamBuffer = '';
      let sentenceIndex = 0;
      let fullResponse = '';

      const messages = [
        {
          role: 'system',
          content: `You are a helpful, concise voice assistant fluent in Indian languages and English. \
Keep responses short and conversational — ideally 1-3 sentences. \
Avoid markdown, bullet points, or formatting. Speak naturally.`
        },
        ...history.map(turn => ({
          role: turn.role,
          content: turn.text
        })),
        { role: 'user', content: userMessage }
      ];

      fetch(this.LLM_ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model:            'sarvam-30b',
          messages,
          stream:           true,
          max_tokens:       128,    // ↓ from 256 — voice needs short answers
          temperature:      0.3,    // ↓ from 0.7 — more decisive, faster to punctuation
          reasoning_effort: null,   // disabled — no chain-of-thought overhead
        }),
        signal: abortSignal
      }).then(async response => {
        if (!response.ok) {
          console.error('LLM | Fetch failed:', response.status, response.statusText);
          observer.next({ type: 'error', error: `HTTP ${response.status}` });
          observer.complete();
          return;
        }

        const reader = response.body!.getReader();
        const decoder = new TextDecoder();

        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          if (abortSignal.aborted) break;

          streamBuffer += decoder.decode(value, { stream: true });
          const lines = streamBuffer.split('\n');
          // Last element is either empty (if ended with \n) or an incomplete line
          streamBuffer = lines.pop() || '';

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            const data = line.slice(6).trim();
            if (data === '[DONE]') {
              console.log('LLM | Stream complete');
              break;
            }

            try {
              const parsed = JSON.parse(data);
              const token = parsed.choices?.[0]?.delta?.content;
              if (!token) continue;

              tokenBuffer  += token;
              fullResponse += token;
              observer.next({ type: 'token', content: token });

              // ── Sentence / clause dispatch ───────────────────────────────
              // Try to find the earliest split point in the buffer.
              // For comma splits, only dispatch when the buffer is long enough
              // to constitute a meaningful clause (avoids micro-fragments).
              const match = this.SENTENCE_REGEX.exec(tokenBuffer);
              if (match) {
                const isCommaSplit = match[0].startsWith(',');
                const sentenceEnd  = match.index + match[0].length;

                // Skip comma-split if the clause so far is too short
                if (isCommaSplit && match.index < this.MIN_CLAUSE_CHARS) {
                  continue;
                }

                const sentence = tokenBuffer.slice(0, sentenceEnd).trim();
                tokenBuffer    = tokenBuffer.slice(sentenceEnd);

                if (sentence.length > 3) {
                  observer.next({
                    type: 'sentence',
                    sentence: { text: sentence, index: sentenceIndex++, isFinal: false }
                  });
                }
              }
              // ────────────────────────────────────────────────────────────
            } catch {
              // Skip malformed SSE lines
            }
          }
        }

        // Flush remaining buffer as the final sentence
        const remaining = tokenBuffer.trim();
        if (remaining.length > 3) {
          observer.next({
            type: 'sentence',
            sentence: { text: remaining, index: sentenceIndex++, isFinal: true }
          });
        }

        observer.next({ type: 'done', content: fullResponse });
        observer.complete();

      }).catch(err => {
        if (err.name !== 'AbortError') {
          console.error('LLM | Fetch error:', err.message);
          observer.next({ type: 'error', error: err.message });
        }
        observer.complete();
      });
    });
  }
}
