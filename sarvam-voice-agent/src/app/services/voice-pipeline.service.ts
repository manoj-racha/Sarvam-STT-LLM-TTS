import { Injectable, NgZone, OnDestroy } from '@angular/core';
import { BehaviorSubject, Subject, Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { VadService } from './vad.service';
import { AudioRecorderService } from './audio-recorder.service';
import { SarvamSttService } from './sarvam-stt.service';
import { SarvamLlmService } from './sarvam-llm.service';
import { SarvamTtsService } from './sarvam-tts.service';
import {
  PipelineState,
  PipelineMetrics,
  ConversationTurn,
} from '../models/voice.models';

@Injectable({ providedIn: 'root' })
export class VoicePipelineService implements OnDestroy {

  // Public state streams
  readonly state$ = new BehaviorSubject<PipelineState>('idle');
  readonly transcript$ = new BehaviorSubject<string>('');
  readonly assistantText$ = new BehaviorSubject<string>('');
  readonly metrics$ = new BehaviorSubject<PipelineMetrics>({
    vadLatency: null, sttLatency: null,
    llmTtft: null, ttsFirstByte: null, totalLatency: null,
  });
  readonly conversationHistory$ = new BehaviorSubject<ConversationTurn[]>([]);
  readonly error$ = new Subject<string>();

  // Internal timing
  private speechStartTime = 0;
  private sttStartTime = 0;
  private llmStartTime = 0;
  private ttsRequestTime = 0;

  // LLM abort controller for barge-in cancellation
  private llmAbortController: AbortController | null = null;

  // Current accumulated assistant response
  private currentAssistantText = '';
  private currentSentenceIndex = 0;

  private subscriptions = new Subscription();
  private initialized = false;

  constructor(
    private vad: VadService,
    private recorder: AudioRecorderService,
    private stt: SarvamSttService,
    private llm: SarvamLlmService,
    private tts: SarvamTtsService,
    private ngZone: NgZone,
  ) {}

  async initialize(): Promise<void> {
    if (this.initialized) return;
    this.initialized = true;

    await this.vad.initialize();
    this.tts.initAudioContext();
    this.wireVADEvents();
    this.wireTTSEvents();
  }

  private wireVADEvents(): void {
    const vadSub = this.vad.vadEvent$.subscribe(event => {
      if (event.type === 'speech_start') {
        this.onSpeechStart(event.timestamp);
      } else if (event.type === 'speech_end') {
        this.onSpeechEnd(event.timestamp);
      }
    });
    this.subscriptions.add(vadSub);
  }

  private wireTTSEvents(): void {
    const ttsSub = this.tts.ttsEvent$.subscribe(event => {
      if (event.type === 'started') {
        // TTS audio is now playing through speakers — raise VAD threshold so the
        // microphone picking up speaker output doesn't trigger a false barge-in.
        this.vad.setTtsSuppression(true);
      }

      if (event.type === 'finished') {
        // TTS done — restore normal VAD sensitivity for user's next utterance.
        this.vad.setTtsSuppression(false);
        if (this.state$.value === 'speaking') {
          this.finalizeAssistantTurn();
          this.transitionTo('listening');
        }
      }

      if (event.type === 'cancelled') {
        // TTS was cancelled (barge-in) — restore normal VAD sensitivity.
        this.vad.setTtsSuppression(false);
      }
    });
    this.subscriptions.add(ttsSub);
  }

  // ── STEP 1: Speech detected ───────────────────────────────────────────
  private onSpeechStart(timestamp: number): void {
    this.speechStartTime = timestamp;

    // BARGE-IN: user spoke while AI is speaking → interrupt everything
    if (this.state$.value === 'speaking') {
      this.handleBargeIn();
    }

    if (this.state$.value === 'listening' || this.state$.value === 'idle') {
      this.transitionTo('listening');
      const stream   = this.vad.getMediaStream();
      const audioCtx = this.vad.getAudioContext();
      if (stream && audioCtx) {
        // startRecording is async (loads AudioWorklet on first call)
        void this.recorder.startRecording(stream, audioCtx);
      }
    }
  }

  // ── STEP 2: Silence detected → end of user turn ───────────────────────
  private onSpeechEnd(timestamp: number): void {
    if (this.state$.value !== 'listening') return;
    if (!this.recorder.isRecording()) return;

    const vadLatency = timestamp - this.speechStartTime;
    this.updateMetrics({ vadLatency });

    this.transitionTo('processing');
    this.sttStartTime = Date.now();

    // Get the recorded audio blob then run STT
    const completeSub = this.recorder.recordingComplete$
      .pipe(filter(blob => blob.size > 0))
      .subscribe(blob => {
        completeSub.unsubscribe();
        this.runSTT(blob);
      });

    this.recorder.stopRecording();
  }

  // ── STEP 3: STT ──────────────────────────────────────────────────────
  private runSTT(audioBlob: Blob): void {
    this.stt.transcribe(audioBlob).subscribe({
      next: (result) => {
        const sttLatency = Date.now() - this.sttStartTime;
        this.updateMetrics({ sttLatency });

        const transcript = result.transcript?.trim();
        if (!transcript) {
          this.transitionTo('listening');
          return;
        }

        this.transcript$.next(transcript);
        this.llmStartTime = Date.now();
        this.runLLM(transcript);
      },
      error: (err) => {
        this.error$.next(`STT failed: ${err.message}`);
        this.transitionTo('listening');
      }
    });
  }

  // ── STEP 4: LLM streaming with sentence dispatch ───────────────────────
  private runLLM(userText: string): void {
    this.currentAssistantText = '';
    this.currentSentenceIndex = 0;
    this.assistantText$.next('');
    this.tts.reset();

    this.llmAbortController = new AbortController();
    let llmFirstToken = true;

    const llmSub = this.llm.streamResponse(
      this.conversationHistory$.value,
      userText,
      this.llmAbortController.signal
    ).subscribe({
      next: (event) => {
        if (event.type === 'token') {
          if (llmFirstToken) {
            const llmTtft = Date.now() - this.llmStartTime;
            this.updateMetrics({ llmTtft });
            llmFirstToken = false;
            this.transitionTo('speaking');
          }
          this.currentAssistantText += event.content ?? '';
          this.assistantText$.next(this.currentAssistantText);
        }

        // KEY OPTIMIZATION: dispatch each sentence to TTS immediately
        if (event.type === 'sentence' && event.sentence) {
          const { text, index } = event.sentence;
          this.ttsRequestTime = Date.now();
          this.dispatchToTTS(text, index);
        }

        if (event.type === 'done') {
          // Add user turn to history
          this.addToHistory('user', userText);
        }

        if (event.type === 'error') {
          this.error$.next(`LLM failed: ${event.error}`);
          this.transitionTo('listening');
        }
      },
      error: (err) => {
        this.error$.next(`LLM error: ${err.message}`);
        this.transitionTo('listening');
      }
    });

    this.subscriptions.add(llmSub);
  }

  // ── STEP 5: TTS per sentence ───────────────────────────────────────────
  private async dispatchToTTS(sentence: string, chunkIndex: number): Promise<void> {
    if (this.llmAbortController?.signal.aborted) return;

    await this.tts.synthesizeAndEnqueue(sentence, chunkIndex);

    if (chunkIndex === 0) {
      const ttsFirstByte = Date.now() - this.ttsRequestTime;
      const totalLatency = Date.now() - this.speechStartTime;
      this.updateMetrics({ ttsFirstByte, totalLatency });
    }
  }

  // ── BARGE-IN ──────────────────────────────────────────────────────────
  private handleBargeIn(): void {
    // 1. Restore normal VAD sensitivity immediately so the incoming user
    //    utterance is captured at the normal threshold.
    this.vad.setTtsSuppression(false);

    // 2. Abort LLM stream
    this.llmAbortController?.abort();
    this.llmAbortController = null;

    // 3. Stop all TTS audio immediately
    this.tts.cancelAll();

    // 4. Save whatever was spoken as assistant turn
    if (this.currentAssistantText.trim()) {
      this.finalizeAssistantTurn();
    }

    // 5. Start listening for the new user utterance
    this.transitionTo('listening');
  }

  private finalizeAssistantTurn(): void {
    if (this.currentAssistantText.trim()) {
      this.addToHistory('assistant', this.currentAssistantText.trim());
      this.currentAssistantText = '';
    }
  }

  private addToHistory(role: 'user' | 'assistant', text: string): void {
    const turn: ConversationTurn = {
      id: crypto.randomUUID(),
      role,
      text,
      timestamp: new Date(),
      metrics: role === 'assistant' ? { ...this.metrics$.value } : undefined,
    };
    this.conversationHistory$.next([...this.conversationHistory$.value, turn]);
  }

  private transitionTo(state: PipelineState): void {
    this.ngZone.run(() => {
      this.state$.next(state);
    });
  }

  private updateMetrics(partial: Partial<PipelineMetrics>): void {
    this.ngZone.run(() => {
      this.metrics$.next({ ...this.metrics$.value, ...partial });
    });
  }

  resetConversation(): void {
    this.llmAbortController?.abort();
    this.tts.cancelAll();
    this.tts.reset();
    this.conversationHistory$.next([]);
    this.transcript$.next('');
    this.assistantText$.next('');
    this.metrics$.next({
      vadLatency: null, sttLatency: null,
      llmTtft: null, ttsFirstByte: null, totalLatency: null,
    });
    this.transitionTo('idle');
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
    this.vad.destroy();
  }
}
