export type PipelineState = 'idle' | 'listening' | 'processing' | 'speaking';

export interface PipelineMetrics {
  vadLatency: number | null;
  sttLatency: number | null;
  llmTtft: number | null;      // time-to-first-token
  ttsFirstByte: number | null;
  totalLatency: number | null;
}

export interface ConversationTurn {
  id: string;
  role: 'user' | 'assistant';
  text: string;
  timestamp: Date;
  metrics?: PipelineMetrics;
}

export interface SarvamSTTResponse {
  transcript: string;
  language_code?: string;
}

export interface SarvamTTSRequest {
  text: string;
  target_language_code: string;
  speaker: string;
  model: string;
  response_format?: string;
  pace?: number;
  speech_sample_rate?: number;
}

export interface SentenceChunk {
  text: string;
  index: number;
  isFinal: boolean;
}

export interface AudioChunk {
  data: ArrayBuffer;
  chunkIndex: number;
}
