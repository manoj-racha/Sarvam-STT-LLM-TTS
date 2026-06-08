import {
  Component, OnInit, OnDestroy, ChangeDetectionStrategy, ChangeDetectorRef
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { Subscription } from 'rxjs';
import { VoicePipelineService } from '../../services/voice-pipeline.service';
import { PipelineState, PipelineMetrics, ConversationTurn } from '../../models/voice.models';

@Component({
  selector: 'app-voice-agent',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './voice-agent.component.html',
  styleUrls: ['./voice-agent.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class VoiceAgentComponent implements OnInit, OnDestroy {
  state: PipelineState = 'idle';
  transcript = '';
  assistantText = '';
  metrics: PipelineMetrics = {
    vadLatency: null, sttLatency: null,
    llmTtft: null, ttsFirstByte: null, totalLatency: null,
  };
  history: ConversationTurn[] = [];
  error = '';
  isStarted = false;

  private subs = new Subscription();

  constructor(
    public pipeline: VoicePipelineService,
    private cdr: ChangeDetectorRef,
  ) {}

  ngOnInit(): void {
    this.subs.add(
      this.pipeline.state$.subscribe(s => {
        this.state = s;
        this.cdr.markForCheck();
      })
    );
    this.subs.add(
      this.pipeline.transcript$.subscribe(t => {
        this.transcript = t;
        this.cdr.markForCheck();
      })
    );
    this.subs.add(
      this.pipeline.assistantText$.subscribe(t => {
        this.assistantText = t;
        this.cdr.markForCheck();
      })
    );
    this.subs.add(
      this.pipeline.metrics$.subscribe(m => {
        this.metrics = m;
        this.cdr.markForCheck();
      })
    );
    this.subs.add(
      this.pipeline.conversationHistory$.subscribe(h => {
        this.history = h;
        this.cdr.markForCheck();
      })
    );
    this.subs.add(
      this.pipeline.error$.subscribe(e => {
        this.error = e;
        setTimeout(() => { this.error = ''; this.cdr.markForCheck(); }, 4000);
        this.cdr.markForCheck();
      })
    );
  }

  async startSession(): Promise<void> {
    try {
      await this.pipeline.initialize();
      this.isStarted = true;
      this.cdr.markForCheck();
    } catch (err: any) {
      this.error = 'Microphone access denied. Please allow mic permissions.';
      this.cdr.markForCheck();
    }
  }

  resetConversation(): void {
    this.pipeline.resetConversation();
    this.isStarted = false;
    this.cdr.markForCheck();
  }

  getStateLabel(): string {
    const labels: Record<PipelineState, string> = {
      idle: 'Ready',
      listening: 'Listening…',
      processing: 'Thinking…',
      speaking: 'Speaking…',
    };
    return labels[this.state];
  }

  formatMs(val: number | null): string {
    return val != null ? `${Math.round(val)}ms` : '—';
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }
}
