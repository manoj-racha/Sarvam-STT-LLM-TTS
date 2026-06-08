import { Injectable, NgZone } from '@angular/core';
import { Subject, BehaviorSubject } from 'rxjs';

export interface VADEvent {
  type: 'speech_start' | 'speech_end' | 'speaking';
  timestamp: number;
  energy?: number;
}

@Injectable({ providedIn: 'root' })
export class VadService {
  private audioContext: AudioContext | null = null;
  private analyser: AnalyserNode | null = null;
  private mediaStream: MediaStream | null = null;
  private animationFrameId: number | null = null;
  private workletNode: AudioWorkletNode | null = null;
  private sourceNode: MediaStreamAudioSourceNode | null = null;

  // VAD state
  private isSpeaking = false;
  private silenceStart: number | null = null;
  private speechStart: number | null = null;

  // ── Tunable thresholds ──────────────────────────────────────────────────
  private readonly ENERGY_THRESHOLD        = 0.015;  // RMS threshold during listening
  private readonly BARGE_IN_THRESHOLD      = 0.08;   // Much higher threshold during TTS playback.
                                                      // Prevents speaker echo from triggering a
                                                      // false barge-in (the #1 cause of premature
                                                      // TTS cutoff).  Only a loud intentional voice
                                                      // exceeds 0.08 in normal room conditions.
  private readonly SPEECH_START_FRAMES     = 3;       // consecutive frames above threshold → speech_start
  private readonly SILENCE_DURATION_MS     = 600;     // ms of silence before end-of-turn
  private consecutiveSpeechFrames          = 0;

  /**
   * When true the VAD uses BARGE_IN_THRESHOLD instead of ENERGY_THRESHOLD.
   * Set to true while TTS is playing so speaker echo cannot self-trigger barge-in.
   * Set to false when TTS finishes / is cancelled and we return to listening.
   */
  private ttsSuppressed = false;

  readonly vadEvent$   = new Subject<VADEvent>();
  readonly isSpeaking$ = new BehaviorSubject<boolean>(false);

  constructor(private ngZone: NgZone) {}

  /**
   * Call with `true` when TTS starts playing and `false` when it ends/cancels.
   * Raises the VAD threshold so speaker output doesn't trigger a false speech_start.
   * Real barge-in still works — the user just needs to speak more loudly than the speaker.
   */
  setTtsSuppression(active: boolean): void {
    this.ttsSuppressed = active;
    if (!active) {
      // Reset frame counter so normal listening resumes cleanly
      this.consecutiveSpeechFrames = 0;
      this.isSpeaking               = false;
      this.silenceStart             = null;
    }
  }

  async initialize(): Promise<void> {
    this.audioContext = new AudioContext();

    this.mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount:     1,
        sampleRate:       16000,
        echoCancellation: true,   // browser-level AEC helps, but isn't perfect
        noiseSuppression: true,
        autoGainControl:  true,
      }
    });

    this.sourceNode = this.audioContext.createMediaStreamSource(this.mediaStream);
    this.analyser   = this.audioContext.createAnalyser();
    this.analyser.fftSize               = 512;
    this.analyser.smoothingTimeConstant = 0.3;

    this.sourceNode.connect(this.analyser);
    this.startVADLoop();
  }

  private startVADLoop(): void {
    // Run the tight rAF loop OUTSIDE Angular Zone so Zone.js does not
    // patch every requestAnimationFrame call and spam the console with
    // stack traces. Events are still emitted inside the zone via ngZone.run().
    this.ngZone.runOutsideAngular(() => {
      const bufferLength = this.analyser!.fftSize;
      const dataArray    = new Float32Array(bufferLength);

      const detect = () => {
        this.analyser!.getFloatTimeDomainData(dataArray);
        const energy = this.computeRMS(dataArray);
        const now    = Date.now();

        // Use elevated threshold while TTS is playing to suppress speaker echo
        const threshold = this.ttsSuppressed
          ? this.BARGE_IN_THRESHOLD
          : this.ENERGY_THRESHOLD;

        if (energy > threshold) {
          this.consecutiveSpeechFrames++;
          this.silenceStart = null;

          if (!this.isSpeaking && this.consecutiveSpeechFrames >= this.SPEECH_START_FRAMES) {
            this.isSpeaking  = true;
            this.speechStart = now;
            this.ngZone.run(() => {
              this.isSpeaking$.next(true);
              this.vadEvent$.next({ type: 'speech_start', timestamp: now, energy });
            });
          } else if (this.isSpeaking) {
            this.ngZone.run(() => {
              this.vadEvent$.next({ type: 'speaking', timestamp: now, energy });
            });
          }
        } else {
          this.consecutiveSpeechFrames = 0;

          if (this.isSpeaking) {
            if (!this.silenceStart) {
              this.silenceStart = now;
            } else if (now - this.silenceStart >= this.SILENCE_DURATION_MS) {
              this.isSpeaking   = false;
              this.silenceStart = null;
              this.ngZone.run(() => {
                this.isSpeaking$.next(false);
                this.vadEvent$.next({ type: 'speech_end', timestamp: now });
              });
            }
          }
        }

        this.animationFrameId = requestAnimationFrame(detect);
      };

      this.animationFrameId = requestAnimationFrame(detect);
    });
  }

  private computeRMS(buffer: Float32Array): number {
    let sum = 0;
    for (let i = 0; i < buffer.length; i++) {
      sum += buffer[i] * buffer[i];
    }
    return Math.sqrt(sum / buffer.length);
  }

  getAudioContext(): AudioContext | null {
    return this.audioContext;
  }

  getMediaStream(): MediaStream | null {
    return this.mediaStream;
  }

  destroy(): void {
    if (this.animationFrameId) {
      cancelAnimationFrame(this.animationFrameId);
    }
    this.mediaStream?.getTracks().forEach(t => t.stop());
    this.audioContext?.close();
    this.sourceNode?.disconnect();
    this.analyser?.disconnect();
  }
}
