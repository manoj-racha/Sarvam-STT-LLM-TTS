import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class AudioRecorderService {
  /**
   * PCM-based recorder — captures raw Float32 samples from the Web Audio
   * graph and encodes them as a 16-bit PCM WAV file for Sarvam STT.
   *
   * Architecture:
   *  AudioWorkletNode  (preferred, off-main-thread)
   *    → pcm-capture-processor.js sends { type:'chunk', data } per 128 samples
   *    → sends { type:'stopped' } when done
   *    → main thread assembles WAV only after 'stopped' — no race condition
   *
   *  ScriptProcessorNode (fallback)
   *    → collects samples synchronously
   *    → WAV assembled after a short drain delay post-stop
   */

  private workletNode:    AudioWorkletNode | null    = null;
  private processorNode:  ScriptProcessorNode | null = null;
  private sourceNode:     MediaStreamAudioSourceNode | null = null;
  private silentGain:     GainNode | null            = null;
  private pcmSamples:     Float32Array[]             = [];
  private _isRecording    = false;
  private usingWorklet    = false;
  private audioCtx:       AudioContext | null        = null;
  private workletLoaded   = false;

  readonly audioChunk$        = new Subject<Blob>();  // kept for API compat
  readonly recordingComplete$ = new Subject<Blob>();

  // ── Public API ─────────────────────────────────────────────────────────────

  async startRecording(stream: MediaStream, audioCtx: AudioContext): Promise<void> {
    if (this._isRecording) return;

    this.audioCtx     = audioCtx;
    this.pcmSamples   = [];
    this._isRecording = true;
    this.usingWorklet = false;

    // Ensure AudioContext is running (browser autoplay policy)
    if (audioCtx.state === 'suspended') {
      await audioCtx.resume();
    }

    this.sourceNode = audioCtx.createMediaStreamSource(stream);

    const workletOk = await this.tryStartWorklet(audioCtx);
    if (!workletOk) {
      this.startScriptProcessor(audioCtx);
    }

    console.log(`AudioRecorder | started (${this.usingWorklet ? 'AudioWorklet' : 'ScriptProcessor'}) | sampleRate=${audioCtx.sampleRate} Hz`);
  }

  stopRecording(): void {
    if (!this._isRecording) return;
    this._isRecording = false;

    if (this.usingWorklet && this.workletNode) {
      // Send 'stop' — the worklet will reply with { type: 'stopped' }
      // after flushing its last buffer. WAV assembly happens in onmessage.
      this.workletNode.port.postMessage('stop');
      // Don't disconnect yet — wait for 'stopped' confirmation in onmessage
    } else {
      // ScriptProcessor: onaudioprocess won't fire after we disconnect.
      // Give the JS event loop one tick to process any queued onaudioprocess
      // callbacks before assembling the WAV.
      this.disconnectNodes();
      setTimeout(() => this.assembleAndEmitWav(), 0);
    }
  }

  isRecording(): boolean {
    return this._isRecording;
  }

  // ── AudioWorklet ────────────────────────────────────────────────────────────

  private async tryStartWorklet(audioCtx: AudioContext): Promise<boolean> {
    try {
      if (!this.workletLoaded) {
        await audioCtx.audioWorklet.addModule('pcm-capture-processor.js');
        this.workletLoaded = true;
      }

      this.workletNode = new AudioWorkletNode(audioCtx, 'pcm-capture-processor');

      this.workletNode.port.onmessage = (event: MessageEvent) => {
        const msg = event.data;

        if (msg?.type === 'chunk') {
          // Collect ALL chunks — no _isRecording guard here.
          // The worklet stops sending after receiving 'stop', so we naturally
          // get all audio up to the moment of silence detection.
          this.pcmSamples.push(msg.data as Float32Array);

        } else if (msg?.type === 'stopped') {
          // Worklet confirmed it has flushed its last buffer.
          // NOW it's safe to assemble the WAV — all chunks are present.
          this.disconnectNodes();
          this.assembleAndEmitWav();
        }
      };

      this.silentGain = audioCtx.createGain();
      this.silentGain.gain.value = 0;

      this.sourceNode!.connect(this.workletNode);
      this.workletNode.connect(this.silentGain);
      this.silentGain.connect(audioCtx.destination);

      this.usingWorklet = true;
      return true;

    } catch (err) {
      console.warn('AudioRecorder | AudioWorklet unavailable, using ScriptProcessorNode:', err);
      return false;
    }
  }

  // ── ScriptProcessor fallback ────────────────────────────────────────────────

  private startScriptProcessor(audioCtx: AudioContext): void {
    // eslint-disable-next-line deprecation/deprecation
    this.processorNode = audioCtx.createScriptProcessor(4096, 1, 1);

    this.processorNode.onaudioprocess = (event) => {
      if (!this._isRecording) return;
      const input = event.inputBuffer.getChannelData(0);
      this.pcmSamples.push(new Float32Array(input));
    };

    this.silentGain = audioCtx.createGain();
    this.silentGain.gain.value = 0;

    this.sourceNode!.connect(this.processorNode);
    this.processorNode.connect(this.silentGain);
    this.silentGain.connect(audioCtx.destination);
  }

  // ── Shared helpers ──────────────────────────────────────────────────────────

  private disconnectNodes(): void {
    try { this.workletNode?.disconnect(); }   catch {}
    try { this.processorNode?.disconnect(); } catch {}
    try { this.silentGain?.disconnect(); }    catch {}
    try { this.sourceNode?.disconnect(); }    catch {}

    this.workletNode   = null;
    this.processorNode = null;
    this.silentGain    = null;
    this.sourceNode    = null;
  }

  private assembleAndEmitWav(): void {
    const chunks     = this.pcmSamples;
    this.pcmSamples  = [];

    const totalSamples = chunks.reduce((n, c) => n + c.length, 0);
    if (totalSamples === 0) {
      console.warn('AudioRecorder | stopRecording: no samples captured');
      return;
    }

    const allSamples = new Float32Array(totalSamples);
    let offset = 0;
    for (const chunk of chunks) {
      allSamples.set(chunk, offset);
      offset += chunk.length;
    }

    const sampleRate  = this.audioCtx?.sampleRate ?? 16000;
    const durationSec = totalSamples / sampleRate;
    const wavBuffer   = this.encodePcmWav(allSamples, sampleRate);
    const wavBlob     = new Blob([wavBuffer], { type: 'audio/wav' });

    console.log(`AudioRecorder | WAV ready | ${(wavBlob.size / 1024).toFixed(1)} KB | ${durationSec.toFixed(2)}s | ${sampleRate} Hz`);

    this.recordingComplete$.next(wavBlob);
  }

  // ── WAV encoder ─────────────────────────────────────────────────────────────

  private encodePcmWav(samples: Float32Array, sampleRate: number): ArrayBuffer {
    const bytesPerSample = 2;
    const dataSize       = samples.length * bytesPerSample;
    const buffer         = new ArrayBuffer(44 + dataSize);
    const view           = new DataView(buffer);

    const writeStr = (off: number, str: string) => {
      for (let i = 0; i < str.length; i++) view.setUint8(off + i, str.charCodeAt(i));
    };

    writeStr(0,  'RIFF');
    view.setUint32( 4, 36 + dataSize,              true);
    writeStr(8,  'WAVE');
    writeStr(12, 'fmt ');
    view.setUint32(16, 16,                          true);
    view.setUint16(20, 1,                           true); // PCM
    view.setUint16(22, 1,                           true); // mono
    view.setUint32(24, sampleRate,                  true);
    view.setUint32(28, sampleRate * bytesPerSample, true);
    view.setUint16(32, bytesPerSample,              true);
    view.setUint16(34, 16,                          true);
    writeStr(36, 'data');
    view.setUint32(40, dataSize,                    true);

    let off = 44;
    for (let i = 0; i < samples.length; i++) {
      const s = Math.max(-1, Math.min(1, samples[i]));
      view.setInt16(off, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
      off += 2;
    }

    return buffer;
  }
}
