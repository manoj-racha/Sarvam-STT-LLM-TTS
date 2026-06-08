/**
 * pcm-capture-processor.js — AudioWorklet processor for raw PCM capture.
 *
 * Runs in the audio rendering thread (off main thread).
 * Sends typed messages so the main thread knows when all chunks are flushed:
 *   { type: 'chunk', data: Float32Array }  — audio samples
 *   { type: 'stopped' }                    — worklet has stopped, no more chunks
 */

class PcmCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this._active = true;

    this.port.onmessage = (event) => {
      if (event.data === 'stop') {
        this._active = false;
        // 'stopped' is sent on the NEXT process() call so the audio thread
        // can flush any partially-filled buffer before confirming completion.
      }
    };
  }

  process(inputs) {
    if (!this._active) {
      // Confirm to the main thread that we are done — no more chunks after this.
      this.port.postMessage({ type: 'stopped' });
      return false; // remove node from graph
    }

    const input = inputs[0];
    if (input && input[0] && input[0].length > 0) {
      const samples = new Float32Array(input[0]);
      this.port.postMessage({ type: 'chunk', data: samples }, [samples.buffer]);
    }

    return true;
  }
}

registerProcessor('pcm-capture-processor', PcmCaptureProcessor);
