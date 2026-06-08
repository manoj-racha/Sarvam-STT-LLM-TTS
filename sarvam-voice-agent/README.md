# Sarvam Voice Agent

Real-time conversational AI system using Sarvam AI APIs (Saaras STT + LLM + Bulbul TTS) with full barge-in support and optimized end-to-end latency.

---

## Project Structure

```
sarvam-voice-agent/
├── sarvam-backend/               # Python FastAPI Backend Proxy
│   ├── main.py                   # FastAPI application script
│   └── requirements.txt          # Python dependencies
│
└── sarvam-voice-agent/           # Angular 17 Frontend App
    ├── src/app/
    │   ├── models/
    │   │   └── voice.models.ts   # TypeScript interfaces
    │   ├── services/
    │   │   ├── vad.service.ts    # Voice Activity Detection (RMS energy-based)
    │   │   ├── audio-recorder.ts # Microphone recording controller
    │   │   ├── sarvam-stt.ts     # Saaras v3 STT interface
    │   │   ├── sarvam-llm.ts     # Sarvam LLM client (with sentence chunking)
    │   │   ├── sarvam-tts.ts     # Bulbul v3 TTS voice response & order queue
    │   │   └── voice-pipeline.ts # Main event orchestration and barge-in logic
    │   └── components/
    │       └── voice-agent/      # Visual agent interface
    ├── package.json              # NPM dependencies
    ├── angular.json              # Angular workspace configuration
    ├── proxy.conf.json           # Dev server proxy configuration
    └── tsconfig.json             # TypeScript configuration
```

---

## Execution Instructions

To run this application, you must start both the **Python FastAPI Backend** and the **Angular Frontend**.

### Step 1: Run the Python Backend
The Python backend acts as a proxy to protect your Sarvam API Key and manage Server-Sent Events (SSE) streaming.

1. Open a terminal and navigate to the backend directory:
   ```bash
   cd sarvam-backend
   ```
2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   # On Windows (PowerShell):
   .\venv\Scripts\Activate.ps1
   # On macOS/Linux:
   source venv/bin/activate
   ```
3. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set your Sarvam API Key as an environment variable:
   * **Windows (PowerShell)**:
     ```powershell
     $env:SARVAM_API_KEY="your_sarvam_api_key_here"
     ```
   * **macOS/Linux**:
     ```bash
     export SARVAM_API_KEY="your_sarvam_api_key_here"
     ```
5. Start the FastAPI development server:
   ```bash
   uvicorn main:app --reload --port 3000
   ```
   The backend proxy will run at `http://localhost:3000`. You can test its health at `http://localhost:3000/health`.

### Step 2: Run the Angular Frontend
The Angular frontend manages microphone input, client-side Voice Activity Detection, and real-time sentence-by-sentence audio playback.

1. Open a new terminal window and navigate to the frontend directory:
   ```bash
   cd sarvam-voice-agent
   ```
2. Install the node packages:
   ```bash
   npm install
   ```
3. Start the Angular development server:
   ```bash
   npm start
   ```
   *Note: This command runs `ng serve` using the local proxy configuration `proxy.conf.json` so that all frontend requests to `/api/*` are automatically routed to the Python backend running on port 3000.*
4. Open your web browser and navigate to `http://localhost:4200` to start using the voice agent.

---

## Architecture & Optimizations

### The Pipeline
```
Microphone → [Client VAD] → [Audio Recorder]
                 ↓ speech_end
             [STT: Saaras v3]
                 ↓ transcript
             [LLM: Sarvam stream]
                 ↓ sentence by sentence (segmented using regex)
             [TTS: Bulbul v3]  ←— Dispatched per sentence
                 ↓
             Speaker (AudioContext queueing)
```

### Latency Budget & Techniques
* **Client-Side VAD**: RMS thresholds computed locally via Web Audio API. Eliminates round-trip delay for silence detection.
* **Sentence-Level TTS Dispatching**: Sentence chunks are synthesized via Bulbul v3 as soon as they are segmented from the stream. This reduces initial response onset by 300ms.
* **Ordered Audio Queueing**: Audio chunks are buffered and played sequentially based on index order, preventing jitter and speech overlaps.
* **Barge-In Interrupts**: Detects user voice starting (`speech_start` event) while the assistant is speaking, immediately aborts the active LLM stream (`AbortController`), and stops active audio playback (`AudioBufferSourceNode.stop()`).

---

## Configuration

* **VAD Tuning** ([vad.service.ts](file:///c:/Users/manoj/Downloads/sarvam-voice-agent/sarvam-voice-agent/src/app/services/vad.service.ts)):
  ```typescript
  private readonly ENERGY_THRESHOLD   = 0.015;  // Speech energy threshold
  private readonly SPEECH_START_FRAMES = 3;      // Consecutive frames to confirm speech
  private readonly SILENCE_DURATION_MS = 600;    // Silence duration before ending turn
  ```
* **Language & Voice** ([voice-pipeline.service.ts](file:///c:/Users/manoj/Downloads/sarvam-voice-agent/sarvam-voice-agent/src/app/services/voice-pipeline.service.ts)):
  ```typescript
  // Synthesize using standard language code and speaker
  await this.tts.synthesizeAndEnqueue(sentence, chunkIndex, 'hi-IN', 'meera');
  ```
  *Available Speakers*: `meera`, `arvind`, `amol`, `amartya`, `diya`, `neel`, `maitreyi`
* **LLM Model** ([sarvam-llm.service.ts](file:///c:/Users/manoj/Downloads/sarvam-voice-agent/sarvam-voice-agent/src/app/services/sarvam-llm.service.ts)):
  * `sarvam-m` (default, fast)
  * `sarvam-30b`
  * `sarvam-105b`

---

## Sarvam API Docs
* STT: https://docs.sarvam.ai/api-reference-docs/speech-to-text
* LLM: https://docs.sarvam.ai/api-reference-docs/chat-completions
* TTS: https://docs.sarvam.ai/api-reference-docs/text-to-speech
