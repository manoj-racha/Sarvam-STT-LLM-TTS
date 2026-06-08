import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, from } from 'rxjs';
import { SarvamSTTResponse } from '../models/voice.models';

@Injectable({ providedIn: 'root' })
export class SarvamSttService {
  // Point this to your backend proxy to protect the API key
  private readonly STT_ENDPOINT = '/api/sarvam/stt';

  constructor(private http: HttpClient) {}

  /**
   * Transcribe an audio blob using Sarvam Saaras v3.
   * Uses multipart/form-data as required by the Sarvam STT REST API.
   */
  transcribe(audioBlob: Blob, languageCode = 'hi-IN'): Observable<SarvamSTTResponse> {
    const formData = new FormData();
    formData.append('file', audioBlob, 'audio.wav');  // recorder now produces WAV (PCM 16kHz)
    formData.append('model', 'saaras:v3');
    formData.append('language_code', languageCode);
    formData.append('mode', 'transcribe');
    formData.append('with_timestamps', 'false');
    formData.append('with_disfluencies', 'false');
    formData.append('debug_mode', 'false');

    return this.http.post<SarvamSTTResponse>(this.STT_ENDPOINT, formData);
  }
}
