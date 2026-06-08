import { Component } from '@angular/core';
import { VoiceAgentComponent } from './components/voice-agent/voice-agent.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [VoiceAgentComponent],
  template: `<app-voice-agent></app-voice-agent>`,
})
export class AppComponent {}
