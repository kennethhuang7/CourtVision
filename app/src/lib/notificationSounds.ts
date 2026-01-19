

import { logger } from './logger';

export type NotificationSoundType = 
  | 'chime'
  | 'beep'
  | 'ding'
  | 'pop'
  | 'whoosh'
  | 'bell'
  | 'triple-beep'
  | 'note'
  | 'anticipate'
  | 'bloom'
  | 'calypso'
  | 'choo-choo'
  | 'descent'
  | 'fanfare'
  | 'ladder'
  | 'minuet'
  | 'news-flash'
  | 'noir'
  | 'sherwood-forest'
  | 'spell'
  | 'suspense'
  | 'telegraph'
  | 'tiptoes'
  | 'typewriters'
  | 'update'
  | 'cosmic'
  | 'constellation'
  | 'circuit'
  | 'crystals'
  | 'opening'
  | 'radar'
  | 'signal'
  | 'silver'
  | 'sonar'
  | 'fanfare-short';

interface SoundDefinition {
  name: string;
  description: string;
  play: (audioContext: AudioContext, volume?: number) => void;
}


const createNote = (ctx: AudioContext, freq: number, startTime: number, duration: number, baseVolume: number = 0.2, type: OscillatorType = 'sine', volumeMultiplier: number = 1.0) => {
  const osc = ctx.createOscillator();
  const gain = ctx.createGain();
  osc.connect(gain);
  gain.connect(ctx.destination);
  osc.frequency.value = freq;
  osc.type = type;
  const adjustedVolume = Math.min(1.0, baseVolume * volumeMultiplier); 
  gain.gain.setValueAtTime(adjustedVolume, startTime);
  gain.gain.exponentialRampToValueAtTime(0.01, startTime + duration);
  osc.start(startTime);
  osc.stop(startTime + duration);
};

const soundDefinitions: Record<NotificationSoundType, SoundDefinition> = {
  chime: {
    name: 'Chime',
    description: 'Gentle chime',
    play: (ctx, volume) => {
      const notes = [523.25, 659.25, 783.99]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.3, 0.2, 'sine', volume);
      });
    },
  },
  beep: {
    name: 'Beep',
    description: 'Simple beep',
    play: (ctx, volume) => {
      createNote(ctx, 800, ctx.currentTime, 0.1, 0.3, 'sine', volume);
    },
  },
  ding: {
    name: 'Ding',
    description: 'Single ding',
    play: (ctx, volume) => {
      createNote(ctx, 1000, ctx.currentTime, 0.15, 0.4, 'sine', volume);
    },
  },
  pop: {
    name: 'Pop',
    description: 'Quick pop',
    play: (ctx) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.setValueAtTime(400, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(200, ctx.currentTime + 0.05);
      osc.type = 'sine';
      gain.gain.setValueAtTime(0.3, ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.05);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.05);
    },
  },
  whoosh: {
    name: 'Whoosh',
    description: 'Swoosh sound',
    play: (ctx, volume) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      const filter = ctx.createBiquadFilter();
      osc.connect(filter);
      filter.connect(gain);
      gain.connect(ctx.destination);
      filter.type = 'lowpass';
      filter.frequency.value = 2000;
      osc.frequency.setValueAtTime(200, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(800, ctx.currentTime + 0.1);
      osc.type = 'sawtooth';
      gain.gain.setValueAtTime(Math.min(1.0, 0.25 * volume), ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.1);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.1);
    },
  },
  bell: {
    name: 'Bell',
    description: 'Bell ring',
    play: (ctx, volume) => {
      [880, 1108.73].forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.05, 0.4, 0.3, 'sine', volume);
      });
    },
  },
  'triple-beep': {
    name: 'Triple Beep',
    description: 'Three quick beeps',
    play: (ctx, volume) => {
      [0, 0.08, 0.16].forEach((offset) => {
        createNote(ctx, 800, ctx.currentTime + offset, 0.05, 0.25, 'sine', volume);
      });
    },
  },
  note: {
    name: 'Note',
    description: 'Single musical note',
    play: (ctx, volume) => {
      createNote(ctx, 523.25, ctx.currentTime, 0.2, 0.3, 'sine', volume);
    },
  },
  anticipate: {
    name: 'Anticipate',
    description: 'Rising anticipation',
    play: (ctx, volume) => {
      const notes = [392, 440, 494, 523.25, 587.33]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.06, 0.15, 0.25, 'sine', volume);
      });
    },
  },
  bloom: {
    name: 'Bloom',
    description: 'Blossoming notes',
    play: (ctx, volume) => {
      const notes = [261.63, 329.63, 392, 523.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.08, 0.25, 0.2, 'sine', volume);
      });
    },
  },
  calypso: {
    name: 'Calypso',
    description: 'Tropical rhythm',
    play: (ctx, volume) => {
      const notes = [523.25, 659.25, 783.99, 659.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.2, 0.25, 'sine', volume);
      });
    },
  },
  'choo-choo': {
    name: 'Choo Choo',
    description: 'Train whistle',
    play: (ctx, volume) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.setValueAtTime(400, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(600, ctx.currentTime + 0.2);
      osc.type = 'sine';
      gain.gain.setValueAtTime(Math.min(1.0, 0.3 * volume), ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.3);
    },
  },
  descent: {
    name: 'Descent',
    description: 'Descending notes',
    play: (ctx, volume) => {
      const notes = [784, 698, 622, 554, 494, 440]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.08, 0.15, 0.22, 'sine', volume);
      });
    },
  },
  fanfare: {
    name: 'Fanfare',
    description: 'Triumphant fanfare',
    play: (ctx, volume) => {
      const notes = [523.25, 659.25, 783.99, 1046.5]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.12, 0.3, 0.28, 'sine', volume);
      });
    },
  },
  ladder: {
    name: 'Ladder',
    description: 'Climbing scale',
    play: (ctx, volume) => {
      const notes = [261.63, 293.66, 329.63, 349.23, 392, 440, 493.88, 523.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.05, 0.12, 0.2, 'sine', volume);
      });
    },
  },
  minuet: {
    name: 'Minuet',
    description: 'Classical minuet',
    play: (ctx, volume) => {
      const notes = [523.25, 659.25, 783.99, 523.25, 783.99]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.25, 0.25, 'sine', volume);
      });
    },
  },
  'news-flash': {
    name: 'News Flash',
    description: 'Breaking news alert',
    play: (ctx, volume) => {
      [880, 880, 880].forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.15, 0.1, 0.35, 'sine', volume);
      });
    },
  },
  noir: {
    name: 'Noir',
    description: 'Film noir style',
    play: (ctx, volume) => {
      const notes = [277.18, 311.13, 349.23]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.12, 0.3, 0.25, 'square', volume);
      });
    },
  },
  'sherwood-forest': {
    name: 'Sherwood Forest',
    description: 'Nature sounds',
    play: (ctx, volume) => {
      const notes = [329.63, 392, 493.88]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.4, 0.22, 'sine', volume);
      });
    },
  },
  spell: {
    name: 'Spell',
    description: 'Magical sparkle',
    play: (ctx, volume) => {
      const notes = [659.25, 783.99, 987.77, 1318.51]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.05, 0.15, 0.2, 'sine', volume);
      });
    },
  },
  suspense: {
    name: 'Suspense',
    description: 'Tension building',
    play: (ctx, volume) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.setValueAtTime(200, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(400, ctx.currentTime + 0.4);
      osc.type = 'triangle';
      gain.gain.setValueAtTime(Math.min(1.0, 0.25 * volume), ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.4);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.4);
    },
  },
  telegraph: {
    name: 'Telegraph',
    description: 'Morse code style',
    play: (ctx, volume) => {
      [0, 0.1, 0.2, 0.35].forEach((offset) => {
        createNote(ctx, 800, ctx.currentTime + offset, 0.05, 0.3, 'sine', volume);
      });
    },
  },
  tiptoes: {
    name: 'Tiptoes',
    description: 'Light steps',
    play: (ctx, volume) => {
      const notes = [523.25, 587.33, 659.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.2, 0.18, 'sine', volume);
      });
    },
  },
  typewriters: {
    name: 'Typewriters',
    description: 'Typing sound',
    play: (ctx, volume) => {
      [600, 700, 650, 750].forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.08, 0.05, 0.25, 'square', volume);
      });
    },
  },
  update: {
    name: 'Update',
    description: 'Software update',
    play: (ctx, volume) => {
      const notes = [440, 554, 659.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.2, 0.25, 'sine', volume);
      });
    },
  },
  cosmic: {
    name: 'Cosmic',
    description: 'Space sounds',
    play: (ctx, volume) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      const filter = ctx.createBiquadFilter();
      osc.connect(filter);
      filter.connect(gain);
      gain.connect(ctx.destination);
      filter.type = 'lowpass';
      filter.frequency.value = 800;
      osc.frequency.setValueAtTime(200, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(600, ctx.currentTime + 0.3);
      osc.type = 'sawtooth';
      gain.gain.setValueAtTime(Math.min(1.0, 0.2 * volume), ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.3);
    },
  },
  constellation: {
    name: 'Constellation',
    description: 'Starry pattern',
    play: (ctx, volume) => {
      const notes = [523.25, 659.25, 783.99, 659.25, 523.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.08, 0.18, 0.2, 'sine', volume);
      });
    },
  },
  circuit: {
    name: 'Circuit',
    description: 'Electronic pulse',
    play: (ctx, volume) => {
      [400, 600, 500, 700].forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.06, 0.08, 0.25, 'square', volume);
      });
    },
  },
  crystals: {
    name: 'Crystals',
    description: 'Crystal chimes',
    play: (ctx, volume) => {
      const notes = [1046.5, 1318.51, 1567.98]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.4, 0.2, 'sine', volume);
      });
    },
  },
  opening: {
    name: 'Opening',
    description: 'Grand opening',
    play: (ctx, volume) => {
      const notes = [261.63, 329.63, 392, 523.25]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.15, 0.35, 0.28, 'sine', volume);
      });
    },
  },
  radar: {
    name: 'Radar',
    description: 'Radar sweep',
    play: (ctx, volume) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      const filter = ctx.createBiquadFilter();
      osc.connect(filter);
      filter.connect(gain);
      gain.connect(ctx.destination);
      filter.type = 'bandpass';
      filter.frequency.value = 800;
      filter.Q.value = 30;
      osc.frequency.value = 800;
      osc.type = 'sine';
      gain.gain.setValueAtTime(Math.min(1.0, 0.3 * volume), ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.25);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.25);
    },
  },
  signal: {
    name: 'Signal',
    description: 'Signal beep',
    play: (ctx, volume) => {
      [800, 800].forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.2, 0.1, 0.3, 'sine', volume);
      });
    },
  },
  silver: {
    name: 'Silver',
    description: 'Shimmering silver',
    play: (ctx, volume) => {
      const notes = [659.25, 783.99, 987.77]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.08, 0.3, 0.22, 'sine', volume);
      });
    },
  },
  sonar: {
    name: 'Sonar',
    description: 'Sonar ping',
    play: (ctx, volume) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.setValueAtTime(800, ctx.currentTime);
      osc.frequency.exponentialRampToValueAtTime(400, ctx.currentTime + 0.2);
      osc.type = 'sine';
      gain.gain.setValueAtTime(Math.min(1.0, 0.3 * volume), ctx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.2);
      osc.start(ctx.currentTime);
      osc.stop(ctx.currentTime + 0.2);
    },
  },
  'fanfare-short': {
    name: 'Fanfare (Short)',
    description: 'Brief fanfare',
    play: (ctx, volume) => {
      const notes = [523.25, 659.25, 783.99]; 
      notes.forEach((freq, i) => {
        createNote(ctx, freq, ctx.currentTime + i * 0.1, 0.2, 0.3, 'sine', volume);
      });
    },
  },
};



let globalAudioContext: AudioContext | null = null;

function getAudioContext(): AudioContext {
  if (!globalAudioContext) {
    globalAudioContext = new (window.AudioContext || (window as any).webkitAudioContext)();
  }
  
  
  if (globalAudioContext.state === 'suspended') {
    globalAudioContext.resume().catch(err => {
      logger.error('Error resuming AudioContext', err as Error);
    });
  }
  
  return globalAudioContext;
}

export function playNotificationSound(soundType: NotificationSoundType, volume: number = 1.0): void {
  try {
    const audioContext = getAudioContext();
    const sound = soundDefinitions[soundType];
    if (sound) {
      
      
      sound.play(audioContext, volume);
    }
  } catch (error) {
    logger.error('Error playing notification sound', error as Error);
  }
}


export function previewNotificationSound(soundType: NotificationSoundType, volume: number = 1.0): void {
  playNotificationSound(soundType, volume);
}


export function getAvailableSounds(): Array<{ value: NotificationSoundType; name: string; description: string }> {
  return Object.entries(soundDefinitions).map(([value, def]) => ({
    value: value as NotificationSoundType,
    name: def.name,
    description: def.description,
  }));
}

