import { defineConfig } from 'vite';
import { resolve } from 'path';


export default defineConfig({
  build: {
    outDir: 'dist-electron',
    emptyOutDir: false, 
    watch: process.env.ELECTRON_WATCH === 'true' ? {} : null, 
    lib: {
      entry: {
        main: resolve(__dirname, 'electron/main.ts'),
        preload: resolve(__dirname, 'electron/preload.ts'),
      },
      formats: ['cjs'],
    },
    rollupOptions: {
      external: (id) => {
        
        return id === 'electron' || id === 'electron-store' || id.startsWith('electron/');
      },
      output: {
        entryFileNames: '[name].cjs',
        format: 'cjs',
      },
    },
    minify: false, 
  },
});

