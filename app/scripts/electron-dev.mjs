#!/usr/bin/env node

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import waitOn from 'wait-on';
import getPort from 'get-port';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, '..');

async function main() {
  
  const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : await getPort({ port: 5173 });
  const VITE_URL = `http://localhost:${PORT}`;

  console.log(`Starting Electron dev environment on port ${PORT}...\n`);

  
  const vite = spawn('vite', ['--port', PORT.toString()], {
    cwd: rootDir,
    stdio: 'inherit',
    shell: true,
    env: { ...process.env, VITE_DEV_SERVER_URL: VITE_URL },
  });

  const electronBuild = spawn(
    'vite',
    ['build', '--watch', '--config', 'vite.config.electron.ts'],
    {
      cwd: rootDir,
      stdio: 'inherit',
      shell: true,
      env: { ...process.env, ELECTRON_WATCH: 'true' },
    }
  );

  
  waitOn({ resources: [VITE_URL], timeout: 30000 })
    .then(() => {
      console.log(`\nVite ready at ${VITE_URL}, starting Electron...\n`);
      const electron = spawn('electron', ['.'], {
        cwd: rootDir,
        stdio: 'inherit',
        shell: true,
        env: {
          ...process.env,
          NODE_ENV: 'development',
          VITE_DEV_SERVER_URL: VITE_URL,
        },
      });

      electron.on('exit', () => {
        vite.kill();
        electronBuild.kill();
        process.exit(0);
      });
    })
    .catch((err) => {
      console.error('Failed to start:', err);
      vite.kill();
      electronBuild.kill();
      process.exit(1);
    });

  
  process.on('SIGINT', () => {
    vite.kill();
    electronBuild.kill();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    vite.kill();
    electronBuild.kill();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});

