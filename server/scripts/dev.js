import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const serverDirectory = fileURLToPath(new URL('../', import.meta.url));
const clientDirectory = fileURLToPath(new URL('../../client/', import.meta.url));
const env = {
  ...process.env,
  DEMO_MODE: process.env.DEMO_MODE || 'true',
  KAFKA_BROKERS: process.env.KAFKA_BROKERS || 'localhost:19092',
  REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:16379',
};
const children = new Set();
let closing = false;

function launch(args, cwd = serverDirectory) {
  const child = spawn(process.execPath, args, { cwd, env, stdio: 'inherit' });
  children.add(child);
  child.once('exit', () => children.delete(child));
  child.once('error', (error) => {
    console.error(error.message);
    stop(1);
  });
  return child;
}
function stop(code = 0) {
  if (closing) return;
  closing = true;
  process.exitCode = code;
  for (const child of children) child.kill('SIGTERM');
  setTimeout(() => {
    for (const child of children) child.kill('SIGKILL');
  }, 16000).unref();
}
process.once('SIGINT', () => stop());
process.once('SIGTERM', () => stop());

const setup = launch(['scripts/create-topics.js']);
const code = await new Promise((resolve) => setup.once('exit', resolve));
if (code !== 0) stop(1);
else if (!closing) {
  const processes = [launch(['--watch', 'consumer/worker.js']), launch(['--watch', 'server.js'])];

  if (process.argv.includes('--client')) {
    processes.push(launch(['node_modules/vite/bin/vite.js'], clientDirectory));
  }
  for (const child of processes)
    child.once('exit', (exitCode) => {
      if (!closing) stop(exitCode || 1);
    });
  console.log(
    'API + location worker started together. MongoDB uses server/.env. Ctrl+C stops both.',
  );
}
