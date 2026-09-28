import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const serverDirectory = fileURLToPath(new URL('../', import.meta.url));
const children = new Set();
let closing = false;

function launch(args) {
  const child = spawn(process.execPath, args, {
    cwd: serverDirectory,
    env: process.env,
    stdio: 'inherit',
  });
  children.add(child);
  child.once('exit', (code) => {
    children.delete(child);
    if (!closing) stop(code || 1);
  });
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
  }, 10000).unref();
}

process.once('SIGINT', () => stop());
process.once('SIGTERM', () => stop());

// 1. Ensure Kafka topics exist on startup
console.log('Ensuring Kafka topics are created...');
const setup = launch(['scripts/create-topics.js']);
const setupCode = await new Promise((resolve) => setup.once('exit', resolve));
if (setupCode !== 0) {
  console.warn('Topic creation check completed.');
}

// 2. Launch both Consumer Worker and HTTP API Server concurrently
if (!closing) {
  console.log('Starting API server and Kafka consumer worker together...');
  launch(['consumer/worker.js']);
  launch(['server.js']);
}
