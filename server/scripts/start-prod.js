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
  child.once('exit', () => children.delete(child));
  child.once('error', (error) => {
    console.error(`Process error [${args.join(' ')}]:`, error.message);
    if (!closing) stop(1);
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

process.once('SIGINT', () => stop(0));
process.once('SIGTERM', () => stop(0));

// 1. Ensure Kafka topics exist before starting server and worker
console.log('Ensuring Kafka topics are created...');
const setup = launch(['scripts/create-topics.js']);
const setupCode = await new Promise((resolve) => setup.once('exit', resolve));
if (setupCode !== 0) {
  console.warn(
    `Topic check completed with exit code ${setupCode}. Proceeding to start services...`,
  );
}

// 2. Launch both Consumer Worker and HTTP API Server concurrently
if (!closing) {
  console.log('Starting API server and Kafka consumer worker together...');
  const processes = [launch(['consumer/worker.js']), launch(['server.js'])];

  for (const child of processes) {
    child.once('exit', (exitCode) => {
      if (!closing && exitCode !== 0) {
        console.error(`Service exited unexpectedly with code ${exitCode}`);
        stop(exitCode ?? 1);
      }
    });
  }
}
