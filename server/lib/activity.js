// Keep activity searchable by orderId without printing tokens, headers or connection strings.
export function logActivity(event, details = {}, level = 'info') {
  if (process.env.ACTIVITY_LOGS === 'false' && level === 'info') return;
  const write = console[level] || console.log;
  write(JSON.stringify({ time: new Date().toISOString(), pid: process.pid, event, ...details }));
}
