export function installShutdown(cleanup) {
  let closing = false;
  const shutdown = async (signal) => {
    if (closing) return;
    closing = true;
    console.log(`Shutting down (${signal})`);
    const timeout = setTimeout(() => process.exit(1), 15000).unref();
    try {
      await cleanup();
    } catch (error) {
      console.error(error.message);
      process.exitCode = 1;
    } finally {
      clearTimeout(timeout);
    }
  };
  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  return shutdown;
}
