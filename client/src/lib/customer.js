import { createLocationState } from './location-state.js';
import { logActivity } from './activity.js';

export function createCustomerTracker({ socket, api, orderId, onLocation, onStatus = () => {} }) {
  const state = createLocationState(orderId, onLocation);
  let disposed = false;
  let generation = 0;
  let retry;
  let request;

  function cancelSync() {
    generation++;
    clearTimeout(retry);
    request?.abort();
  }

  async function synchronize() {
    cancelSync();
    const attempt = generation;
    request = new AbortController();
    try {
      onStatus('Connecting to your rider…');
      logActivity('customer.socket.connected', { orderId, socketId: socket.id });
      // The ACK is sent only after the Redis subscription and local room are ready.
      const result = await socket.timeout(5000).emitWithAck('tracking:subscribe');
      if (disposed || attempt !== generation) return;
      if (!result?.ok) throw new Error(result?.error || 'Subscription failed');
      logActivity('customer.order.subscribed', { orderId });
      onStatus('Syncing latest location…');
      const { location } = await api.latest(orderId, request.signal);
      logActivity('customer.snapshot.received', { orderId, location });
      if (disposed || attempt !== generation) return;
      state.accept(location);
      onStatus(state.current ? 'Live • connected' : 'Connected • waiting for rider');
    } catch (error) {
      if (disposed || attempt !== generation) return;
      onStatus(error.message);
      if (socket.connected && error.status !== 403) retry = setTimeout(synchronize, 2000);
    }
  }

  const onUpdate = (location) => {
    logActivity('customer.location.received', location);
    if (state.accept(location)) onStatus('Live • connected');
  };
  const onDisconnect = (reason) => {
    logActivity('customer.socket.disconnected', { orderId, reason });
    cancelSync();
    onStatus(
      reason === 'io server disconnect'
        ? 'Session ended • connect again with a valid token'
        : 'Connection lost • reconnecting…',
    );
  };
  const onError = (error) => {
    logActivity('customer.socket.error', { orderId, reason: error.message });
    onStatus(error.message);
  };
  socket.on('location:update', onUpdate);
  socket.on('connect', synchronize);
  socket.on('disconnect', onDisconnect);
  socket.on('connect_error', onError);

  return {
    state,
    start() {
      socket.connect();
    },
    stop() {
      disposed = true;
      cancelSync();
      socket.off('location:update', onUpdate);
      socket.off('connect', synchronize);
      socket.off('disconnect', onDisconnect);
      socket.off('connect_error', onError);
      socket.disconnect();
    },
  };
}
