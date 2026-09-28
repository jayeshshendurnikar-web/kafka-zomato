import { createLocationState } from './location-state.js';
import { logActivity } from './activity.js';

// Customer Tracking Manager: Handles WebSocket subscription, API snapshot fetching, and state synchronization
export function createCustomerTracker({ socket, api, orderId, onLocation, onStatus = () => {} }) {
  // State guard ensures only newer timestamps update the map, rejecting out-of-order points
  const state = createLocationState(orderId, onLocation);
  let disposed = false;
  let generation = 0;
  let retry;
  let request;

  // Cancel any ongoing snapshot fetch or scheduled retry
  function cancelSync() {
    generation++;
    clearTimeout(retry);
    request?.abort();
  }

  // Synchronize on initial connect or reconnect:
  // 1. Subscribe to order room via WebSocket and wait for ACK
  // 2. Fetch latest snapshot from API to avoid missing locations during connect
  async function synchronize() {
    cancelSync();
    const attempt = generation;
    request = new AbortController();
    try {
      onStatus('Connecting to your rider…');
      logActivity('customer.socket.connected', { orderId, socketId: socket.id });

      // Edge Case: The server sends ACK only after Redis pub/sub channel is subscribed and ready
      const result = await socket.timeout(5000).emitWithAck('tracking:subscribe');
      if (disposed || attempt !== generation) return;
      if (!result?.ok) throw new Error(result?.error || 'Subscription failed');
      logActivity('customer.order.subscribed', { orderId });

      // Fetch snapshot from Redis via Express API
      onStatus('Syncing latest location…');
      const { location } = await api.latest(orderId, request.signal);
      logActivity('customer.snapshot.received', { orderId, location });
      if (disposed || attempt !== generation) return;

      // state.accept checks if live socket message already arrived with a newer timestamp
      state.accept(location);
      onStatus(state.current ? 'Live • connected' : 'Connected • waiting for rider');
    } catch (error) {
      if (disposed || attempt !== generation) return;
      onStatus(error.message);
      // Auto-retry sync after 2 seconds if still connected and not an auth error (403)
      if (socket.connected && error.status !== 403) retry = setTimeout(synchronize, 2000);
    }
  }

  // Handle incoming live location updates from WebSocket
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
