import { logActivity } from './activity.js';
export function createRiderPublisher({
  api,
  orderId,
  riderId,
  intervalMs = 4000,
  onStatus = () => {},
}) {
  let timer;
  let stopped = true;
  let request;

  async function tick(getPosition) {
    if (stopped) return;
    try {
      const point = await getPosition();
      if (stopped) return;
      request = new AbortController();
      logActivity('rider.location.sending', { orderId, riderId, ...point });
      await api.send({ orderId, riderId, ...point }, request.signal);
      if (!stopped) {
        logActivity('rider.location.accepted', { orderId, riderId, timestamp: point.timestamp });
        onStatus(
          `${riderId} → ${orderId}: sent at ${new Date(point.timestamp).toLocaleTimeString()}`,
        );
      }
    } catch (error) {
      if (!stopped) {
        logActivity('rider.location.failed', { orderId, riderId, reason: error.message });
        onStatus(error.message);
      }
    } finally {
      // Schedule after completion: slow GPS/network cannot create overlapping requests.
      if (!stopped) timer = setTimeout(() => tick(getPosition), intervalMs);
    }
  }

  return {
    start(getPosition) {
      if (!stopped) return;
      stopped = false;
      logActivity('rider.sharing.started', { orderId, riderId });
      void tick(getPosition);
    },
    stop() {
      logActivity('rider.sharing.stopped', { orderId, riderId });
      stopped = true;
      clearTimeout(timer);
      request?.abort();
    },
  };
}

export function getGpsPosition() {
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation) return reject(new Error('GPS is unavailable in this browser'));
    navigator.geolocation.getCurrentPosition(
      ({ coords, timestamp }) => {
        resolve({
          latitude: coords.latitude,
          longitude: coords.longitude,
          timestamp: Math.floor(timestamp),
        });
      },
      (error) => reject(new Error(error.message)),
      { enableHighAccuracy: true, maximumAge: 0, timeout: 10000 },
    );
  });
}

export function createDemoRoute() {
  const route = [
    [28.6304, 77.2177],
    [28.6308, 77.2184],
    [28.6312, 77.2192],
    [28.6316, 77.22],
    [28.632, 77.2208],
    [28.6325, 77.2205],
    [28.633, 77.2197],
    [28.6334, 77.2189],
    [28.6337, 77.218],
    [28.6333, 77.2172],
    [28.6328, 77.2167],
    [28.6322, 77.2166],
    [28.6316, 77.2168],
    [28.631, 77.2171],
  ];
  let index = 0;
  return () => {
    const [latitude, longitude] = route[index++ % route.length];
    return { latitude, longitude, timestamp: Date.now() };
  };
}
