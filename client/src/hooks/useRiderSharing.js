import { useEffect, useState } from 'react';
import { createApi } from '../lib/api.js';
import { createRiderPublisher, getGpsPosition, createDemoRoute } from '../lib/rider.js';

export function useRiderSharing(session, minIntervalMs) {
  const [status, setStatus] = useState('Location sharing is off');
  useEffect(() => {
    if (!session) {
      setStatus('Location sharing is off');
      return;
    }
    setStatus('Starting location sharing…');
    const apiUrl = import.meta.env.VITE_API_URL?.replace(/\/$/, '') || '';
    const publisher = createRiderPublisher({
      api: createApi({ token: session.token, baseUrl: apiUrl }),
      orderId: session.orderId,
      riderId: session.riderId,
      intervalMs: Math.max(4000, minIntervalMs),
      onStatus: setStatus,
    });

    publisher.start(session.source === 'demo' ? createDemoRoute() : getGpsPosition);
    return () => publisher.stop();
  }, [session, minIntervalMs]);
  return status;
}
