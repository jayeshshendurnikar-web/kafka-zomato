import { useEffect, useState } from 'react';
import { io } from 'socket.io-client';
import { createApi } from '../lib/api.js';
import { createCustomerTracker } from '../lib/customer.js';

export function useCustomerTracking(session) {
  const [location, setLocation] = useState(null);
  const [status, setStatus] = useState('Ready when you are');

  useEffect(() => {
    setLocation(null);
    if (!session) {
      setStatus('Ready when you are');
      return;
    }
    const { orderId, token } = session;
    const apiUrl = import.meta.env.VITE_API_URL?.replace(/\/$/, '') || undefined;
    const socket = io(apiUrl, {
      transports: ['websocket'],
      autoConnect: false,
      auth: { orderId, token },
      reconnectionDelayMax: 5000,
    });
    const tracker = createCustomerTracker({
      socket,
      api: createApi({ token, baseUrl: apiUrl || '' }),
      orderId,
      onLocation: setLocation,
      onStatus: setStatus,
    });

    tracker.start();
    return () => tracker.stop();
  }, [session]);

  return { location: location?.orderId === session?.orderId ? location : null, status };
}
