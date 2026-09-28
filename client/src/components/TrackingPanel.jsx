import { useEffect, useState } from 'react';
import DeliveryMap from './DeliveryMap.jsx';

export default function TrackingPanel({ location, orderId, status }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const seconds = location ? Math.max(0, Math.floor((now - location.timestamp) / 1000)) : null;
  const live = status.startsWith('Live');
  const tag =
    live && seconds > 15 ? 'Waiting for a fresh location' : live ? 'Live location' : status;
  return (
    <section className="panel tracking-panel" aria-label="Delivery location">
      <div className="map-header">
        <div>
          <span className="eyebrow">YOUR DELIVERY</span>
          <h2>{orderId || 'No order selected'}</h2>
        </div>
        <span className="map-tag">{tag}</span>
      </div>
      <DeliveryMap location={location} orderId={orderId} />
      <div className="rider-card">
        <div className="avatar">↗</div>
        <div>
          <strong>{location?.riderId || 'Waiting for your rider'}</strong>
          <p>
            {location
              ? 'Your rider’s most recently reported location.'
              : 'The journey starts with the first location update.'}
          </p>
        </div>
        <span className="updated">
          {seconds === null ? '—' : seconds < 5 ? 'Just updated' : `${seconds}s ago`}
        </span>
      </div>
      <div className="metrics">
        <div>
          <span>LATITUDE</span>
          <strong>{location?.latitude.toFixed(6) ?? '—'}</strong>
        </div>
        <div>
          <span>LONGITUDE</span>
          <strong>{location?.longitude.toFixed(6) ?? '—'}</strong>
        </div>
        <div>
          <span>LAST UPDATE</span>
          <strong>{location ? new Date(location.timestamp).toLocaleTimeString() : '—'}</strong>
        </div>
      </div>
    </section>
  );
}
