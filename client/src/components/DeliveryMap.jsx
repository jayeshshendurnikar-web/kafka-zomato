import { useEffect, useRef } from 'react';
import L from 'leaflet';

export default function DeliveryMap({ location, orderId }) {
  const container = useRef(null);
  const mapRef = useRef(null);
  const markerRef = useRef(null);
  const lineRef = useRef(null);
  const trailRef = useRef([]);

  useEffect(() => {
    const map = L.map(container.current, { zoomControl: false }).setView([28.632, 77.218], 16);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 19,
    }).addTo(map);
    L.control.zoom({ position: 'topright' }).addTo(map);
    lineRef.current = L.polyline([], { color: '#e94e46', weight: 4, opacity: 0.65 }).addTo(map);
    mapRef.current = map;
    const resize = new ResizeObserver(() => {
      map.invalidateSize();
      if (markerRef.current) map.panTo(markerRef.current.getLatLng(), { animate: false });
    });
    resize.observe(container.current);
    return () => {
      resize.disconnect();
      map.remove();
      mapRef.current = null;
      markerRef.current = null;
    };
  }, []);

  useEffect(() => {
    markerRef.current?.remove();
    markerRef.current = null;
    trailRef.current = [];
    lineRef.current?.setLatLngs([]);
  }, [orderId]);

  useEffect(() => {
    if (!location || !mapRef.current) return;
    const point = [location.latitude, location.longitude];
    if (!markerRef.current) {
      markerRef.current = L.marker(point, {
        icon: L.divIcon({
          className: 'rider-marker',
          html: '↗',
          iconSize: [38, 38],
          iconAnchor: [19, 19],
        }),
      }).addTo(mapRef.current);
      mapRef.current.setView(point, 16);
    } else markerRef.current.setLatLng(point);
    trailRef.current = [...trailRef.current.slice(-99), point];
    lineRef.current.setLatLngs(trailRef.current);
  }, [location]);

  return (
    <div className="map-wrap">
      <div id="map" ref={container} aria-label="Rider location map" />
      {!location && (
        <div className="map-empty">
          <span>⌖</span>
          <strong>Your rider will appear here</strong>
          <p>
            {orderId
              ? 'Waiting for the first location update.'
              : 'Enter an order ID to start tracking.'}
          </p>
        </div>
      )}
      <button
        className="recenter"
        type="button"
        aria-label="Center map on rider"
        onClick={() => {
          if (location) mapRef.current?.setView([location.latitude, location.longitude], 16);
        }}
      >
        ⌖
      </button>
    </div>
  );
}
