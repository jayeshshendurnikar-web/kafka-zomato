import { useEffect, useState } from 'react';
import { useCustomerTracking } from './hooks/useCustomerTracking.js';
import { useRiderSharing } from './hooks/useRiderSharing.js';
import { CustomerForm, RiderForm } from './components/TrackingForms.jsx';
import TrackingPanel from './components/TrackingPanel.jsx';

export default function App() {
  const [mode, setMode] = useState('customer');
  const [settings, setSettings] = useState(null);
  const [configError, setConfigError] = useState('');
  const [customer, setCustomer] = useState(null);
  const [rider, setRider] = useState(null);
  const { location, status } = useCustomerTracking(customer);
  const riderStatus = useRiderSharing(rider, settings?.minIntervalMs ?? 3000);

  useEffect(() => {
    const controller = new AbortController();
    const apiUrl = import.meta.env.VITE_API_URL?.replace(/\/$/, '') || '';
    fetch(`${apiUrl}/api/config`, { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error('Could not load settings. Refresh to retry.');
        setSettings(await response.json());
      })
      .catch((error) => {
        if (!controller.signal.aborted) setConfigError(error.message);
      });
    return () => controller.abort();
  }, []);

  return (
    <>
      <header className="topbar">
        <a className="brand" href="/" aria-label="On the way home">
          <span className="brand-icon">↗</span> ontheway<span className="brand-dot">.</span>
        </a>
        <span className="header-label">A little closer, every moment</span>
        <span className="pill">{settings?.demo ? 'LOCAL DEMO' : 'DELIVERY TRACKING'}</span>
      </header>
      <main>
        <div className="heading">
          <div>
            <p className="eyebrow">FROM THEIR JOURNEY TO YOUR DOOR</p>
            <h1>Your delivery. In motion.</h1>
            <p className="subtitle">Follow your rider’s location as it happens.</p>
          </div>
          <div className="live-badge">
            <span /> LIVE TRACKING
          </div>
        </div>
        <div className="workspace">
          <aside className="panel controls">
            <div className="tabs" role="tablist" aria-label="App mode">
              {['customer', 'rider'].map((name) => (
                <button
                  key={name}
                  id={`${name}-tab`}
                  className={`tab ${mode === name ? 'active' : ''}`}
                  role="tab"
                  aria-selected={mode === name}
                  aria-controls={`${name}-panel`}
                  onClick={() => setMode(name)}
                >
                  {name === 'customer' ? 'Customer' : 'Rider'}
                </button>
              ))}
            </div>
            <section
              id="customer-panel"
              role="tabpanel"
              aria-labelledby="customer-tab"
              hidden={mode !== 'customer'}
            >
              <CustomerForm
                demo={settings?.demo}
                enabled={Boolean(settings)}
                active={Boolean(customer)}
                onStart={setCustomer}
                onStop={() => setCustomer(null)}
                status={configError || status}
              />
            </section>
            <section
              id="rider-panel"
              role="tabpanel"
              aria-labelledby="rider-tab"
              hidden={mode !== 'rider'}
            >
              <RiderForm
                demo={settings?.demo}
                enabled={Boolean(settings)}
                active={Boolean(rider)}
                onStart={setRider}
                onStop={() => setRider(null)}
                status={configError || riderStatus}
              />
            </section>
          </aside>
          <TrackingPanel location={location} orderId={customer?.orderId} status={status} />
        </div>
        <footer>
          <span className="footer-mark">↗</span> A smoother journey, from pickup to doorstep.
          <span className="footer-right">Made for the moments in between.</span>
        </footer>
      </main>
    </>
  );
}
