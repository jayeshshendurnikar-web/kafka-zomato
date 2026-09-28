import { useEffect, useState } from 'react';

function Field({ id, label, ...props }) {
  return (
    <>
      <label htmlFor={id}>{label}</label>
      <input id={id} {...props} />
    </>
  );
}

function OrderFields({ role, demo, disabled = false }) {
  return (
    <>
      <Field
        id={`${role}-order`}
        name="orderId"
        label="Order ID"
        defaultValue="ORDER-1001"
        disabled={disabled}
        required
        pattern="[a-zA-Z0-9_-]{1,80}"
        maxLength={80}
      />
      {role === 'rider' && (
        <Field
          id="rider-id"
          name="riderId"
          label="Rider ID"
          defaultValue="RIDER-01"
          disabled={disabled}
          required
          pattern="[a-zA-Z0-9_-]{1,80}"
          maxLength={80}
        />
      )}
      {!demo && (
        <Field
          id={`${role}-token`}
          name="token"
          label={`${role === 'rider' ? 'Rider' : 'Customer'} tracking token`}
          type="password"
          disabled={disabled}
          autoComplete="off"
          placeholder="Paste your order token"
          required
        />
      )}
    </>
  );
}

export function Status({ children }) {
  return (
    <div className="status-box">
      <span className="status-dot" />
      <p role="status" aria-live="polite">
        {children}
      </p>
    </div>
  );
}

const formValues = (event) => {
  event.preventDefault();
  return Object.fromEntries(
    [...new FormData(event.currentTarget)].map(([key, value]) => [key, value.trim()]),
  );
};

export function CustomerForm({ demo, enabled, active, onStart, onStop, status }) {
  return (
    <>
      <div className="section-icon">⌖</div>
      <h2>Follow your order</h2>
      <p className="muted">Your rider’s latest location, all the way to your door.</p>
      <form onSubmit={(event) => onStart(formValues(event))}>
        <OrderFields role="customer" demo={demo} />
        <button className="primary" type="submit" disabled={!enabled}>
          {active ? 'Reconnect delivery' : 'Track my delivery'} <span>→</span>
        </button>
        {active && (
          <button className="secondary" type="button" onClick={onStop}>
            Disconnect
          </button>
        )}
      </form>
      <Status>{status}</Status>
      <div className="tip">
        <strong>Always up to date</strong>
        <p>
          If your connection drops, tracking reconnects and picks up the latest location
          automatically.
        </p>
      </div>
    </>
  );
}

export function RiderForm({ demo, enabled, active, onStart, onStop, status }) {
  const [source, setSource] = useState('gps');
  useEffect(() => {
    setSource(demo ? 'demo' : 'gps');
  }, [demo]);
  return (
    <>
      <div className="section-icon">↗</div>
      <h2>Share your journey</h2>
      <p className="muted">Send your location while you’re on a delivery.</p>
      <form onSubmit={(event) => onStart(formValues(event))}>
        <OrderFields role="rider" demo={demo} disabled={active} />
        <label htmlFor="source">Location source</label>
        <select
          id="source"
          name="source"
          value={source}
          onChange={(event) => setSource(event.target.value)}
          disabled={active}
        >
          <option value="gps">My device GPS</option>
          <option value="demo">Demo ride · New Delhi</option>
        </select>
        <button className="primary" type="submit" disabled={!enabled || active}>
          Start sharing <span>↗</span>
        </button>
        {active && (
          <button className="secondary" type="button" onClick={onStop}>
            Stop sharing
          </button>
        )}
      </form>
      <Status>{status}</Status>
      {active && (
        <p className="small muted">
          Stop sharing before changing the rider or order. Use another tab for a second rider.
        </p>
      )}
      <p className="small muted">
        Keep this page open. GPS requires HTTPS or localhost and your location permission.
      </p>
    </>
  );
}
