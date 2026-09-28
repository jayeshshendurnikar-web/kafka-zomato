export function createApi({ token = '', fetchImpl = fetch, baseUrl = '' } = {}) {
  async function request(orderId, options = {}) {
    const response = await fetchImpl(
      `${baseUrl}/api/orders/${encodeURIComponent(orderId)}/location`,
      {
        ...options,
        signal: options.signal
          ? AbortSignal.any([options.signal, AbortSignal.timeout(10000)])
          : AbortSignal.timeout(10000),
        cache: 'no-store',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
      },
    );
    const body = await response.json();
    if (!response.ok) {
      const error = new Error(body.error || 'Request failed');
      error.status = response.status;
      throw error;
    }
    return body;
  }
  return {
    latest: (orderId, signal) => request(orderId, { signal }),
    send: (location, signal) =>
      request(location.orderId, { method: 'POST', body: JSON.stringify(location), signal }),
  };
}
