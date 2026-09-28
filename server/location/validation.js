import { HttpError } from '../lib/errors.js';

export function validateId(value, name = 'orderId') {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(value)) {
    throw new HttpError(400, `${name} must contain 1–80 letters, numbers, underscores or hyphens`);
  }
  return value;
}

export function validateLocation(
  input,
  { now = Date.now(), maxAgeMs = Infinity, maxFutureMs = 30000 } = {},
) {
  if (!input || typeof input !== 'object' || Array.isArray(input))
    throw new HttpError(400, 'Location must be an object');
  const { orderId, riderId, latitude, longitude, timestamp } = input;
  validateId(orderId);
  validateId(riderId, 'riderId');
  if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90)
    throw new HttpError(400, 'Invalid latitude');
  if (!Number.isFinite(longitude) || longitude < -180 || longitude > 180)
    throw new HttpError(400, 'Invalid longitude');
  if (
    !Number.isSafeInteger(timestamp) ||
    timestamp <= 0 ||
    timestamp > now + maxFutureMs ||
    timestamp < now - maxAgeMs
  ) {
    throw new HttpError(400, 'timestamp must be Unix milliseconds within the accepted time window');
  }
  return { orderId, riderId, latitude, longitude, timestamp };
}
