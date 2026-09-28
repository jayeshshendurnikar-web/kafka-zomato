import { createHmac, timingSafeEqual } from 'node:crypto';
import { HttpError } from '../lib/errors.js';

const sign = (payload, secret) => createHmac('sha256', secret).update(payload).digest('base64url');

export function issueToken(claims, secret) {
  const payload = Buffer.from(JSON.stringify(claims)).toString('base64url');
  return `${payload}.${sign(payload, secret)}`;
}

export function createAuthorizer({ demo, secret }) {
  return (token, { role, orderId, riderId }) => {
    if (demo) return { role, orderId, riderId, exp: Math.floor(Date.now() / 1000) + 3600 };
    try {
      if (typeof token !== 'string' || token.length > 2048) throw new Error();
      const [payload, signature, extra] = token.split('.');
      if (!payload || !signature || extra !== undefined) throw new Error();
      const actual = Buffer.from(signature);
      const expected = Buffer.from(sign(payload, secret));
      if (actual.length !== expected.length || !timingSafeEqual(actual, expected))
        throw new Error();
      const claims = JSON.parse(Buffer.from(payload, 'base64url').toString());
      if (!Number.isSafeInteger(claims.exp) || claims.exp <= Date.now() / 1000) throw new Error();
      if (
        claims.role !== role ||
        claims.orderId !== orderId ||
        (role === 'rider' && claims.riderId !== riderId)
      )
        throw new Error();
      return claims;
    } catch {
      throw new HttpError(403, 'Invalid, expired or unauthorized tracking token');
    }
  };
}

export const bearerToken = (request) => request.headers.authorization?.replace(/^Bearer /, '');
