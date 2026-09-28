import { config } from '../config/index.js';
import { issueToken } from '../auth/tokens.js';
import { validateId } from '../api/validation.js';

const [role, orderId, riderId] = process.argv.slice(2);
if (!['rider', 'customer'].includes(role) || config.auth.secret.length < 32) {
  throw new Error(
    'Set TRACKING_TOKEN_SECRET (32+ characters). Usage: npm run token -- rider|customer orderId [riderId]',
  );
}
validateId(orderId);
if (role === 'rider') validateId(riderId, 'riderId');
console.log(
  issueToken(
    {
      role,
      orderId,
      ...(role === 'rider' ? { riderId } : {}),
      exp: Math.floor(Date.now() / 1000) + 3600,
    },
    config.auth.secret,
  ),
);
