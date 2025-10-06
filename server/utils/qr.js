// utils/qr.js (ESM)
import crypto from 'node:crypto';

function randomId() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString('hex');
}

export function signQR(dataObj, key, ttlMs = 60_000) {
  const jti = randomId();
  const exp = Date.now() + ttlMs;
  const payload = { ...dataObj, jti, exp };        // e.g., {kind:'earn', user:'child:rio', amt:5, task:'brushteth'}
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig  = crypto.createHmac('sha256', key).update(body).digest('base64url');
  return `${body}.${sig}`;                          // token to embed in QR
}

export function verifyQR(token, key) {
  const [body, sig] = token.split('.');
  if (!body || !sig) throw new Error('bad_token');
  const expected = crypto.createHmac('sha256', key).update(body).digest('base64url');
  if (sig !== expected) throw new Error('bad_sig');
  const payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  if (Date.now() > payload.exp) throw new Error('expired');
  return payload;                                   // {kind,user,amt,task?,jti,exp}
}
