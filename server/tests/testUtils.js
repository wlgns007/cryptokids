import assert from 'node:assert/strict';

const fetch = globalThis.fetch;

function masterCookieValue(masterKey = process.env.MASTER_ADMIN_KEY) {
  const key = masterKey ?? '';
  return `ck_admin_key=${encodeURIComponent(key)}`;
}

export async function getFirstActiveFamilyId(baseUrl, masterKey = process.env.MASTER_ADMIN_KEY) {
  assert.ok(baseUrl, 'baseUrl is required to fetch families');
  const url = new URL('/api/admin/families?status=active', baseUrl);
  const res = await fetch(url, {
    method: 'GET',
    headers: {
      cookie: masterCookieValue(masterKey),
    },
  });
  assert.equal(res.status, 200, 'families listing should succeed for master admin');
  const rows = await res.json();
  assert.ok(Array.isArray(rows), 'families listing should return an array');
  assert.ok(rows.length > 0, 'at least one active family should exist');
  return rows[0].id;
}

export function buildMasterCookie(masterKey = process.env.MASTER_ADMIN_KEY) {
  return masterCookieValue(masterKey);
}
