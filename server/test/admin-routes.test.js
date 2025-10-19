import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import { createTestContext } from './setup.js';
import { seedBaselineData, families, members, MASTER_KEY } from './fixtures.js';
import './teardown.js';

await createTestContext();

test.beforeEach(async () => {
  await seedBaselineData();
});

test('master admin can list members for any family', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({ 'x-admin-key': MASTER_KEY });

  const response = await client.get(`/api/admin/families/${families.tang.id}/members`);
  assert.equal(response.status, 200);
  assert.ok(Array.isArray(response.body?.members));
  assert.equal(response.body.members.length, 1);
  assert.equal(response.body.members[0].id, members.tangKid.id);
});

test('family admin can list members for their own family', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({ 'x-admin-key': families.tang.adminKey });

  const response = await client.get(`/api/admin/families/${families.tang.id}/members`);
  assert.equal(response.status, 200);
  assert.equal(response.body.members.length, 1);
  assert.equal(response.body.members[0].name, members.tangKid.name);
});

test('family admin forbidden from other families', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({ 'x-admin-key': families.tang.adminKey });

  await assert.rejects(async () => {
    await client.get(`/api/admin/families/${families.jang.id}/members`);
  }, (error) => {
    assert.equal(error.status, 403);
    assert.deepEqual(error.body, { error: 'forbidden_family_scope' });
    return true;
  });
});

test('unknown admin key returns 401', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({ 'x-admin-key': 'bad-key' });

  await assert.rejects(async () => {
    await client.get(`/api/admin/families/${families.tang.id}/members`);
  }, (error) => {
    assert.equal(error.status, 401);
    assert.deepEqual(error.body, { error: 'invalid_admin_key' });
    return true;
  });
});

test('missing family id returns 404', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({ 'x-admin-key': MASTER_KEY });
  const missingId = randomUUID();

  await assert.rejects(async () => {
    await client.get(`/api/admin/families/${missingId}/members`);
  }, (error) => {
    assert.equal(error.status, 404);
    assert.deepEqual(error.body, { error: 'family_not_found' });
    return true;
  });
});

test('family panel endpoints enforce family existence', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({ 'x-admin-key': MASTER_KEY });
  const missingId = randomUUID();

  for (const path of ['tasks', 'rewards', 'holds']) {
    await assert.rejects(async () => {
      await client.get(`/api/admin/families/${missingId}/${path}`);
    }, (error) => {
      assert.equal(error.status, 404);
      assert.deepEqual(error.body, { error: 'family_not_found' });
      return true;
    });
  }
});

test('scoped panel endpoints return arrays for tasks, rewards, and holds', async () => {
  const { createClient } = await createTestContext();
  const client = createClient({
    'x-admin-key': MASTER_KEY,
    'x-family': families.tang.id,
  });

  const tasks = await client.get(`/api/admin/families/${families.tang.id}/tasks`);
  assert.equal(tasks.status, 200);
  assert.ok(Array.isArray(tasks.body));
  assert.equal(tasks.body.length, 1);

  const rewards = await client.get(`/api/admin/families/${families.tang.id}/rewards`);
  assert.equal(rewards.status, 200);
  assert.ok(Array.isArray(rewards.body));
  assert.equal(rewards.body.length, 1);

  const holds = await client.get(`/api/admin/families/${families.tang.id}/holds`);
  assert.equal(holds.status, 200);
  assert.ok(Array.isArray(holds.body));
  assert.equal(holds.body.length, 1);
});
