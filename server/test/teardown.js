import test from 'node:test';
import { getActiveContext } from './setup.js';

test.after(async () => {
  const context = await getActiveContext();
  if (context) {
    await context.close();
  }
});
