const APP_CACHE = 'cleverkids-app-v3';
const RUNTIME_CACHE = 'cleverkids-runtime-v3';

self.skipWaiting();

self.addEventListener('install', (event) => {
  event.waitUntil(Promise.all([caches.open(APP_CACHE), caches.open(RUNTIME_CACHE)]));
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(keys.filter((key) => ![APP_CACHE, RUNTIME_CACHE].includes(key)).map((key) => caches.delete(key)));
      await self.clients.claim();
    })()
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/')) return;
  if (url.pathname.startsWith('/admin')) return;

  event.respondWith(
    (async () => {
      const cached = await caches.match(request);
      if (cached) {
        event.waitUntil(updateRuntimeCache(request));
        return cached;
      }
      const response = await fetch(request).catch(() => null);
      if (response && response.ok) {
        event.waitUntil(storeInRuntimeCache(request, response.clone()));
      }
      return response || caches.match('/offline.html');
    })()
  );
});

async function updateRuntimeCache(request) {
  try {
    const response = await fetch(request, { cache: 'no-store' });
    if (response && response.ok) {
      await storeInRuntimeCache(request, response.clone());
    }
  } catch (error) {
    // ignore network errors during background refresh
  }
}

async function storeInRuntimeCache(request, response) {
  try {
    const cache = await caches.open(RUNTIME_CACHE);
    await cache.put(request, response);
  } catch (error) {
    // ignore cache write failures
  }
}
