const APP_CACHE = 'cleverkids-app-v1';
const RUNTIME_CACHE = 'cleverkids-runtime-v1';

self.addEventListener('install', (event) => {
  event.waitUntil(Promise.all([caches.open(APP_CACHE), caches.open(RUNTIME_CACHE)]));
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(keys.filter((key) => ![APP_CACHE, RUNTIME_CACHE].includes(key)).map((key) => caches.delete(key)))
      )
  );
  clients.claim();
});
