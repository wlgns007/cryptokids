const CACHE = 'ck-v3-__BUILD__';
const ASSETS = [
  `/admin.html?v=__BUILD__`,
  `/child.html?v=__BUILD__`,
  `/admin.js?v=__BUILD__`,
  `/child.js?v=__BUILD__`,
  `/qrcode.min.js?v=__BUILD__`,
  `/manifest.webmanifest?v=__BUILD__`,
  '/icon-192.png',
  '/icon-512.png'
];

self.addEventListener('install', (e) => {
  e.waitUntil((async () => {
    const cache = await caches.open(CACHE);
    // add individually so one 404 doesn't break install
    const results = await Promise.allSettled(ASSETS.map(u => cache.add(u)));
    // optional: log misses (dev only)
    // results.forEach((r,i)=>{ if(r.status==='rejected') console.warn('Cache miss:', ASSETS[i]); });
  })());
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(keys.map(k => k !== CACHE && caches.delete(k))))
  );
});

self.addEventListener('fetch', (e) => {
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  );
});
