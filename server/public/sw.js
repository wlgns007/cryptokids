const CACHE = 'ck-v2'; // bump version to refresh SW
const ASSETS = [
  '/', '/admin.html', '/child.html',
  '/admin.js', '/qrcode.min.js',
  '/manifest.webmanifest',
  '/icon-192.png', '/icon-512.png' // keep if you added them
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
