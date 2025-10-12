const CK_SW_VERSION = 'v1.0.0';

self.addEventListener('install', (event) => {
  console.log(`[CK Wallet SW] install ${CK_SW_VERSION}`);
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  console.log(`[CK Wallet SW] activate ${CK_SW_VERSION}`);
  event.waitUntil(self.clients.claim());
});
