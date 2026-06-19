const CACHE = 'eduagent-v1';
const ASSETS = [
  '/index.html',
  '/manifest.json',
  '/assets/eduagent.jpg',
  '/assets/white-knights.jpg',
  '/assets/nextlevel.jpg',
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(ASSETS)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  // Don't intercept API calls
  if (e.request.url.includes('127.0.0.1:8000')) return;
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  );
});