/* SmartDine service worker: app-shell cache only.
   /api responses are never cached: they carry per-session financial data and
   must always come from the live server. Offline ordering uses the IndexedDB
   queue in db.ts, not the SW cache. */
const CACHE = 'smartdine-v2';
const SHELL = ['/', '/index.html', '/manifest.webmanifest', '/alpha-blue.png'];
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys()
    .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  if (url.pathname.startsWith('/api/')) return; // always network
  e.respondWith(
    caches.match(e.request, { ignoreSearch: true }).then(hit => hit || fetch(e.request).then(res => {
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(e.request, copy));
      return res;
    }).catch(() => caches.match('/index.html')))
  );
});
