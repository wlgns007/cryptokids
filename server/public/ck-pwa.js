(() => {
  const CK_SW_VERSION = '1.0.0';
  const INSTALL_SUPPRESS_MS = 7 * 24 * 60 * 60 * 1000;
  const EVENT_STORE_KEY = 'ck_pwa_events_v1';
  const SRC_KEY = 'ck_pwa_src';
  const IOS_TIP_KEY = 'ck_pwa_ios_tip';
  const INSTALL_SUPPRESS_KEY = 'ck_pwa_install_suppress_until';
  const SUMMARY_FLAG_KEY = 'ck_pwa_summary_logged';

  const KNOWN_SOURCES = new Set(['pwa', 'qr', 'desktop']);
  const DEFAULT_SOURCE = 'desktop';

  const now = () => Date.now();

  function safeStorage() {
    try {
      if (!window.localStorage || !window.sessionStorage) return null;
      return { local: window.localStorage, session: window.sessionStorage };
    } catch (error) {
      console.warn('[CK Wallet] storage unavailable', error);
      return null;
    }
  }

  function isStandalone() {
    const mql = window.matchMedia ? window.matchMedia('(display-mode: standalone)') : null;
    return (mql && mql.matches) || window.navigator.standalone === true;
  }

  function readStoredSource() {
    const storage = safeStorage();
    if (!storage) return null;
    const values = [
      storage.session?.getItem(SRC_KEY),
      storage.local?.getItem(SRC_KEY)
    ];
    for (const value of values) {
      if (value && KNOWN_SOURCES.has(value)) return value;
    }
    return null;
  }

  function persistSource(src) {
    const storage = safeStorage();
    if (!storage) return;
    try {
      storage.session?.setItem(SRC_KEY, src);
      storage.local?.setItem(SRC_KEY, src);
    } catch (error) {
      console.warn('[CK Wallet] failed to persist src', error);
    }
  }

  function resolveSource(defaultGuess = DEFAULT_SOURCE) {
    let src = null;
    try {
      const params = new URLSearchParams(window.location.search);
      const candidate = (params.get('src') || '').toLowerCase();
      if (KNOWN_SOURCES.has(candidate)) src = candidate;
    } catch (error) {
      console.warn('[CK Wallet] unable to read src query', error);
    }

    if (!src) {
      src = readStoredSource();
    }

    if (isStandalone()) {
      src = 'pwa';
    }

    if (!src || !KNOWN_SOURCES.has(src)) {
      src = defaultGuess;
    }

    persistSource(src);
    return src;
  }

  function readEvents() {
    const storage = safeStorage();
    if (!storage) return [];
    try {
      const raw = storage.local.getItem(EVENT_STORE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      console.warn('[CK Wallet] unable to read PWA events', error);
      return [];
    }
  }

  function writeEvents(events) {
    const storage = safeStorage();
    if (!storage) return;
    try {
      storage.local?.setItem(EVENT_STORE_KEY, JSON.stringify(events));
    } catch (error) {
      console.warn('[CK Wallet] unable to persist PWA events', error);
    }
  }

  function trimEvents(events, keepMs = 30 * 24 * 60 * 60 * 1000) {
    const cutoff = now() - keepMs;
    const filtered = events.filter((event) => Number(event.ts) >= cutoff);
    if (filtered.length > 400) {
      return filtered.slice(filtered.length - 400);
    }
    return filtered;
  }

  function trackEvent(name, { src, meta } = {}) {
    const ts = now();
    const source = KNOWN_SOURCES.has(src) ? src : resolveSource();
    const payload = { name, src: source, ts, meta: meta || null };
    const events = trimEvents([...readEvents(), payload]);
    writeEvents(events);
    console.info('[CK Wallet] event', name, { src: source, at: new Date(ts).toISOString(), meta });
    return payload;
  }

  function logWeeklySummary() {
    const storage = safeStorage();
    if (!storage) return;
    try {
    const alreadyLogged = storage.session?.getItem(SUMMARY_FLAG_KEY);
    if (alreadyLogged) return;
    storage.session?.setItem(SUMMARY_FLAG_KEY, '1');
    } catch (error) {
      console.warn('[CK Wallet] unable to memoize summary log', error);
    }

    const events = readEvents();
    if (!events.length) return;
    const cutoff = now() - 7 * 24 * 60 * 60 * 1000;
    const summaryMap = new Map();
    for (const event of events) {
      if (Number(event.ts) < cutoff) continue;
      const key = event.src || 'unknown';
      if (!summaryMap.has(key)) {
        summaryMap.set(key, { source: key, pwa_view: 0, pwa_install_click: 0, pwa_installed: 0, pwa_open_standalone: 0 });
      }
      const entry = summaryMap.get(key);
      if (entry[event.name] !== undefined) {
        entry[event.name] += 1;
      }
    }
    if (!summaryMap.size) return;
    const rows = Array.from(summaryMap.values());
    if (console.table) {
      console.groupCollapsed('[CK Wallet] 7-day PWA funnel');
      console.table(rows);
      console.groupEnd();
    } else {
      console.log('[CK Wallet] 7-day PWA funnel', rows);
    }
  }

  function registerServiceWorker(version = CK_SW_VERSION) {
    if (!('serviceWorker' in navigator)) {
      console.info('[CK Wallet] service worker unsupported');
      return;
    }
    const url = `/sw.js?v=${encodeURIComponent(version)}`;
    navigator.serviceWorker
      .register(url)
      .then((registration) => {
        console.info('[CK Wallet] service worker registered', version, registration.scope);
      })
      .catch((error) => {
        console.error('[CK Wallet] service worker registration failed', error);
      });
  }

  function shouldSuppressPrompt() {
    const storage = safeStorage();
    if (!storage) return false;
    const raw = storage.local?.getItem(INSTALL_SUPPRESS_KEY);
    if (!raw) return false;
    const until = Number(raw);
    return Number.isFinite(until) && until > now();
  }

  function rememberPromptSuppression() {
    const storage = safeStorage();
    if (!storage) return;
    try {
      storage.local?.setItem(INSTALL_SUPPRESS_KEY, String(now() + INSTALL_SUPPRESS_MS));
    } catch (error) {
      console.warn('[CK Wallet] unable to save prompt suppression', error);
    }
  }

  function clearPromptSuppression() {
    const storage = safeStorage();
    if (!storage) return;
    try {
      storage.local?.removeItem(INSTALL_SUPPRESS_KEY);
    } catch (error) {
      console.warn('[CK Wallet] unable to clear suppression', error);
    }
  }

  function setupInstallPrompt({ button, src, fallbackElement } = {}) {
    if (!button) return () => {};

    let deferredPrompt = null;
    button.hidden = true;
    button.setAttribute('aria-hidden', 'true');

    const hideButton = () => {
      button.hidden = true;
      button.setAttribute('aria-hidden', 'true');
    };

    const showButton = () => {
      button.hidden = false;
      button.removeAttribute('aria-hidden');
    };

    const handleBeforeInstallPrompt = (event) => {
      event.preventDefault();
      if (shouldSuppressPrompt() || isStandalone()) {
        return;
      }
      deferredPrompt = event;
      showButton();
    };

    window.addEventListener('beforeinstallprompt', handleBeforeInstallPrompt);

    button.addEventListener('click', async () => {
      trackEvent('pwa_install_click', { src });
      if (!deferredPrompt) {
        if (fallbackElement) {
          fallbackElement.hidden = false;
        }
        return;
      }

      deferredPrompt.prompt();
      try {
        const choice = await deferredPrompt.userChoice;
        if (choice && choice.outcome === 'accepted') {
          clearPromptSuppression();
        } else {
          rememberPromptSuppression();
          hideButton();
        }
      } catch (error) {
        console.warn('[CK Wallet] install prompt error', error);
      }
      deferredPrompt = null;
    });

    window.addEventListener('appinstalled', () => {
      hideButton();
      clearPromptSuppression();
      trackEvent('pwa_installed', { src });
    });

    const mql = window.matchMedia ? window.matchMedia('(display-mode: standalone)') : null;
    if (mql && typeof mql.addEventListener === 'function') {
      mql.addEventListener('change', (event) => {
        if (event.matches) hideButton();
      });
    }

    if (!('onbeforeinstallprompt' in window)) {
      if (fallbackElement) fallbackElement.hidden = false;
    }

    return () => {
      window.removeEventListener('beforeinstallprompt', handleBeforeInstallPrompt);
    };
  }

  function isIosSafari() {
    const ua = window.navigator.userAgent || '';
    const isIOS = /iPad|iPhone|iPod/.test(ua);
    if (!isIOS) return false;
    const isSafari = /Safari/.test(ua) && !/CriOS|FxiOS|OPiOS|EdgiOS/.test(ua);
    return isSafari;
  }

  function setupIosTip({ tip, dismiss, src }) {
    if (!tip) return;
    if (!isIosSafari() || isStandalone()) {
      tip.hidden = true;
      return;
    }
    const storage = safeStorage();
    const alreadySeen = storage?.local?.getItem(IOS_TIP_KEY);
    if (alreadySeen) {
      tip.hidden = true;
      return;
    }
    tip.hidden = false;
    if (dismiss) {
      dismiss.addEventListener('click', () => {
        tip.hidden = true;
        try {
          storage?.local?.setItem(IOS_TIP_KEY, String(now()));
        } catch (error) {
          console.warn('[CK Wallet] unable to remember iOS tip dismissal', error);
        }
      });
    }
  }

  function applySourceToLinks(src) {
    document.querySelectorAll('a[data-keep-src]').forEach((anchor) => {
      try {
        const href = anchor.getAttribute('href');
        if (!href) return;
        const url = new URL(href, window.location.origin);
        url.searchParams.set('src', src);
        const formatted = url.origin === window.location.origin
          ? `${url.pathname}${url.search}${url.hash}`
          : url.toString();
        anchor.setAttribute('href', formatted);
      } catch (error) {
        console.warn('[CK Wallet] unable to decorate link', error);
      }
    });
  }

  function initBase({ defaultSource = DEFAULT_SOURCE } = {}) {
    const src = resolveSource(defaultSource);
    trackEvent('pwa_view', { src });
    if (isStandalone()) {
      trackEvent('pwa_open_standalone', { src: 'pwa' });
    }
    logWeeklySummary();
    return src;
  }

  function initAppShell(options = {}) {
    const src = initBase({ defaultSource: DEFAULT_SOURCE });
    applySourceToLinks(src);
    registerServiceWorker(options.swVersion || CK_SW_VERSION);

    const button = options.installButtonSelector
      ? document.querySelector(options.installButtonSelector)
      : options.installButton || null;
    const fallback = options.noPromptSelector ? document.querySelector(options.noPromptSelector) : null;
    setupInstallPrompt({ button, src, fallbackElement: fallback });

    const iosTip = options.iosTipSelector ? document.querySelector(options.iosTipSelector) : null;
    const iosDismiss = options.iosDismissSelector ? document.querySelector(options.iosDismissSelector) : null;
    if (iosTip) {
      const storage = safeStorage();
      const alreadySeen = storage?.local?.getItem(IOS_TIP_KEY);
      if (!alreadySeen) {
        setupIosTip({ tip: iosTip, dismiss: iosDismiss, src });
      } else {
        iosTip.hidden = true;
      }
    }
  }

  function initInstallPage(options = {}) {
    const src = initBase({ defaultSource: 'qr' });
    applySourceToLinks(src);

    if (isStandalone()) {
      const target = `/?src=${encodeURIComponent(src === 'qr' ? 'pwa' : src)}`;
      if (window.location.pathname !== '/' || window.location.search !== `?src=${encodeURIComponent(src)}`) {
        window.location.replace(target);
      }
      return;
    }

    registerServiceWorker(options.swVersion || CK_SW_VERSION);

    const button = options.installButtonSelector
      ? document.querySelector(options.installButtonSelector)
      : options.installButton || null;
    const fallback = options.noPromptSelector ? document.querySelector(options.noPromptSelector) : null;
    setupInstallPrompt({ button, src, fallbackElement: fallback });

    const iosTip = options.iosTipSelector ? document.querySelector(options.iosTipSelector) : null;
    const iosDismiss = options.iosDismissSelector ? document.querySelector(options.iosDismissSelector) : null;
    setupIosTip({ tip: iosTip, dismiss: iosDismiss, src });
  }

  window.CKPWA = {
    initAppShell,
    initInstallPage,
    resolveSource,
    isStandalone,
    trackEvent,
  };
})();
