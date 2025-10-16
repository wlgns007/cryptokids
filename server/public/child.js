import { renderHeader } from './js/header.js';

const SUPPORTED_LANGS = ['en', 'ko'];
let ckI18nApi = {};

function syncHeaderLangButtons(active) {
  document
    .querySelectorAll('#lang-controls button[data-lang]')
    .forEach((btn) => {
      const isActive = btn.dataset.lang === active;
      btn.classList.toggle('active', isActive);
      btn.setAttribute('aria-pressed', String(isActive));
    });
}

function readStoredLang(getLangFn) {
  if (typeof getLangFn === 'function') {
    const current = getLangFn();
    if (SUPPORTED_LANGS.includes(current)) return current;
  }
  try {
    const stored = window.localStorage?.getItem('ck.lang');
    if (stored && SUPPORTED_LANGS.includes(stored)) return stored;
  } catch (error) {
    console.warn('Unable to read stored language', error);
  }
  return SUPPORTED_LANGS[0];
}

function translate(key, fallback = key) {
  const translator = ckI18nApi.t || (window.ckI18n && window.ckI18n.t);
  return typeof translator === 'function' ? translator(key) : fallback;
}

document.addEventListener('DOMContentLoaded', () => {
  console.log('child.js loaded ok');

  const { setLang: setLangGlobal, applyAdminTranslations, getLang, t } = window.ckI18n || {};
  if (typeof setLangGlobal !== 'function' || typeof applyAdminTranslations !== 'function') {
    console.error('i18n not loaded before child.js');
    return;
  }

  ckI18nApi = { setLang: setLangGlobal, applyAdminTranslations, getLang, t };

  applyAdminTranslations(document);

  const titleEl = document.querySelector('[data-i18n="app.title"]');
  if (titleEl && typeof t === 'function') {
    titleEl.textContent = t('app.title');
  }

  function handleSetLang(code) {
    const normalized = SUPPORTED_LANGS.includes(code) ? code : SUPPORTED_LANGS[0];
    setLangGlobal(normalized);
    syncHeaderLangButtons(normalized);
    return normalized;
  }

  const bindLangButton = (lang) => {
    const btn = document.querySelector(`[data-lang="${lang}"]`);
    if (!btn || btn.dataset.i18nBound) return;
    btn.dataset.i18nBound = 'true';
    btn.addEventListener('click', () => handleSetLang(lang));
  };

  bindLangButton('en');
  bindLangButton('ko');

  renderHeader({
    mountId: 'app-header',
    langs: SUPPORTED_LANGS,
    onLangChange: handleSetLang,
    variant: 'band',
    showInstall: true
  });

  bindLangButton('en');
  bindLangButton('ko');

  const initialLang = readStoredLang(getLang);
  handleSetLang(initialLang);

  window.setLang = handleSetLang;
});

function getYouTubeId(url) {
  if (!url) return "";
  try {
    const u = new URL(url);
    if (u.hostname.includes("youtu.be")) return u.pathname.slice(1);
    if (u.searchParams.has("v")) return u.searchParams.get("v");
    const parts = u.pathname.split("/").filter(Boolean);
    const idx = parts.findIndex((p) => p === "embed" || p === "shorts");
    if (idx !== -1 && parts[idx + 1]) return parts[idx + 1];
    return "";
  } catch {
    const m = String(url).match(/(?:v=|\/embed\/|youtu\.be\/|\/shorts\/)([A-Za-z0-9_\-]{6,})/);
    return m ? m[1] : "";
  }
}

function getYouTubeThumbnail(url) {
  const id = getYouTubeId(url);
  return id ? `https://img.youtube.com/vi/${id}/hqdefault.jpg` : "";
}

function getYouTubeEmbed(url, { host = "www.youtube.com", autoplay = true } = {}) {
  const id = getYouTubeId(url);
  if (!id) return "";
  const params = new URLSearchParams({
    modestbranding: "1",
    rel: "0",
    playsinline: "1",
  });
  if (autoplay) params.set("autoplay", "1");
  return `https://${host}/embed/${id}?${params.toString()}`;
}

function isLikelyVerticalYouTube(url) {
  if (!url) return false;
  const base = typeof window !== "undefined" ? window.location.origin : "https://youtube.com";
  try {
    const parsed = new URL(url, base);
    if (parsed.pathname?.toLowerCase().includes("/shorts/")) return true;
    for (const value of parsed.searchParams.values()) {
      if (String(value).toLowerCase().includes("shorts")) return true;
    }
    return false;
  } catch {
    return String(url).toLowerCase().includes("shorts");
  }
}

window.getYouTubeId = getYouTubeId;
window.getYouTubeThumbnail = getYouTubeThumbnail;
window.getYouTubeEmbed = getYouTubeEmbed;
window.isLikelyVerticalYouTube = isLikelyVerticalYouTube;

(() => {
  const $ = (id) => document.getElementById(id);
  const LS_FILTER = 'ck_child_filters';
  const RECENT_REDEEM_LIMIT = 50;
  const RECENT_REDEEM_DISPLAY = 5;
  const FULL_REDEEM_LIMIT = 200;
  let lastRedeemEntry = null;
  let recentRedeemsVisible = false;
  let fullRedeemsVisible = false;

  const ADMIN_CONTEXT_STORAGE = 'CK_ADMIN_CONTEXT';
  const DEFAULT_FAMILY_ID = 'default';
  const CHILD_ID_STORAGE = 'ck.childUserId';
  const UUIDish = /^[0-9a-f-]{8,}$/i;
  let activeUser = null;

  function storageGet(key) {
    try {
      return window.localStorage?.getItem(key) ?? '';
    } catch {
      return '';
    }
  }

  function storageSet(key, value) {
    try {
      if (value === null || value === undefined) {
        window.localStorage?.removeItem(key);
      } else {
        window.localStorage?.setItem(key, String(value));
      }
      return true;
    } catch {
      return false;
    }
  }

  function getSaved() {
    try {
      return window.localStorage?.getItem(CHILD_ID_STORAGE) || '';
    } catch {
      return '';
    }
  }

  function setSaved(value) {
    try {
      const next = (value ?? '').toString().trim();
      if (next) {
        window.localStorage?.setItem(CHILD_ID_STORAGE, next);
      } else {
        window.localStorage?.removeItem(CHILD_ID_STORAGE);
      }
    } catch {}
  }

  function setActiveUser(user) {
    if (user && typeof user === 'object') {
      const id = (user.id ?? '').toString().trim();
      activeUser = id
        ? {
            id,
            name: (user.name ?? '').toString().trim() || id,
            family_id: user.family_id ?? user.familyId ?? null
          }
        : null;
    } else {
      activeUser = null;
    }
  }

  function getChildId() {
    return activeUser?.id || '';
  }

  function getChildName() {
    return activeUser?.name || '';
  }

  function readConfiguredFamilyId() {
    if (typeof document === 'undefined') return null;
    const bodyFamily = document.body?.dataset?.familyId;
    if (bodyFamily && bodyFamily.trim()) {
      return bodyFamily.trim();
    }
    const meta = document.querySelector('meta[name="ck:family-id"], meta[name="ck:family_id"]');
    if (meta && meta.content && meta.content.trim()) {
      return meta.content.trim();
    }
    return null;
  }

  function readStoredAdminContext() {
    try {
      const raw = window.localStorage?.getItem(ADMIN_CONTEXT_STORAGE);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (error) {
      console.warn('Unable to read stored admin context', error);
      return null;
    }
  }

  function resolveFamilyId() {
    try {
      const search = new URLSearchParams(window.location?.search || '');
      const queryFamily = search.get('family_id') || search.get('familyId') || search.get('family');
      if (queryFamily && queryFamily.trim()) {
        return queryFamily.trim();
      }
    } catch (error) {
      console.warn('family query parse failed', error);
    }
    const configured = readConfiguredFamilyId();
    if (configured) {
      return configured;
    }
    const globalFamily = typeof window !== 'undefined' ? window.currentFamilyId : null;
    if (typeof globalFamily === 'string' && globalFamily.trim()) {
      return globalFamily.trim();
    }
    const stored = readStoredAdminContext();
    if (stored) {
      const fromContext = stored.currentFamilyId || stored.family_id || stored.familyId;
      if (fromContext && String(fromContext).trim()) {
        return String(fromContext).trim();
      }
    }
    return DEFAULT_FAMILY_ID;
  }

  window.CKPWA?.initAppShell({
    swVersion: '1.0.0',
    installButtonSelector: '#installBtn',
    iosTipSelector: '#iosInstallTip',
    iosDismissSelector: '#dismissIosTip'
  });

    function extractYouTubeId(u) {
      if (!u) return "";
      try {
        // Allow raw IDs
        if (/^[\w-]{11}$/.test(u)) return u;

        const x = new URL(u);
        // youtu.be/<id>
        if (x.hostname.includes("youtu.be")) {
          return (x.pathname.split("/")[1] || "").split("?")[0].split("&")[0];
        }
        // youtube.com/watch?v=<id>
        const v = x.searchParams.get("v");
        if (v) return v.split("&")[0];

        // youtube.com/shorts/<id>
        const mShorts = x.pathname.match(/\/shorts\/([\w-]{11})/);
        if (mShorts) return mShorts[1];

        // youtube.com/embed/<id>
        const mEmbed = x.pathname.match(/\/embed\/([\w-]{11})/);
        if (mEmbed) return mEmbed[1];

        // Last resort: first 11-char token
        const m = u.match(/([\w-]{11})/);
        if (m) return m[1];
      } catch {
        // ignore parsing errors and fall back to loose matching below
      }
      const fallback = String(u).match(/([\w-]{11})/);
      return fallback ? fallback[1] : "";
    }

  function waitForReady(oframe, timeout = 2000) {
    return new Promise((resolve, reject) => {
      let timer = null;
      let settled = false;
      let handshake = null;

      const finish = (fn, value) => {
        if (settled) return;
        settled = true;
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        if (handshake) {
          clearInterval(handshake);
          handshake = null;
        }
        window.removeEventListener('message', onMessage);
        fn(value);
      };

      function onMessage(event) {
        if (event.source !== oframe.contentWindow) return;
        let payload = event.data;
        if (typeof payload === 'string') {
          try {
            payload = JSON.parse(payload);
          } catch (error) {
            // ignore
          }
        }
        if (payload && payload.event === 'onReady') {
          finish(resolve);
        }
      }

      function sendHandshake() {
        try {
          const target = oframe.contentWindow;
          if (!target) return;
          const payload = JSON.stringify({ event: 'listening', channel: 'widget', id: oframe.id || 'ck-video' });
          target.postMessage(payload, '*');
        } catch (error) {
          console.warn('iframe handshake failed', error);
        }
      }

      window.addEventListener('message', onMessage);
      sendHandshake();
      handshake = setInterval(sendHandshake, 400);
      oframe.addEventListener('load', sendHandshake, { once: true });
      timer = setTimeout(() => finish(reject, new Error('timeout')), timeout);
    });
  }

  function getYouTubeThumbnail(url) {
    const id = getYouTubeId(url);
    return id ? `https://img.youtube.com/vi/${id}/hqdefault.jpg` : '';
  }

  function getYouTubeEmbed(url, { host = 'www.youtube.com', autoplay = true } = {}) {
    const id = getYouTubeId(url);
    if (!id) return '';
    const params = new URLSearchParams({
      modestbranding: '1',
      rel: '0',
      playsinline: '1',
    });
    if (autoplay) params.set('autoplay', '1');
    return `https://${host}/embed/${id}?${params.toString()}`;
  }

  (function setupVideoModal() {
    const modal = document.getElementById("videoModal");
    if (!modal) return;

    window.openVideoModal = function openVideoModal(url) {
      const id = window.getYouTubeId ? getYouTubeId(url) : "";
      if (!id) {
        console.warn("openVideoModal: no video id for url:", url);
        return;
      }
      const iframe = modal?.querySelector("iframe");
      const link = modal?.querySelector("#openOnYouTube");
      const dialog = modal?.querySelector(".modal-dialog");
      if (!iframe) {
        console.error("openVideoModal: modal/iframe not found");
        return;
      }
      const embedUrl =
        window.getYouTubeEmbed?.(`https://www.youtube.com/watch?v=${id}`, {
          autoplay: true,
        }) ||
        `https://www.youtube.com/embed/${id}?autoplay=1&rel=0&modestbranding=1&playsinline=1`;
      iframe.src = embedUrl;
      if (dialog) {
        const isVertical = window.isLikelyVerticalYouTube?.(url) || isLikelyVerticalYouTube(url);
        dialog.classList.toggle("vertical", Boolean(isVertical));
      }
      if (link) {
        link.href = `https://www.youtube.com/watch?v=${id}`;
      }
      modal.classList.remove("hidden");
      modal.classList.add("open");
    };

    window.closeVideoModal = function closeVideoModal() {
      const iframe = modal?.querySelector("iframe");
      const link = modal?.querySelector("#openOnYouTube");
      if (iframe) iframe.src = "";
      if (link) link.removeAttribute("href");
      modal.classList.remove("open");
      modal.classList.add("hidden");
    };

    // Close on backdrop or [data-close]
    modal.addEventListener("click", (e) => {
      if (e.target.matches("[data-close]") || e.target === modal.querySelector(".modal-backdrop")) {
        e.preventDefault();
        closeVideoModal();
      }
    });

    // Close on Esc
    window.addEventListener("keydown", (e) => {
      if (!modal.classList.contains("hidden") && e.key === "Escape") closeVideoModal();
    });
  })();

  function getUserId() {
    return getChildId().trim();
  }

  function syncActiveUserInputs() {
    const idInput = document.getElementById('childUserId');
    if (idInput && typeof idInput.value === 'string') {
      idInput.value = getChildId();
    }
  }

  async function resolveUser(userInput) {
    const raw = (userInput ?? '').toString().trim();
    if (!raw) throw new Error('Enter a user name or ID.');
    const res = await fetch(`/api/child/resolve-user?user=${encodeURIComponent(raw)}`);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      let message = typeof data?.error === 'string' && data.error.trim() ? data.error.trim() : 'login failed';
      if (message === 'user required') message = 'Enter a user name or ID.';
      throw new Error(message);
    }
    return data;
  }

  async function loadFamilyLists(userId) {
    if (!userId) return;
    await Promise.allSettled([loadEarnTemplates(userId), loadRewards(userId)]);
    refreshRedeemNotice();
  }

  async function loadChildDataFor(user) {
    if (!user || !user.id) return;
    await loadFamilyLists(user.id);
  }

  function enterApp(user, remember) {
    setActiveUser(user);
    syncActiveUserInputs();
    const loginSection = document.querySelector('#child-login');
    const appSection = document.querySelector('#child-content');
    loginSection?.classList.add('hidden');
    appSection?.classList.remove('hidden');
    const shortId = user.id && user.id.length > 6 ? `${user.id.slice(0, 6)}…` : user.id;
    const banner = document.querySelector('#child-current');
    if (banner) {
      const name = getChildName();
      banner.textContent = name ? `Signed in as ${name} (${shortId})` : `Signed in as ${shortId}`;
    }
    const rememberBox = document.querySelector('#child-remember');
    if (rememberBox) rememberBox.checked = !!remember;
    const msg = document.querySelector('#child-login-msg');
    if (msg) msg.textContent = '';
    if (remember) {
      setSaved(user.id);
    } else {
      setSaved('');
    }
    loadChildDataFor(user);
  }

  function backToLogin() {
    setActiveUser(null);
    syncActiveUserInputs();
    const loginSection = document.querySelector('#child-login');
    const appSection = document.querySelector('#child-content');
    loginSection?.classList.remove('hidden');
    appSection?.classList.add('hidden');
    const banner = document.querySelector('#child-current');
    if (banner) banner.textContent = '';
    const msg = document.querySelector('#child-login-msg');
    if (msg) msg.textContent = '';
    const input = document.querySelector('#child-user');
    if (input) {
      input.value = '';
      input.focus();
    }
    const earnBox = $('earnList');
    if (earnBox) earnBox.innerHTML = '<div class="muted">Log in to load tasks.</div>';
    const rewardsBox = $('shopList');
    if (rewardsBox) rewardsBox.innerHTML = '';
    $('shopMsg').textContent = 'Log in to load rewards.';
    const empty = $('shopEmpty');
    if (empty) empty.style.display = 'block';
    setQR('');
    lastRedeemEntry = null;
    updateRedeemNotice(null, { fallbackText: 'Log in to see recent redeemed rewards.' });
    setRecentVisible(false);
    setFullVisible(false);
  }

  function saveFilters(filters) {
    const payload = JSON.stringify(filters || {});
    storageSet(LS_FILTER, payload);
  }
  function loadFilters() {
    try {
      return JSON.parse(storageGet(LS_FILTER) || '{}');
    } catch { return {}; }
  }

  function formatDateTime(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '';
    const date = new Date(num);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleString();
  }

  function updateRedeemNotice(entry, { fallbackText } = {}) {
    const box = $('redeemNotice');
    if (!box) return;
    box.innerHTML = '';
    if (!entry) {
      box.classList.add('muted');
      box.textContent = fallbackText || 'No redeemed rewards yet.';
      return;
    }
    box.classList.remove('muted');
    const title = document.createElement('div');
    title.className = 'notice-title';
    title.textContent = entry.note || 'Reward redeemed';

    const whenText = formatDateTime(entry.at);
    const whenLine = document.createElement('div');
    whenLine.className = 'notice-meta';
    whenLine.textContent = whenText ? `Redeemed on ${whenText}` : 'Redeemed reward';

    const detailParts = [];
    const spent = Math.abs(Number(entry.delta) || 0);
    if (spent) detailParts.push(`Spent ${spent} points`);
    const balanceAfter = Number(entry.balance_after);
    if (Number.isFinite(balanceAfter)) detailParts.push(`Remaining balance: ${balanceAfter} points`);

    box.appendChild(title);
    box.appendChild(whenLine);
    if (detailParts.length) {
      const detailLine = document.createElement('div');
      detailLine.className = 'notice-meta';
      detailLine.textContent = detailParts.join(' • ');
      box.appendChild(detailLine);
    }
  }

  function updateRecentButton() {
    const btn = $('btnRecentRedeems');
    if (!btn) return;
    btn.textContent = recentRedeemsVisible ? 'Hide Recent Redeemed' : 'Show Recent Redeemed';
  }

  function updateFullButton() {
    const btn = $('btnFullRedeems');
    if (!btn) return;
    btn.textContent = fullRedeemsVisible ? 'Hide Full Redeemed History' : 'Show Full Redeemed History';
  }

  function setRecentVisible(visible) {
    const box = $('recentRedeems');
    recentRedeemsVisible = visible;
    if (box) {
      box.classList.toggle('active', visible);
      if (!visible) {
        box.innerHTML = '';
      }
    }
    updateRecentButton();
  }

  function setFullVisible(visible) {
    const box = $('fullRedeems');
    fullRedeemsVisible = visible;
    if (box) {
      box.classList.toggle('active', visible);
      if (!visible) {
        box.innerHTML = '';
      } else {
        box.scrollTop = 0;
      }
    }
    updateFullButton();
  }

  function renderRedeemList(targetId, items, limit) {
    const box = $(targetId);
    if (!box) return;
    box.innerHTML = '';
    const list = Array.isArray(items) ? (typeof limit === 'number' ? items.slice(0, limit) : items.slice()) : [];
    if (!list.length) {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = 'No redeemed rewards yet.';
      box.appendChild(empty);
      return;
    }
    const frag = document.createDocumentFragment();
    for (const entry of list) {
      const row = document.createElement('div');
      row.className = 'recent-row';
      const name = document.createElement('div');
      name.className = 'recent-name';
      name.textContent = entry.note || 'Reward redeemed';
      const meta = document.createElement('div');
      meta.className = 'recent-meta';
      const parts = [];
      const when = formatDateTime(entry.at);
      if (when) parts.push(when);
      const spent = Math.abs(Number(entry.delta) || 0);
      if (spent) parts.push(`Spent ${spent} points`);
      const balanceAfter = Number(entry.balance_after);
      if (Number.isFinite(balanceAfter)) parts.push(`Balance ${balanceAfter} points`);
      if (!parts.length) parts.push('Reward redeemed');
      meta.textContent = parts.join(' • ');
      row.appendChild(name);
      row.appendChild(meta);
      frag.appendChild(row);
    }
    box.appendChild(frag);
  }

  async function fetchRedeemHistory(userId, limit = RECENT_REDEEM_LIMIT) {
    const res = await fetch(`/api/history/user/${encodeURIComponent(userId)}?limit=${limit}`);
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data?.error ? String(data.error) : 'Unable to load redeemed rewards.');
    }
    const rows = Array.isArray(data?.rows) ? data.rows : [];
    return rows.filter(row => row.action === 'spend_redeemed');
  }

  async function refreshRedeemNotice() {
    const userId = getUserId();
    if (!userId) {
      lastRedeemEntry = null;
      updateRedeemNotice(null, { fallbackText: 'Log in to see recent redeemed rewards.' });
      setRecentVisible(false);
      setFullVisible(false);
      return;
    }
    if (lastRedeemEntry) {
      updateRedeemNotice(lastRedeemEntry);
    } else {
      updateRedeemNotice(null, { fallbackText: 'Checking for redeemed rewards...' });
    }
    try {
      const redeems = await fetchRedeemHistory(userId, 20);
      lastRedeemEntry = redeems[0] || null;
      if (lastRedeemEntry) {
        updateRedeemNotice(lastRedeemEntry);
      } else {
        updateRedeemNotice(null, { fallbackText: 'No redeemed rewards yet.' });
      }
    } catch (err) {
      updateRedeemNotice(null, { fallbackText: err?.message || 'Unable to load redeemed rewards.' });
    }
  }

  async function loadRecentRedeems() {
    const userId = getUserId();
    if (!userId) {
      alert('Log in first.');
      return;
    }
    const box = $('recentRedeems');
    setRecentVisible(true);
    if (box) {
      box.innerHTML = '<div class="muted">Loading...</div>';
    }
    try {
      const redeems = await fetchRedeemHistory(userId);
      lastRedeemEntry = redeems[0] || null;
      if (lastRedeemEntry) {
        updateRedeemNotice(lastRedeemEntry);
      } else {
        updateRedeemNotice(null, { fallbackText: 'No redeemed rewards yet.' });
      }
      renderRedeemList('recentRedeems', redeems, RECENT_REDEEM_DISPLAY);
    } catch (err) {
      if (box) {
        box.innerHTML = '';
        const msg = document.createElement('div');
        msg.className = 'muted';
        msg.textContent = err?.message || 'Failed to load redeemed rewards.';
        box.appendChild(msg);
      }
    }
  }

  async function loadFullRedeems() {
    const userId = getUserId();
    if (!userId) {
      alert('Log in first.');
      return;
    }
    const box = $('fullRedeems');
    setFullVisible(true);
    if (box) {
      box.innerHTML = '<div class="muted">Loading...</div>';
    }
    try {
      const redeems = await fetchRedeemHistory(userId, FULL_REDEEM_LIMIT);
      renderRedeemList('fullRedeems', redeems);
    } catch (err) {
      if (box) {
        box.innerHTML = '';
        const msg = document.createElement('div');
        msg.className = 'muted';
        msg.textContent = err?.message || 'Failed to load redeemed rewards.';
        box.appendChild(msg);
      }
    }
  }

  async function toggleRecentRedeems() {
    if (recentRedeemsVisible) {
      setRecentVisible(false);
      return;
    }
    await loadRecentRedeems();
  }

  async function toggleFullRedeems() {
    if (fullRedeemsVisible) {
      setFullVisible(false);
      return;
    }
    await loadFullRedeems();
  }

  // ===== Balance & history =====
  async function checkBalance() {
    const userId = getUserId();
    if (!userId) { alert('Log in first.'); return; }
    try {
      const res = await fetch(`/summary/${encodeURIComponent(userId)}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'failed');
      $('balanceResult').textContent = `Balance: ${data.balance} points • Earned ${data.earned} • Spent ${data.spent}`;
      refreshRedeemNotice();
    } catch (err) {
      $('balanceResult').textContent = err.message || 'Failed to load balance';
    }
  }
  $('btnCheck')?.addEventListener('click', checkBalance);

  async function loadHistory() {
    const userId = getUserId();
    if (!userId) { alert('Log in first.'); return; }
    const filters = getFilters();
    const list = $('historyList');
    list.innerHTML = '<div class="muted">Loading...</div>';
    try {
      const res = await fetch(`/api/history/user/${encodeURIComponent(userId)}?limit=200`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'failed');
      const rows = Array.isArray(data.rows) ? data.rows : [];
      renderHistory(rows, filters);
      fullRedeemsCache = rows.filter(row => row.action === 'spend_redeemed');
      fullRedeemsCacheUserId = userId;
      if (fullRedeemsVisible && Array.isArray(fullRedeemsCache)) {
        renderRedeemList('fullRedeems', fullRedeemsCache);
        updateFullButton();
      }
      const latestRedeem = rows.find(row => row.action === 'spend_redeemed') || null;
      lastRedeemEntry = latestRedeem;
      if (latestRedeem) {
        updateRedeemNotice(latestRedeem);
      } else {
        updateRedeemNotice(null, { fallbackText: 'No redeemed rewards yet.' });
      }
    } catch (err) {
      list.innerHTML = `<div class="muted">${err.message || 'Failed to load history'}</div>`;
    }
  }
  $('btnHistory')?.addEventListener('click', loadHistory);
  $('btnCsv')?.addEventListener('click', () => {
    const userId = getUserId();
    if (!userId) { alert('Log in first.'); return; }
    window.open(`/api/history.csv/${encodeURIComponent(userId)}`, '_blank');
  });

  function getFilters() {
    return {
      earn: $('showEarn').checked,
      spend: $('showSpend').checked
    };
  }

  function renderHistory(rows, filters) {
    const list = $('historyList');
    list.innerHTML = '';
    const filtered = rows.filter(row => {
      if (row.action.startsWith('earn_') && !filters.earn) return false;
      if (row.action.startsWith('spend_') && !filters.spend) return false;
      return true;
    });
    if (!filtered.length) {
      list.innerHTML = '<div class="muted">No history yet.</div>';
      $('summary').textContent = '';
      return;
    }
    let earnSum = 0, spendSum = 0;
    for (const row of filtered) {
      if (row.delta > 0) earnSum += row.delta;
      if (row.delta < 0) spendSum += Math.abs(row.delta);
      const div = document.createElement('div');
      div.className = 'hist-row';
      div.innerHTML = `<div>${new Date(row.at).toLocaleString()}</div><div>${row.action}</div><div>${row.delta}</div>`;
      list.appendChild(div);
    }
    $('summary').textContent = `Showing ${filtered.length} • Earned ${earnSum} • Spent ${spendSum}`;
  }

  function initFilters() {
    const saved = loadFilters();
    $('showEarn').checked = !!saved.earn;
    $('showSpend').checked = !!saved.spend;
    ['showEarn', 'showSpend'].forEach(id => {
      $(id)?.addEventListener('change', () => {
        saveFilters(getFilters());
        if (getUserId()) loadHistory();
      });
    });
  }
  initFilters();

  // ===== Earn templates =====
  let templates = [];
  async function loadEarnTemplates(userIdOverride) {
    const userId = (userIdOverride ?? getChildId()).trim();
    const earnBox = $('earnList');
    if (!userId) {
      if (earnBox) earnBox.innerHTML = '<div class="muted">Log in to load tasks.</div>';
      templates = [];
      return;
    }
    if (earnBox) earnBox.innerHTML = '<div class="muted">Loading...</div>';
    try {
      const res = await fetch(`/api/child/tasks?userId=${encodeURIComponent(userId)}`);
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        const message = (data && data.error) || (typeof data === 'string' ? data : 'Failed to load tasks');
        throw new Error(message);
      }
      templates = Array.isArray(data)
        ? data
            .map((tpl) => ({
              id: tpl.id,
              title: tpl.title || '',
              points: Number(tpl.points ?? 0) || 0,
              description: tpl.description || '',
              youtube_url: tpl.youtube_url || tpl.master_youtube || '',
              master_youtube: tpl.master_youtube || '',
              sort_order: Number(tpl.sort_order ?? 0) || 0
            }))
            .sort((a, b) => a.title.localeCompare(b.title))
        : [];
      renderEarnList();
    } catch (err) {
      $('earnList').innerHTML = `<div class="muted">${err.message || 'Failed to load tasks'}</div>`;
    }
  }
  function renderEarnList() {
    const box = $('earnList');
    box.innerHTML = '';
    for (const tpl of templates) {
      const card = document.createElement('label');
      card.className = 'earn-card';
      card.innerHTML = `
        <header>
          <span>${tpl.title}</span>
          <span>+${tpl.points}</span>
        </header>
        <div class="desc">${tpl.description || ''}</div>
        <div class="video-slot"></div>
        <div style="display:flex; align-items:center; gap:8px;">
          <input type="checkbox" data-id="${tpl.id}" data-points="${tpl.points}">
          <span class="muted">Include</span>
        </div>
      `;
      const videoSlot = card.querySelector('.video-slot');
      const clipUrl = tpl.youtube_url || tpl.master_youtube || '';
      if (videoSlot && clipUrl) {
        const watchBtn = document.createElement('button');
        watchBtn.type = 'button';
        watchBtn.className = 'btn btn-sm';
        watchBtn.textContent = 'Watch clip';
        watchBtn.dataset.youtube = clipUrl;
        watchBtn.addEventListener('click', (event) => {
          event.preventDefault();
          event.stopPropagation();
          const url = watchBtn.dataset.youtube;
          if (url) openVideoModal(url);
        });
        videoSlot.appendChild(watchBtn);
      } else if (videoSlot) {
        videoSlot.remove();
      }
      box.appendChild(card);
    }
    box.querySelectorAll('input[type="checkbox"]').forEach(chk => chk.addEventListener('change', updateEarnSummary));
    updateEarnSummary();
  }
  function updateEarnSummary() {
    const selected = Array.from(document.querySelectorAll('#earnList input[type="checkbox"]:checked'));
    const total = selected.reduce((sum, el) => sum + Number(el.dataset.points || 0), 0);
    $('earnSummary').textContent = `Selected: ${total} points (${selected.length} task${selected.length === 1 ? '' : 's'})`;
  }
  $('btnEarnQr')?.addEventListener('click', async () => {
    const userId = getUserId();
    if (!userId) { alert('Log in first.'); return; }
    const selected = Array.from(document.querySelectorAll('#earnList input[type="checkbox"]:checked')).map(el => ({ id: Number(el.dataset.id), count: 1 }));
    if (!selected.length) { alert('Pick at least one task'); return; }
    try {
      const res = await fetch('/api/tokens/earn', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, templates: selected })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'failed');
      renderQr('earnQrBox', data.qrText);
    } catch (err) {
      alert(err.message || 'Failed to create QR');
    }
  });

  let qrLibraryPromise = null;

  function getQrConstructor() {
    if (typeof window === 'undefined') return null;
    const candidates = [
      window.QRCode,
      window?.QRCode?.QRCode,
      window?.QRCode?.default,
    ];
    return candidates.find((ctor) => typeof ctor === 'function') || null;
  }

  function ensureQrLibrary() {
    const existingCtor = getQrConstructor();
    if (existingCtor) {
      return Promise.resolve(existingCtor);
    }
    if (qrLibraryPromise) {
      return qrLibraryPromise;
    }
    qrLibraryPromise = new Promise((resolve, reject) => {
      let settled = false;

      const finish = (fn, value) => {
        if (settled) return;
        settled = true;
        fn(value);
      };

      const fail = (reason) => {
        const err = reason instanceof Error ? reason : new Error('Failed to load QR library');
        finish(reject, err);
      };

      const attemptResolve = () => {
        const ctor = getQrConstructor();
        if (ctor) {
          finish(resolve, ctor);
          return true;
        }
        return false;
      };

      const onReady = () => {
        if (attemptResolve()) return;
        setTimeout(() => {
          if (attemptResolve()) return;
          fail(new Error('QR library loaded without QRCode constructor'));
        }, 0);
      };

      const scripts = Array.from(document.getElementsByTagName('script'));
      const existing = scripts.find((script) => script.src && script.src.includes('qrcode'));
      if (existing) {
        const markReady = () => {
          existing.dataset.ckQrReady = '1';
          onReady();
        };
        const alreadyLoaded = existing.dataset.ckQrReady === '1' || existing.readyState === 'complete' || existing.readyState === 'loaded';
        if (typeof window.QRCode === 'function') {
          markReady();
        } else if (alreadyLoaded) {
          markReady();
        } else {
          existing.addEventListener('load', markReady, { once: true });
          existing.addEventListener('error', () => reject(new Error('Failed to load QR library')), { once: true });
        }
        return;
      }

      const script = document.createElement('script');
      script.src = '/qrcode.min.js?v=__BUILD__';
      script.async = true;
      script.dataset.ckQrLoader = '1';
      script.addEventListener('load', onReady, { once: true });
      script.addEventListener('error', () => fail(new Error('Failed to load QR library')), { once: true });
      document.head.appendChild(script);
    }).catch((err) => {
      qrLibraryPromise = null;
      throw err;
    });
    return qrLibraryPromise;
  }

  function openQrModal(imageUrl) {
    if (!imageUrl) return;
    if (typeof openImageModal === 'function') {
      openImageModal(imageUrl);
      return;
    }
    const win = window.open(imageUrl, '_blank', 'noopener');
    if (!win) {
      window.location.href = imageUrl;
    }
  }

  function isHttpUrl(value) {
    if (!value) return false;
    try {
      const url = new URL(value);
      return url.protocol === 'http:' || url.protocol === 'https:';
    } catch {
      return false;
    }
  }

  function buildQrFigure(imageUrl, rawText, options = {}) {
    const figure = document.createElement('figure');
    figure.className = 'qr-output';
    let actions = null;
    const ensureActions = () => {
      if (!actions) {
        actions = document.createElement('div');
        actions.className = 'qr-actions';
      }
      return actions;
    };

    if (imageUrl) {
      const img = new Image();
      img.src = imageUrl;
      img.alt = 'Reward QR code';
      img.loading = 'eager';
      img.decoding = 'sync';
      img.width = 320;
      img.height = 320;
      img.style.cursor = 'zoom-in';
      img.addEventListener('click', () => openQrModal(imageUrl));
      figure.appendChild(img);

      const caption = document.createElement('figcaption');
      caption.className = 'qr-caption';
      caption.textContent = options.captionText || 'Tap the QR to enlarge or share.';
      figure.appendChild(caption);

      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className = 'qr-action';
      openBtn.textContent = 'Open full screen';
      openBtn.addEventListener('click', () => openQrModal(imageUrl));
      ensureActions().appendChild(openBtn);
    } else {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = options.emptyMessage || 'QR unavailable.';
      figure.appendChild(empty);
    }

    if (rawText) {
      if (isHttpUrl(rawText)) {
        const link = document.createElement('a');
        link.className = 'qr-link';
        link.href = rawText;
        link.target = '_blank';
        link.rel = 'noopener';
        link.textContent = 'Open link instead';
        ensureActions().appendChild(link);
      } else {
        const copyBtn = document.createElement('button');
        copyBtn.type = 'button';
        copyBtn.className = 'qr-action';
        copyBtn.textContent = 'Copy code';
        const setCopiedState = () => {
          const original = copyBtn.dataset.originalText || copyBtn.textContent;
          copyBtn.dataset.originalText = original;
          copyBtn.textContent = 'Copied!';
          copyBtn.disabled = true;
          setTimeout(() => {
            copyBtn.textContent = copyBtn.dataset.originalText || 'Copy code';
            copyBtn.disabled = false;
            delete copyBtn.dataset.originalText;
          }, 1600);
        };
        copyBtn.addEventListener('click', async () => {
          try {
            if (navigator.clipboard?.writeText) {
              await navigator.clipboard.writeText(rawText);
              setCopiedState();
              return;
            }
            const area = document.createElement('textarea');
            area.value = rawText;
            area.setAttribute('readonly', 'readonly');
            area.style.position = 'absolute';
            area.style.left = '-9999px';
            document.body.appendChild(area);
            area.select();
            const success = document.execCommand('copy');
            document.body.removeChild(area);
            if (success) {
              setCopiedState();
            } else {
              throw new Error('copy command failed');
            }
          } catch (error) {
            console.warn('Copy failed', error);
            toast('Copy not supported on this device.');
          }
        });
        ensureActions().appendChild(copyBtn);
      }
    }

    if (actions?.childElementCount) {
      figure.appendChild(actions);
    }

    return figure;
  }

  async function renderQr(elId, text) {
    const el = $(elId);
    if (!el) return;
    el.innerHTML = '';
    if (!text) return;
    try {
      await ensureQrLibrary();
      const Ctor = getQrConstructor();
      if (typeof Ctor !== 'function') {
        throw new Error('QR constructor unavailable');
      }
      const staging = document.createElement('div');
      staging.style.position = 'absolute';
      staging.style.pointerEvents = 'none';
      staging.style.opacity = '0';
      staging.style.left = '-9999px';
      staging.style.top = '0';
      document.body.appendChild(staging);
      let imageUrl = '';
      try {
        const options = {
          text,
          width: 320,
          height: 320,
          colorDark: '#000000',
          colorLight: '#ffffff',
        };
        const correctLevel = Ctor?.CorrectLevel?.H ?? Ctor?.CorrectLevel?.Q;
        if (typeof correctLevel !== 'undefined') {
          options.correctLevel = correctLevel;
        }
        const instance = new Ctor(staging, options);
        if (instance && typeof instance.makeCode === 'function') {
          instance.makeCode(text);
        }
        const canvas = staging.querySelector('canvas');
        const inlineImg = staging.querySelector('img');
        if (canvas && typeof canvas.toDataURL === 'function') {
          const ctx = canvas.getContext('2d');
          if (ctx) {
            ctx.globalCompositeOperation = 'destination-over';
            ctx.fillStyle = '#ffffff';
            ctx.fillRect(0, 0, canvas.width, canvas.height);
            ctx.globalCompositeOperation = 'source-over';
          }
          imageUrl = canvas.toDataURL('image/png');
        } else if (inlineImg?.src) {
          imageUrl = inlineImg.src;
        }
      } finally {
        staging.remove();
      }
      const figure = buildQrFigure(imageUrl, text);
      el.appendChild(figure);
    } catch (err) {
      console.error('renderQr failed', err);
      const fallback = buildQrFigure('', text, { emptyMessage: 'QR unavailable. Use the option below:' });
      el.appendChild(fallback);
    }
  }

  // ===== Scan to receive =====
  function setupScanner(btnId, videoId, canvasId, statusId, onToken) {
    const btn = $(btnId);
    const video = $(videoId);
    const canvas = $(canvasId);
    const status = $(statusId);
    if (!btn || !video || !canvas) return;
    const ctx = canvas.getContext('2d');
    let stream = null;
    let raf = 0;
    let busy = false;

    function say(msg) { if (status) status.textContent = msg || ''; }

    async function start() {
      if (stream) { stop(); return; }
      if (!navigator.mediaDevices?.getUserMedia) {
        say('Camera not available'); return;
      }
      try {
        stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' } } });
        video.srcObject = stream;
        video.style.display = 'block';
        await video.play();
        say('Point camera at QR');
        tick();
      } catch (err) {
        say('Camera blocked or unavailable');
      }
    }
    function stop() {
      cancelAnimationFrame(raf);
      if (stream) stream.getTracks().forEach(t => t.stop());
      stream = null;
      video.pause();
      video.srcObject = null;
      video.style.display = 'none';
      say('Camera stopped');
    }
    function tick() {
      if (!stream) return;
      if (video.readyState === video.HAVE_ENOUGH_DATA) {
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
        const img = ctx.getImageData(0, 0, canvas.width, canvas.height);
        const code = jsQR(img.data, img.width, img.height, { inversionAttempts: 'dontInvert' });
        if (code?.data && !busy) {
          busy = true;
          say('QR detected...');
          onToken(code.data).finally(() => { busy = false; say('Ready'); });
        }
      }
      raf = requestAnimationFrame(tick);
    }
    btn.addEventListener('click', () => { if (stream) stop(); else start(); });
  }

  function parseToken(raw) {
    try {
      let token = raw.trim();
      if (token.startsWith('http')) {
        const url = new URL(token);
        if (url.searchParams.get('t')) token = url.searchParams.get('t');
      }
      const part = token.split('.')[0];
      const padded = part.replace(/-/g, '+').replace(/_/g, '/');
      const mod = padded.length % 4;
      const base = mod ? padded + '='.repeat(4 - mod) : padded;
      const payload = JSON.parse(atob(base));
      return { token, payload };
    } catch (err) {
      console.error(err);
      return null;
    }
  }

  setupScanner('btnScanReceive', 'receiveVideo', 'receiveCanvas', 'receiveStatus', async (raw) => {
    const parsed = parseToken(raw);
    if (!parsed || parsed.payload.typ !== 'give') {
      $('receiveStatus').textContent = 'Not a give token';
      return;
    }
    try {
      const res = await fetch('/api/earn/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: parsed.token })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'redeem failed');
      $('receiveStatus').textContent = `Received ${data.amount} points!`;
      checkBalance();
      loadHistory();
    } catch (err) {
      $('receiveStatus').textContent = err.message || 'Failed to redeem';
    }
  });

  function toast(msg) {
    if (!msg) return;
    window.alert(msg);
  }

  function setQR(text = '', url = '', raw = '') {
    const msg = $('shopMsg');
    if (msg) msg.textContent = text || '';
    const box = $('shopQrBox');
    if (!box) return;
    box.innerHTML = '';
    if (url) {
      const figure = buildQrFigure(url, raw, { captionText: 'Tap QR to view larger or share.' });
      box.appendChild(figure);
    }
  }

  function openImageModal(src){
    const m = document.createElement('div');
    Object.assign(m.style,{position:'fixed',inset:0,background:'rgba(0,0,0,.7)',display:'grid',placeItems:'center',zIndex:9999});
    m.addEventListener('click',()=>m.remove());
    const big = new Image(); big.src = src; big.style.maxWidth='90vw'; big.style.maxHeight='90vh'; big.style.boxShadow='0 8px 24px rgba(0,0,0,.5)';
    m.appendChild(big); document.body.appendChild(m);
  }

  async function setupChildLogin() {
    syncActiveUserInputs();
    const input = document.querySelector('#child-user');
    const remember = document.querySelector('#child-remember');
    const btn = document.querySelector('#child-login-btn');
    const msg = document.querySelector('#child-login-msg');

    let saved = getSaved();
    if (saved) {
      try {
        if (!UUIDish.test(saved)) {
          const resolved = await resolveUser(saved);
          saved = resolved.id;
          setSaved(saved);
        }
        const user = await resolveUser(saved);
        enterApp(user, true);
      } catch {
        setSaved('');
        backToLogin();
      }
    }

    btn?.addEventListener('click', async () => {
      const value = input && typeof input.value === 'string' ? input.value.trim() : '';
      if (!value) return;
      if (msg) msg.textContent = 'Signing in...';
      try {
        const user = await resolveUser(value);
        enterApp(user, !!remember?.checked);
      } catch (error) {
        if (msg) msg.textContent = error?.message || 'login failed';
      }
    });

    input?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        btn?.click();
      }
    });

    document.querySelector('#child-switch')?.addEventListener('click', () => {
      setSaved('');
      const rememberBox = document.querySelector('#child-remember');
      if (rememberBox) rememberBox.checked = false;
      backToLogin();
    });
  }

  document.getElementById('btnLoadItems')?.addEventListener('click', () => loadRewards());
  $('btnRecentRedeems')?.addEventListener('click', toggleRecentRedeems);
  $('btnFullRedeems')?.addEventListener('click', toggleFullRedeems);
  updateRecentButton();
  updateFullButton();

  setupChildLogin();

  async function loadRewards(userIdOverride){
    const list = $('shopList');
    const empty = $('shopEmpty');
    const userId = (userIdOverride ?? getChildId()).trim();
    if (!userId){
      if (list) list.innerHTML = '';
      if (empty) empty.style.display = 'block';
      $('shopMsg').textContent = 'Log in to load rewards.';
      setQR('');
      return;
    }
    if (list) list.innerHTML = '<div class="muted">Loading...</div>';
    if (empty) empty.style.display = 'none';
    try{
      const res = await fetch(`/api/child/rewards?userId=${encodeURIComponent(userId)}`);
      const data = await res.json().catch(() => null);
      if (!res.ok) {
        const message = (data && data.error) || (typeof data === 'string' ? data : 'Failed to load rewards');
        throw new Error(message);
      }
      renderRewards(Array.isArray(data) ? data : []);
    }catch(err){
      renderError(err.message || String(err));
    }
  }

  function renderRewards(items){
    const list = $('shopList');
    if (!list) return;
    list.innerHTML = '';
    const normalized = (Array.isArray(items) ? items : []).map(item => ({
      id: item.id,
      name: item.name || item.title || 'Reward',
      cost: Number.isFinite(Number(item.cost ?? item.price)) ? Number(item.cost ?? item.price) : 0,
      description: item.description || '',
      image_url: item.image_url || item.imageUrl || '',
      youtube_url: item.youtube_url || item.master_youtube || item.youtubeUrl || '',
      youtubeUrl: item.youtubeUrl || item.youtube_url || item.master_youtube || '',
    }));
    if (!normalized.length){
      $('shopEmpty').style.display = 'block';
      $('shopMsg').textContent = '';
      setQR('');
      return;
    }
    $('shopEmpty').style.display = 'none';
    $('shopMsg').textContent = getUserId() ? 'Tap Redeem to request a reward.' : 'Log in, then tap Redeem.';
    setQR('');
    normalized.forEach((item, index) => {
      const card = document.createElement('div');
      card.className = 'reward-card';

      if (item.image_url){
        const thumb = document.createElement('img');
        thumb.className = 'reward-thumb';
        thumb.src = item.image_url;
        thumb.alt = item.name;
        thumb.loading = 'lazy';
        thumb.width = 96; thumb.height = 96;
        thumb.style.objectFit = 'cover'; thumb.style.aspectRatio = '1/1';
        thumb.addEventListener('click', ()=> openImageModal(thumb.src));
        thumb.onerror = () => thumb.remove();
        card.appendChild(thumb);
      } else {
        const spacer = document.createElement('div');
        spacer.style.width = '96px';
        spacer.style.height = '96px';
        spacer.style.flex = '0 0 auto';
        card.appendChild(spacer);
      }

      const youtubeUrl = item.youtube_url || item.youtubeUrl;
      const youtubeThumbUrl = getYouTubeThumbnail(youtubeUrl);
      if (youtubeThumbUrl) {
        const ytThumb = document.createElement('img');
        ytThumb.className = 'youtube-thumb';
        ytThumb.src = youtubeThumbUrl;
        ytThumb.alt = 'YouTube preview';
        ytThumb.loading = 'lazy';
        ytThumb.width = 72;
        ytThumb.height = 54;
        ytThumb.title = 'Play video';
        ytThumb.dataset.youtube = youtubeUrl;
        ytThumb.addEventListener('click', () => {
          const url = ytThumb.dataset.youtube;
          if (url) openVideoModal(url);
        });
        ytThumb.addEventListener('error', () => ytThumb.remove());
        card.appendChild(ytThumb);
      }

      const info = document.createElement('div');
      info.style.flex = '1 1 auto';

      const title = document.createElement('div');
      title.textContent = `${index + 1}. ${item.name}`;
      info.appendChild(title);

      const cost = document.createElement('div');
      cost.className = 'muted';
      cost.textContent = `${item.cost} points`;
      info.appendChild(cost);

      if (item.description){
        const desc = document.createElement('div');
        desc.className = 'muted';
        desc.textContent = item.description;
        info.appendChild(desc);
      }

      card.appendChild(info);

      const actions = document.createElement('div');
      actions.style.display = 'flex';
      actions.style.flexDirection = 'column';
      actions.style.gap = '6px';
      actions.style.marginLeft = 'auto';
      actions.style.flex = '0 0 auto';

      if (youtubeUrl) {
        const watchBtn = document.createElement('button');
        watchBtn.type = 'button';
        watchBtn.className = 'btn btn-sm';
        watchBtn.textContent = 'Watch clip';
        watchBtn.dataset.youtube = youtubeUrl;
        watchBtn.addEventListener('click', () => {
          const url = watchBtn.dataset.youtube;
          if (url) openVideoModal(url);
        });
        actions.appendChild(watchBtn);
      }

      const btn = document.createElement('button');
      btn.textContent = translate('redeem');
      btn.style.flex = '0 0 auto';
      btn.addEventListener('click', () => createHold(item));
      actions.appendChild(btn);

      card.appendChild(actions);

      list.appendChild(card);
    });
  }

  function renderError(message){
    const list = $('shopList');
    if (list) list.innerHTML = `<div class="muted">${message}</div>`;
    const empty = $('shopEmpty');
    if (empty) empty.style.display = 'none';
    $('shopMsg').textContent = message;
    setQR('');
  }

  async function createHold(item) {
    const userId = getUserId();
    if (!userId) { alert('Log in first.'); return; }
    setQR('Creating hold...');
    try {
      const res = await fetch('/api/holds', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, itemId: item.id })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'hold failed');
      const label = item.name || item.title || 'this item';
      $('shopMsg').textContent = `Show this QR to an adult to pick up ${label}.`;
      renderQr('shopQrBox', data.qrText);
      checkBalance();
      loadHistory();
    } catch (err) {
      setQR('');
      toast(err.message || 'Create hold failed', 'error');
    }
  }

})();
