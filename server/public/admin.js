import { renderHeader } from './js/header.js';

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

const SUPPORTED_LANGS = ['en', 'ko'];

function getCurrentLang() {
  if (window.I18N && typeof window.I18N.getLang === 'function') {
    return window.I18N.getLang();
  }
  return SUPPORTED_LANGS[0];
}

function updateActiveLangButtons(activeLang) {
  const wrap = document.getElementById('lang-controls');
  if (!wrap) return;
  wrap.querySelectorAll('button[data-lang]').forEach((btn) => {
    const isActive = btn.dataset.lang === activeLang;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-pressed', String(isActive));
  });
}

function setLang(code) {
  const normalized = SUPPORTED_LANGS.includes(code) ? code : SUPPORTED_LANGS[0];
  if (window.I18N && typeof window.I18N.setLang === 'function') {
    window.I18N.setLang(normalized);
  }
  updateActiveLangButtons(normalized);
}

function renderLangButtons() {
  const wrap = document.getElementById('lang-controls');
  if (!wrap) return;

  wrap.innerHTML = '';
  const current = getCurrentLang();

  SUPPORTED_LANGS.forEach((code) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'chip lang';
    btn.textContent = code.toUpperCase();
    btn.dataset.lang = code;
    btn.setAttribute('aria-pressed', String(code === current));
    if (code === current) btn.classList.add('active');
    btn.addEventListener('click', () => setLang(code));
    wrap.appendChild(btn);
  });
}

window.setLang = setLang;

let _booted = false;

function boot() {
  if (_booted) return;
  _booted = true;

  renderLangButtons();
  initAdmin();
}

document.addEventListener('DOMContentLoaded', boot);

function initAdmin() {
  if (window.__CK_ADMIN_READY__) return;
  window.__CK_ADMIN_READY__ = true;

  const ADMIN_KEY_DEFAULT = 'Mamapapa';
  const ADMIN_INVALID_MSG = 'Admin key invalid.';
  const ADMIN_KEY_REQUIRED_MSG = 'Please enter the adminkey first';
  const $k = (id) => document.getElementById(id);
  const $ = $k;
  const keyInput = $k('adminKey'); // use current ID
  const memoryStore = {};

  window.CKPWA?.initAppShell({
    swVersion: '1.0.0',
    installButtonSelector: '#installBtn'
  });

  function storageGet(key) {
    try {
      const value = window.localStorage.getItem(key);
      if (value != null) memoryStore[key] = value;
      return value;
    } catch (error) {
      console.warn('localStorage getItem failed', error);
      return Object.prototype.hasOwnProperty.call(memoryStore, key) ? memoryStore[key] : null;
    }
  }

  function storageSet(key, value) {
    let ok = true;
    try {
      window.localStorage.setItem(key, value);
    } catch (error) {
      console.warn('localStorage setItem failed', error);
      ok = false;
    }
    memoryStore[key] = value;
    return ok;
  }

  function storageRemove(key) {
    let ok = true;
    try {
      window.localStorage.removeItem(key);
    } catch (error) {
      console.warn('localStorage removeItem failed', error);
      ok = false;
    }
    delete memoryStore[key];
    return ok;
  }

  function ensureMemberPanelStyles() {
    if (document.getElementById('memberPanelsStyles')) return;
    const style = document.createElement('style');
    style.id = 'memberPanelsStyles';
    style.textContent = `
.member-balance-container {
  display: grid;
  gap: 12px;
}

details.member-fold {
  border: 1px solid var(--line, #e5e7eb);
  border-radius: 10px;
  background: #fff;
  overflow: hidden;
}

details.member-fold[open] {
  box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08);
}

details.member-fold summary {
  list-style: none;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  cursor: pointer;
  font-weight: 600;
  padding: 12px 14px;
  position: relative;
  padding-right: 36px;
}

details.member-fold summary::-webkit-details-marker {
  display: none;
}

details.member-fold summary::after {
  content: '▾';
  position: absolute;
  right: 14px;
  color: var(--muted, #6b7280);
  font-size: 12px;
  transition: transform 0.2s ease;
}

details.member-fold[open] summary::after {
  transform: rotate(-180deg);
}

details.member-fold .summary-value {
  font-size: 13px;
  color: var(--muted, #6b7280);
  font-weight: 500;
}

.member-fold-body {
  padding: 0 14px 14px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.ledger-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 8px;
}

.ledger-summary .chip {
  display: inline-flex;
  flex-direction: column;
  padding: 6px 10px;
  background: rgba(37, 99, 235, 0.1);
  color: var(--accent, #2563eb);
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  min-width: 120px;
}

.ledger-summary .chip span {
  color: var(--muted, #6b7280);
  font-weight: 500;
}

.member-ledger {
  display: grid;
  gap: 8px;
}
`;
    if (document.head) {
      document.head.appendChild(style);
    } else if (document.body) {
      document.body.appendChild(style);
    }
  }

  function ensureMemberPanels() {
    ensureMemberPanelStyles();
    if (!memberInfoPanel) return;
    let container = document.getElementById('memberBalanceContainer');
    if (!container) {
      container = document.createElement('div');
      container.id = 'memberBalanceContainer';
      container.className = 'stack member-balance-container';
      if (memberInfoDetails && memberInfoDetails.nextSibling) {
        memberInfoPanel.insertBefore(container, memberInfoDetails.nextSibling);
      } else {
        memberInfoPanel.appendChild(container);
      }
    } else if (container.parentElement !== memberInfoPanel) {
      memberInfoPanel.appendChild(container);
    }

    if (!container.querySelector('#memberBalanceDetails')) {
      container.innerHTML = `
<details id="memberBalanceDetails" class="member-fold">
  <summary>
    <span>Balance</span>
    <span class="summary-value" id="memberBalanceSummaryValue">—</span>
  </summary>
  <div class="member-fold-body" id="memberBalanceBody">
    <div class="muted">Balance info will appear here.</div>
  </div>
</details>
<details id="memberEarnDetails" class="member-fold">
  <summary>
    <span>Earn</span>
    <span class="summary-value" id="memberEarnSummaryValue">—</span>
  </summary>
  <div class="member-fold-body" id="memberEarnBody">
    <div id="memberEarnSummary" class="ledger-summary">
      <div class="muted">Earn activity will appear here.</div>
    </div>
  </div>
</details>
<details id="memberRedeemDetails" class="member-fold">
  <summary>
    <span>Redeem</span>
    <span class="summary-value" id="memberRedeemSummaryValue">—</span>
  </summary>
  <div class="member-fold-body">
    <div id="memberRedeemSummary" class="ledger-summary">
      <div class="muted">Redeem activity will appear here.</div>
    </div>
    <div id="memberLedger" class="member-ledger muted">Redeemed rewards will appear here.</div>
  </div>
</details>
<details id="memberRefundDetails" class="member-fold">
  <summary>
    <span>Refund</span>
    <span class="summary-value" id="memberRefundSummaryValue">—</span>
  </summary>
  <div class="member-fold-body" id="memberRefundBody">
    <div id="memberRefundSummary" class="ledger-summary">
      <div class="muted">Refund activity will appear here.</div>
    </div>
  </div>
</details>
`;
    }
  }

  if (keyInput) {
    keyInput.placeholder = 'enter admin key';
    const saved = storageGet('CK_ADMIN_KEY');
    if (saved) keyInput.value = saved;
  }

  const toastHost = $('toastHost');

  function toast(msg, type = 'success', ms = 2400) {
    if (!toastHost) return alert(msg);
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = msg;
    toastHost.appendChild(el);
    requestAnimationFrame(() => el.classList.add('show'));
    setTimeout(() => {
      el.classList.remove('show');
      setTimeout(() => el.remove(), 200);
    }, ms);
  }

  function openImageModal(src){
    if (!src) return;
    const m=document.createElement('div');
    Object.assign(m.style,{position:'fixed',inset:0,background:'rgba(0,0,0,.7)',display:'grid',placeItems:'center',zIndex:9999});
    m.addEventListener('click',()=>m.remove());
    const big=new Image();
    big.src=src;
    big.style.maxWidth='90vw';
    big.style.maxHeight='90vh';
    big.style.boxShadow='0 8px 24px rgba(0,0,0,.5)';
    m.appendChild(big);
    document.body.appendChild(m);
  }

  function getYouTubeId(u) {
    if (!u) return '';
    try {
      // Allow raw IDs
      if (/^[\w-]{11}$/.test(u)) return u;

      const x = new URL(u);
      // youtu.be/<id>
      if (x.hostname.includes('youtu.be')) {
        return (x.pathname.split('/')[1] || '').split('?')[0].split('&')[0];
      }
      // youtube.com/watch?v=<id>
      const v = x.searchParams.get('v');
      if (v) return v.split('&')[0];

      // youtube.com/shorts/<id>
      const mShorts = x.pathname.match(/\/shorts\/([\w-]{11})/);
      if (mShorts) return mShorts[1];

      // youtube.com/embed/<id>
      const mEmbed = x.pathname.match(/\/embed\/([\w-]{11})/);
      if (mEmbed) return mEmbed[1];

      // Last resort: first 11-char token
      const m = u.match(/([\w-]{11})/);
      return m ? m[1] : '';
    } catch {
      const m = String(u).match(/([\w-]{11})/);
      return m ? m[1] : '';
    }
  }

  (function setupVideoModal() {
    const modal = document.getElementById("videoModal");
    if (!modal) return;

    window.openVideoModal = function openVideoModal(url) {
      const id = window.getYouTubeId ? getYouTubeId(url) : "";
      if (!id) {
        if (typeof toast === "function") toast(I18N.t("invalid_youtube"), "error");
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

  const ADMIN_KEY_STORAGE = 'CK_ADMIN_KEY';
  function saveAdminKey(value) {
    if (!value) {
      return storageRemove(ADMIN_KEY_STORAGE);
    }
    return storageSet(ADMIN_KEY_STORAGE, value);
  }

  $('saveAdminKey')?.addEventListener('click', () => {
    const value = (keyInput?.value || '').trim();
    const persisted = saveAdminKey(value);
    toast(persisted ? 'Admin key saved' : 'Admin key saved for this session only (storage blocked).');
    if (value) {
      loadFeatureFlagsFromServer();
    }
  });

  document.addEventListener('DOMContentLoaded', () => {
    const saved = loadAdminKey();
    if (saved && keyInput) keyInput.value = saved;
  });

  async function loadFeatureFlagsFromServer() {
    try {
      const { res, body } = await adminFetch('/api/features');
      if (!res.ok) return;
      const data = body && typeof body === 'object' ? body : {};
      if (data && typeof data === 'object') {
        applyFeatureFlags(data);
      }
    } catch (err) {
      console.warn('feature flag fetch failed', err);
    }
  }

  function getAdminKey(){
    const el = document.getElementById('adminKey');
    return (el?.value || '').trim();
  }
  function ensureAdminKey() {
    return getAdminKey();
  }
  function ensureAdminKey() {
    return getAdminKey();
  }
  async function adminFetch(url, opts = {}) {
    const { idempotencyKey, headers: extraHeaders, ...fetchOpts } = opts;
    const key = ensureAdminKey();
    if (!key) {
      return {
        res: {
          ok: false,
          status: 428,
          statusText: 'Admin key required',
          headers: { get: () => null }
        },
        body: { error: ADMIN_KEY_REQUIRED_MSG, code: 'ADMIN_KEY_REQUIRED' }
      };
    }
    const headers = {
      'x-admin-key': key,
      'x-actor-role': 'admin',
      ...(extraHeaders || {})
    };
    if (idempotencyKey) headers['idempotency-key'] = idempotencyKey;
    const res = await fetch(url, { ...fetchOpts, headers });
    const ct = res.headers.get('content-type') || '';
    const body = ct.includes('application/json') ? await res.json().catch(()=>({})) : await res.text().catch(()=> '');
    return { res, body };
  }

  const ERROR_MESSAGES = {
    INVALID_AMOUNT: 'Enter a positive amount to continue.',
    INVALID_DELTA: 'Adjustment amount must be non-zero.',
    INVALID_USER: 'Choose a member before performing this action.',
    INVALID_PARENT_TX: 'Select a redeemed reward to refund.',
    INVALID_REASON: 'Pick a refund reason.',
    REFUND_NOT_ALLOWED: 'This reward has already been fully refunded.',
    OVER_REFUND: 'Amount exceeds the refundable balance.',
    FEATURE_DISABLED: 'This feature is currently turned off.',
    ROLE_REQUIRED: 'You do not have permission to do that.',
    INSUFFICIENT_FUNDS: 'Not enough balance to complete this action.',
    TOKEN_USED: 'This QR code has already been used.',
    hold_not_pending: 'This reward request is no longer pending.',
    invalid_payload: 'The request was missing required information.',
    scan_failed: 'Unable to redeem that QR code. Try again.',
    ADMIN_KEY_REQUIRED: ADMIN_KEY_REQUIRED_MSG,
    'PLEASE ENTER THE ADMINKEY FIRST': ADMIN_KEY_REQUIRED_MSG
  };

  function presentError(code, fallback) {
    if (!code && fallback) return fallback;
    const normalized = String(code || '').trim();
    if (!normalized) return fallback || 'Something went wrong.';
    const direct = ERROR_MESSAGES[normalized];
    if (direct) return direct;
    const upper = ERROR_MESSAGES[normalized.toUpperCase()];
    if (upper) return upper;
    return fallback || normalized.replace(/_/g, ' ');
  }

  function debounce(fn, wait = 300) {
    let timer = null;
    return function debounced(...args) {
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), wait);
    };
  }

  function renderQr(elId, text) {
    const el = $(elId);
    if (!el) return;
    el.innerHTML = '';
    if (!text) return;
    new QRCode(el, { text, width: 200, height: 200 });
  }

  function generateIdempotencyKey() {
    if (window.crypto?.randomUUID) {
      return window.crypto.randomUUID();
    }
    const rand = Math.random().toString(16).slice(2);
    return `refund-${Date.now()}-${rand}`;
  }

  const MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  function buildDateParts(year, month, day) {
    const y = Number(year);
    const m = Number(month);
    const d = Number(day);
    if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
    const date = new Date(Date.UTC(y, m - 1, d));
    if (date.getUTCFullYear() !== y || date.getUTCMonth() + 1 !== m || date.getUTCDate() !== d) return null;
    return { year: y, month: m, day: d, date };
  }

  function parseDobParts(value) {
    if (value === undefined || value === null) return null;
    const trimmed = String(value).trim();
    if (!trimmed) return null;

    const digits = trimmed.replace(/[^0-9]/g, '');
    if (digits.length === 8) {
      const parts = buildDateParts(digits.slice(0, 4), digits.slice(4, 6), digits.slice(6, 8));
      if (parts) return parts;
    }

    const isoMatch = trimmed.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (isoMatch) {
      const parts = buildDateParts(isoMatch[1], isoMatch[2], isoMatch[3]);
      if (parts) return parts;
    }

    const sanitized = trimmed
      .replace(/(\d+)(st|nd|rd|th)/gi, '$1')
      .replace(/[.,]/g, '')
      .replace(/-/g, ' ');
    const parsed = Date.parse(sanitized);
    if (!Number.isNaN(parsed)) {
      const date = new Date(parsed);
      return buildDateParts(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
    }

    return null;
  }

  function normalizeDobInput(value) {
    const trimmed = (value ?? '').toString().trim();
    if (!trimmed) return '';
    const parts = parseDobParts(trimmed);
    if (!parts) return trimmed;
    const month = String(parts.month).padStart(2, '0');
    const day = String(parts.day).padStart(2, '0');
    return `${parts.year.toString().padStart(4, '0')}-${month}-${day}`;
  }

  function formatDobFriendly(value) {
    const trimmed = (value ?? '').toString().trim();
    if (!trimmed) return '';
    const parts = parseDobParts(trimmed);
    if (!parts) return trimmed;
    const monthName = MONTH_NAMES[parts.month - 1] || parts.month;
    const day = parts.day;
    const suffix = (() => {
      const mod100 = day % 100;
      if (mod100 >= 11 && mod100 <= 13) return 'th';
      switch (day % 10) {
        case 1: return 'st';
        case 2: return 'nd';
        case 3: return 'rd';
        default: return 'th';
      }
    })();
    return `${parts.year}-${monthName} ${day}${suffix}`;
  }

  function normalizeSexValue(value) {
    const normalized = (value ?? '').toString().trim().toLowerCase();
    if (!normalized) return '';
    if (['boy', 'male', 'm'].includes(normalized)) return 'Boy';
    if (['girl', 'female', 'f'].includes(normalized)) return 'Girl';
    return '';
  }


  const memberIdInput = $('memberUserId');
  const memberStatusEl = $('memberStatus');
  const memberInfoDetails = $('memberInfoDetails');
  const memberBalanceContainer = $('memberBalanceContainer');
  const memberBalanceDetails = $('memberBalanceDetails');
  const memberBalanceSummaryValue = $('memberBalanceSummaryValue');
  const memberBalanceBody = $('memberBalanceBody');
  const memberEarnDetails = $('memberEarnDetails');
  const memberEarnSummaryValue = $('memberEarnSummaryValue');
  const memberEarnSummary = $('memberEarnSummary');
  const memberRedeemDetails = $('memberRedeemDetails');
  const memberRedeemSummaryValue = $('memberRedeemSummaryValue');
  const memberRedeemSummary = $('memberRedeemSummary');
  const memberRefundDetails = $('memberRefundDetails');
  const memberRefundSummaryValue = $('memberRefundSummaryValue');
  const memberRefundSummary = $('memberRefundSummary');
  const memberLedgerHost = $('memberLedger');
  const memberTableBody = $('memberTable')?.querySelector('tbody');
  const memberListStatus = $('memberListStatus');
  const memberSearchInput = $('memberSearch');
  const memberListSection = $('memberListSection');
  const memberEditModal = $('memberEditModal');
  const memberEditForm = $('memberEditForm');
  const memberEditIdInput = $('memberEditId');
  const memberEditNameInput = $('memberEditName');
  const memberEditDobInput = $('memberEditDob');
  const memberEditSexSelect = $('memberEditSex');
  const memberEditSaveBtn = $('btnMemberEditSave');

  const refundModal = $('refundModal');
  const refundForm = $('refundForm');
  const refundAmountInput = $('refundAmount');
  const refundReasonSelect = $('refundReason');
  const refundNotesInput = $('refundNotes');
  const refundRemainingText = $('refundRemaining');
  const refundConfirmBtn = $('btnRefundConfirm');
  const activityTableBody = $('activityTable')?.querySelector('tbody');
  const activityStatus = $('activityStatus');
  const activityVerb = $('activityVerb');
  const activityActor = $('activityActor');
  const activityFrom = $('activityFrom');
  const activityTo = $('activityTo');
  const btnActivityRefresh = $('btnActivityRefresh');
  const activityRowIndex = new Map();

  const MEMBER_EDIT_SAVE_DEFAULT_TEXT = memberEditSaveBtn?.textContent || 'Save Changes';
  let activeMemberEdit = null;
  let activeRefundContext = null;
  let latestHints = null;
  let latestHintsErrorMessage = null;
  let featureFlags = { refunds: true };

  function applyFeatureFlags(flags = {}) {
    featureFlags = { ...featureFlags, ...flags };
    const refundsEnabled = !!featureFlags.refunds;
    document.documentElement.setAttribute('data-feature-refunds', refundsEnabled ? 'on' : 'off');
    const nodes = document.querySelectorAll('[data-feature="refunds"]');
    nodes.forEach((el) => {
      if (el instanceof HTMLButtonElement) {
        el.disabled = !refundsEnabled;
        el.title = refundsEnabled ? '' : 'Refunds are disabled by policy.';
      } else if (!refundsEnabled) {
        el.setAttribute('aria-disabled', 'true');
      } else {
        el.removeAttribute('aria-disabled');
      }
    });
    if (!refundsEnabled && typeof closeRefundModal === 'function') {
      closeRefundModal();
    }
  }

  function renderBalanceFromHints(hints) {
    if (!memberBalanceSummaryValue || !memberBalanceBody) return;
    if (!hints) {
      memberBalanceSummaryValue.textContent = '—';
      const message = latestHintsErrorMessage || 'Balance info will appear here.';
      setPlaceholder(memberBalanceBody, message);
      return;
    }
    const balanceValue = Number(hints.balance ?? 0);
    const formattedBalance = formatTokenValue(balanceValue);
    memberBalanceSummaryValue.textContent = `${formattedBalance} tokens`;
    memberBalanceBody.innerHTML = '';
    const line = document.createElement('div');
    line.textContent = `Current balance: ${formattedBalance} tokens.`;
    memberBalanceBody.appendChild(line);
    const details = document.createElement('small');
    details.className = 'muted';
    const redeemInfo = hints.can_redeem
      ? `Can redeem up to ${formatTokenValue(hints.max_redeem ?? balanceValue)} tokens.`
      : 'Not enough balance to redeem right now.';
    details.textContent = redeemInfo;
    memberBalanceBody.appendChild(details);
  }

  function applyStateHints(hints, { errorMessage = null } = {}) {
    latestHints = hints ? { ...hints } : null;
    latestHintsErrorMessage = hints ? null : errorMessage || null;
    if (!hints) {
      renderBalanceFromHints(null);
      return;
    }
    applyFeatureFlags(hints.features || {});
    renderBalanceFromHints(hints);
    const refundsEnabled = !!(hints.features?.refunds);
    const refundAllowed = refundsEnabled && !!hints.can_refund;
    document.querySelectorAll('[data-feature="refunds"]').forEach((node) => {
      if (!(node instanceof HTMLButtonElement)) return;
      node.disabled = !refundAllowed;
      node.title = refundAllowed ? '' : refundsEnabled ? 'Nothing refundable right now.' : 'Refunds are disabled by policy.';
    });
  }

  function getMemberIdInfo() {
    const raw = (memberIdInput?.value || '').trim();
    return { raw, normalized: raw.toLowerCase() };
  }

  function normalizeMemberInput() {
    if (!memberIdInput) return getMemberIdInfo();
    const info = getMemberIdInfo();
    if (info.raw && info.raw !== info.normalized) {
      memberIdInput.value = info.normalized;
      return { raw: info.normalized, normalized: info.normalized };
    }
    return info;
  }

  function setPlaceholder(container, text) {
    if (!container) return;
    container.innerHTML = '';
    if (!text) return;
    const div = document.createElement('div');
    div.className = 'muted';
    div.textContent = text;
    container.appendChild(div);
  }

  function resetSummaryValue(el) {
    if (el) el.textContent = '—';
  }

  function collapseDetails(detailsEl) {
    detailsEl?.removeAttribute('open');
  }

  function clearMemberLedger(message = 'Redeemed rewards will appear here.') {
    setPlaceholder(memberLedgerHost, message);
    setPlaceholder(memberEarnSummary, 'Earn activity will appear here.');
    setPlaceholder(memberRedeemSummary, 'Redeem activity will appear here.');
    setPlaceholder(memberRefundSummary, 'Refund activity will appear here.');
    if (memberBalanceBody) setPlaceholder(memberBalanceBody, 'Balance info will appear here.');
    resetSummaryValue(memberBalanceSummaryValue);
    resetSummaryValue(memberEarnSummaryValue);
    resetSummaryValue(memberRedeemSummaryValue);
    resetSummaryValue(memberRefundSummaryValue);
    collapseDetails(memberBalanceDetails);
    collapseDetails(memberEarnDetails);
    collapseDetails(memberRedeemDetails);
    collapseDetails(memberRefundDetails);
    if (memberBalanceContainer) memberBalanceContainer.hidden = true;
    activeRefundContext = null;
  }

  function formatTokenValue(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) {
      return `${value ?? 0}`;
    }
    return num.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 0 });
  }

  function renderSummarySection({ summary, valueEl, container, emptyMessage }) {
    if (valueEl) {
      if (!summary) {
        valueEl.textContent = 'No activity';
      } else {
        const d7 = formatTokenValue(summary.d7 ?? 0);
        const d30 = formatTokenValue(summary.d30 ?? 0);
        valueEl.textContent = `7d ${d7} · 30d ${d30}`;
      }
    }
    if (!container) return;
    container.innerHTML = '';
    if (!summary) {
      setPlaceholder(container, emptyMessage);
      return;
    }
    const rows = [
      { label: 'Past 7 days', value: summary.d7 ?? 0 },
      { label: 'Past 30 days', value: summary.d30 ?? 0 }
    ];
    for (const row of rows) {
      const chip = document.createElement('div');
      chip.className = 'chip';
      chip.textContent = row.label;
      const span = document.createElement('span');
      span.textContent = `${formatTokenValue(row.value)} tokens`;
      chip.appendChild(span);
      container.appendChild(chip);
    }
  }

  function renderLedgerSummary(summary) {
    renderSummarySection({
      summary: summary?.earn,
      valueEl: memberEarnSummaryValue,
      container: memberEarnSummary,
      emptyMessage: 'No earn activity recorded.'
    });
    renderSummarySection({
      summary: summary?.redeem,
      valueEl: memberRedeemSummaryValue,
      container: memberRedeemSummary,
      emptyMessage: 'No redeemed activity recorded.'
    });
    renderSummarySection({
      summary: summary?.refund,
      valueEl: memberRefundSummaryValue,
      container: memberRefundSummary,
      emptyMessage: 'No refunds recorded.'
    });
  }

  function renderRedeemLedger(redeems = []) {
    if (!memberLedgerHost) return;
    activeRefundContext = null;
    memberLedgerHost.innerHTML = '';
    if (!Array.isArray(redeems) || !redeems.length) {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = 'No redeemed rewards yet.';
      memberLedgerHost.appendChild(empty);
      return;
    }
    for (const entry of redeems) {
      const card = document.createElement('div');
      card.className = 'ledger-entry';

      const header = document.createElement('div');
      header.className = 'ledger-entry-header';
      const title = document.createElement('div');
      title.className = 'ledger-entry-title';
      const amount = Math.abs(Number(entry.redeem_amount || entry.delta || 0));
      const label = entry.note || entry.action || 'Redeem';
      title.textContent = `${label} · ${amount} tokens`;
      header.appendChild(title);

      const badge = document.createElement('span');
      badge.className = 'badge';
      if (entry.refund_status === 'refunded') {
        badge.classList.add('success');
        badge.textContent = 'Refunded';
      } else if (entry.refund_status === 'partial') {
        badge.classList.add('warning');
        badge.textContent = `Partial refund`;
      } else {
        badge.textContent = 'Redeemed';
      }
      header.appendChild(badge);

      const actions = document.createElement('div');
      actions.className = 'ledger-actions';
      if (entry.remaining_refundable > 0) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'primary';
        btn.dataset.feature = 'refunds';
        btn.textContent = entry.remaining_refundable === amount
          ? 'Refund'
          : `Refund (remaining ${entry.remaining_refundable})`;
        btn.addEventListener('click', () => openRefundModal(entry));
        actions.appendChild(btn);
      } else {
        const note = document.createElement('span');
        note.className = 'refund-remaining';
        note.textContent = 'No refundable amount remaining';
        actions.appendChild(note);
      }
      header.appendChild(actions);

      card.appendChild(header);

      const meta = document.createElement('div');
      meta.className = 'meta';
      const when = document.createElement('span');
      when.textContent = `Redeemed ${formatTime(entry.at)}`;
      meta.appendChild(when);
      if (entry.actor) {
        const actor = document.createElement('span');
        actor.textContent = `By ${entry.actor}`;
        meta.appendChild(actor);
      }
      const totals = document.createElement('span');
      totals.textContent = `Refunded ${entry.refunded_amount || 0} · Remaining ${entry.remaining_refundable || 0}`;
      meta.appendChild(totals);
      card.appendChild(meta);

      if (Array.isArray(entry.refunds) && entry.refunds.length) {
        const list = document.createElement('div');
        list.className = 'ledger-refund-list';
        for (const refund of entry.refunds) {
          const line = document.createElement('div');
          line.className = 'ledger-refund';
          const headline = document.createElement('strong');
          headline.textContent = `+${Number(refund.delta || 0)} tokens on ${formatTime(refund.at)}`;
          line.appendChild(headline);
          const details = document.createElement('span');
          const parts = [];
          if (refund.refund_reason) parts.push(refund.refund_reason.replace(/_/g, ' '));
          if (refund.actor) parts.push(refund.actor);
          if (refund.id) parts.push(`#${refund.id}`);
          details.textContent = parts.join(' · ');
          line.appendChild(details);
          if (refund.refund_notes) {
            const notes = document.createElement('span');
            notes.textContent = refund.refund_notes;
            line.appendChild(notes);
          }
          list.appendChild(line);
        }
        card.appendChild(list);
      }

      memberLedgerHost.appendChild(card);
    }
  }

  function openRefundModal(entry) {
    if (!refundModal || !refundAmountInput || !refundReasonSelect || !refundConfirmBtn) return;
    activeRefundContext = entry;
    const remaining = Math.max(0, Number(entry.remaining_refundable || 0));
    refundAmountInput.value = remaining || Math.abs(Number(entry.redeem_amount || entry.delta || 0)) || 1;
    refundAmountInput.max = remaining || '';
    refundAmountInput.focus();
    refundReasonSelect.value = refundReasonSelect.options[0]?.value || 'duplicate';
    if (refundNotesInput) refundNotesInput.value = '';
    if (refundRemainingText) {
      const label = entry.note || entry.action || 'reward';
      refundRemainingText.textContent = `Up to ${remaining} tokens can be refunded for “${label}”.`;
    }
    refundConfirmBtn.disabled = false;
    refundConfirmBtn.textContent = 'Confirm refund';
    refundModal.classList.remove('hidden');
  }

  function closeRefundModal() {
    if (!refundModal) return;
    refundModal.classList.add('hidden');
    activeRefundContext = null;
  }

  async function refreshMemberLedger(userId, { showPanels = true } = {}) {
    if (!userId) {
      clearMemberLedger();
      return null;
    }
    if (showPanels && memberBalanceContainer) memberBalanceContainer.hidden = false;
    if (memberLedgerHost) setPlaceholder(memberLedgerHost, 'Loading redeemed items…');
    if (memberEarnSummaryValue) memberEarnSummaryValue.textContent = 'Loading…';
    if (memberRedeemSummaryValue) memberRedeemSummaryValue.textContent = 'Loading…';
    if (memberRefundSummaryValue) memberRefundSummaryValue.textContent = 'Loading…';
    if (memberEarnSummary) setPlaceholder(memberEarnSummary, 'Loading earn activity…');
    if (memberRedeemSummary) setPlaceholder(memberRedeemSummary, 'Loading redeem activity…');
    if (memberRefundSummary) setPlaceholder(memberRefundSummary, 'Loading refund activity…');
    try {
      const { res, body } = await adminFetch(`/ck/ledger/${encodeURIComponent(userId)}`);
      if (res.status === 401) {
        clearMemberLedger('Admin key invalid.');
        toast(ADMIN_INVALID_MSG, 'error');
        return null;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load ledger');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      renderLedgerSummary(data.summary || {});
      renderRedeemLedger(Array.isArray(data.redeems) ? data.redeems : []);
      if (data.hints) {
        applyStateHints(data.hints);
      } else {
        applyStateHints(null);
      }
      if (showPanels && memberBalanceContainer) memberBalanceContainer.hidden = false;
      return data;
    } catch (err) {
      console.error(err);
      const friendly = presentError(err?.message, 'Failed to load ledger history.');
      if (memberLedgerHost) setPlaceholder(memberLedgerHost, friendly);
      renderLedgerSummary(null);
      applyStateHints(null, { errorMessage: presentError(err?.message, 'Balance temporarily unavailable. Please try again.') });
      return null;
    }
  }

  function requireMemberId({ silent = false } = {}) {
    const info = normalizeMemberInput();
    if (!info.normalized) {
      if (!silent) toast('Enter user id', 'error');
      memberIdInput?.focus();
      return null;
    }
    return info.normalized;
  }

  function setMemberStatus(message) {
    if (memberStatusEl) memberStatusEl.textContent = message || '';
  }

  function setMemberInfoMessage(message) {
    if (!memberInfoDetails) return;
    memberInfoDetails.innerHTML = '';
    const div = document.createElement('div');
    div.className = 'muted';
    div.textContent = message;
    memberInfoDetails.appendChild(div);
  }

  // Collapsible panels: pairs <button.card-toggle aria-controls="..."> + <div id="...">
  function initCollapsibles() {
    const toggles = document.querySelectorAll('.card .card-toggle[aria-controls]');
    toggles.forEach(btn => {
      const targetId = btn.getAttribute('aria-controls');
      const target = document.getElementById(targetId);
      if (!target) return;

      const arrow = btn.querySelector('[data-arrow]');

      const setState = (expanded) => {
        btn.setAttribute('aria-expanded', String(expanded));
        target.hidden = !expanded;
        if (arrow) arrow.textContent = expanded ? '▲' : '▼';
      };

      // start collapsed unless markup says otherwise
      setState(btn.getAttribute('aria-expanded') === 'true');

      btn.addEventListener('click', () => {
        const next = btn.getAttribute('aria-expanded') !== 'true';
        setState(next);
      });
    });
  }

  // ensure DOM is ready, then call once
  document.addEventListener('DOMContentLoaded', initCollapsibles);

  function renderMemberInfo(member) {
    if (!memberInfoDetails) return;
    memberInfoDetails.innerHTML = '';
    if (!member) {
      setMemberInfoMessage('No member found.');
      return;
    }
    const nameEl = document.createElement('div');
    nameEl.style.fontWeight = '600';
    nameEl.textContent = member.name || member.userId;
    memberInfoDetails.appendChild(nameEl);

    const idEl = document.createElement('div');
    idEl.className = 'muted mono';
    idEl.textContent = `ID: ${member.userId}`;
    memberInfoDetails.appendChild(idEl);

    const dobEl = document.createElement('div');
    dobEl.className = 'muted';
    const dobDisplay = formatDobFriendly(member.dob || '') || '—';
    dobEl.textContent = `DOB: ${dobDisplay}`;
    memberInfoDetails.appendChild(dobEl);

    const sexEl = document.createElement('div');
    sexEl.className = 'muted';
    const sexDisplay = normalizeSexValue(member.sex) || member.sex || '—';
    sexEl.textContent = `Sex: ${sexDisplay}`;
    memberInfoDetails.appendChild(sexEl);
  }

  function memberIdChanged({ loadActivityNow = true } = {}) {
    normalizeMemberInput();
    setMemberStatus('');
    setMemberInfoMessage('Enter a user ID and click Member Info to view details.');
    clearMemberLedger('Redeemed rewards will appear here.');
    loadHolds();
    loadActivity();
  }

  memberIdInput?.addEventListener('change', (event) => memberIdChanged({ loadActivityNow: event?.isTrusted !== false }));
  memberIdInput?.addEventListener('input', () => {
    if (!memberIdInput.value.trim()) resetActivityView();
  });
  memberIdInput?.addEventListener('blur', normalizeMemberInput);
  memberIdInput?.addEventListener('keyup', (event) => {
    if (event.key === 'Enter') memberIdChanged({ loadActivityNow: event.isTrusted !== false });
  });

  $('btnMemberInfo')?.addEventListener('click', async () => {
    const userId = requireMemberId();
    if (!userId) return;
    loadActivity();
    setMemberStatus('');
    setMemberInfoMessage('Loading member info...');
    clearMemberLedger();
    try {
      const { res, body } = await adminFetch(`/api/members/${encodeURIComponent(userId)}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        setMemberInfoMessage('Admin key invalid.');
        return;
      }
      if (res.status === 404) {
        setMemberInfoMessage(`No profile found for "${userId}".`);
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load member');
        throw new Error(msg);
      }
      const member = body && typeof body === 'object' ? body : null;
      renderMemberInfo(member);
      if (member) {
        setMemberStatus(`Loaded member ${member.userId}.`);
        clearMemberLedger();
      } else {
        clearMemberLedger('Redeemed rewards will appear here.');
      }
    } catch (err) {
      console.error(err);
      setMemberInfoMessage(err.message || 'Failed to load member.');
      toast(err.message || 'Failed to load member', 'error');
      clearMemberLedger(err.message || 'Redeem history unavailable.');
    }
  });

  $('btnMemberBalance')?.addEventListener('click', async () => {
    const userId = requireMemberId();
    if (!userId) return;
    loadActivity();
    setMemberStatus('Fetching balance...');
    if (memberBalanceContainer) memberBalanceContainer.hidden = false;
    collapseDetails(memberBalanceDetails);
    collapseDetails(memberEarnDetails);
    collapseDetails(memberRedeemDetails);
    collapseDetails(memberRefundDetails);
    if (memberBalanceSummaryValue) memberBalanceSummaryValue.textContent = 'Loading…';
    if (memberBalanceBody) setPlaceholder(memberBalanceBody, 'Loading balance…');
    const data = await refreshMemberLedger(userId, { showPanels: true });
    if (data?.hints) {
      const formattedBalance = formatTokenValue(Number(data.hints.balance ?? 0));
      setMemberStatus(`Balance: ${formattedBalance} tokens.`);
    } else {
      setMemberStatus('Balance unavailable.');
    }
  });

  async function submitMemberRegistration() {
    const idEl = $('memberRegisterId');
    const nameEl = $('memberRegisterName');
    const dobEl = $('memberRegisterDob');
    const sexEl = $('memberRegisterSex');
    const userId = (idEl?.value || '').trim().toLowerCase();
    const name = (nameEl?.value || '').trim();
    const dob = (dobEl?.value || '').trim();
    const sex = (sexEl?.value || '').trim();
    if (!userId || !name) {
      toast('User ID and name required', 'error');
      return;
    }
    setMemberStatus('Registering member...');
    try {
      const { res, body } = await adminFetch('/api/members', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, name, dob: dob || undefined, sex: sex || undefined })
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        setMemberStatus('Admin key invalid.');
        return;
      }
      if (res.status === 409) {
        throw new Error('User ID already exists');
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to register');
        throw new Error(msg);
      }
      toast('Member registered');
      setMemberStatus(`Registered member ${userId}.`);
      if (idEl) idEl.value = '';
      if (nameEl) nameEl.value = '';
      if (dobEl) dobEl.value = '';
      if (sexEl) sexEl.value = '';
      if (memberIdInput) {
        memberIdInput.value = userId;
        memberIdChanged();
      }
      await loadMembersList();
    } catch (err) {
      console.error(err);
      setMemberStatus(err.message || 'Failed to register member.');
      toast(err.message || 'Failed to register member', 'error');
    }
  }

  $('btnMemberRegister')?.addEventListener('click', submitMemberRegistration);

  function autoFormatDobField(input) {
    if (!input) return;
    const formatted = formatDobFriendly(input.value);
    input.value = formatted;
  }

  function openMemberEditModal(member) {
    if (!memberEditModal || !memberEditForm) return;
    activeMemberEdit = { ...member };
    if (memberEditIdInput) memberEditIdInput.value = member.userId || '';
    if (memberEditNameInput) memberEditNameInput.value = member.name || '';
    if (memberEditDobInput) {
      memberEditDobInput.value = member.dob ? formatDobFriendly(member.dob) : '';
      autoFormatDobField(memberEditDobInput);
    }
    if (memberEditSexSelect) {
      const normalizedSex = normalizeSexValue(member.sex) || '';
      memberEditSexSelect.value = normalizedSex;
    }
    memberEditModal.classList.remove('hidden');
    if (memberEditNameInput) memberEditNameInput.focus();
  }

  function closeMemberEditModal() {
    if (!memberEditModal) return;
    memberEditModal.classList.add('hidden');
    memberEditForm?.reset?.();
    if (memberEditSaveBtn) {
      memberEditSaveBtn.disabled = false;
      memberEditSaveBtn.textContent = MEMBER_EDIT_SAVE_DEFAULT_TEXT;
    }
    activeMemberEdit = null;
  }

  memberEditDobInput?.addEventListener('blur', () => autoFormatDobField(memberEditDobInput));
  memberEditDobInput?.addEventListener('change', () => autoFormatDobField(memberEditDobInput));

  memberEditForm?.addEventListener('submit', async (event) => {
    event?.preventDefault();
    if (!activeMemberEdit) {
      closeMemberEditModal();
      return;
    }
    const userId = activeMemberEdit.userId;
    const name = memberEditNameInput?.value?.trim() || '';
    if (!name) {
      toast('Name required', 'error');
      memberEditNameInput?.focus?.();
      return;
    }
    const dob = normalizeDobInput(memberEditDobInput?.value || '');
    const sexRaw = memberEditSexSelect?.value || '';
    const sex = normalizeSexValue(sexRaw) || '';
    if (memberEditSaveBtn) {
      memberEditSaveBtn.disabled = true;
      memberEditSaveBtn.textContent = 'Saving...';
    }
    try {
      const payload = { name, dob: dob || undefined, sex: sex || undefined };
      const { res, body } = await adminFetch(`/api/members/${encodeURIComponent(userId)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to update member');
        throw new Error(msg);
      }
      toast('Member updated');
      const updated = body && typeof body === 'object' ? body.member || body : null;
      closeMemberEditModal();
      await loadMembersList();
      if (updated && memberIdInput?.value === updated.userId) {
        renderMemberInfo(updated);
      }
    } catch (err) {
      console.error(err);
      toast(err.message || 'Failed to update member', 'error');
    } finally {
      if (memberEditSaveBtn && memberEditModal && !memberEditModal.classList.contains('hidden')) {
        memberEditSaveBtn.disabled = false;
        memberEditSaveBtn.textContent = MEMBER_EDIT_SAVE_DEFAULT_TEXT;
      }
    }
  });

  function editMember(member) {
    if (!member) return;
    openMemberEditModal(member);
  }

  async function deleteMember(member) {
    if (!member) return;
    if (!confirm(`Delete member "${member.userId}"? This cannot be undone.`)) return;
    try {
      const { res, body } = await adminFetch(`/api/members/${encodeURIComponent(member.userId)}`, {
        method: 'DELETE'
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (res.status === 404) {
        toast('Member already removed', 'error');
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to delete member');
        throw new Error(msg);
      }
      toast('Member deleted');
      if (memberIdInput?.value === member.userId) {
        memberIdInput.value = '';
        memberIdChanged();
      }
      await loadMembersList();
    } catch (err) {
      console.error(err);
      toast(err.message || 'Failed to delete member', 'error');
    }
  }

  async function loadMembersList() {
    if (!memberTableBody) return;
    const search = (memberSearchInput?.value || '').trim().toLowerCase();
    if (!search) {
      memberTableBody.innerHTML = '';
      if (memberListStatus) memberListStatus.textContent = 'Type in the search box to list members.';
      if (memberListSection) memberListSection.hidden = false;
      return;
    }
    if (memberListSection) memberListSection.hidden = false;
    memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">Loading...</td></tr>';
    if (memberListStatus) memberListStatus.textContent = '';
    try {
      const qs = search ? `?search=${encodeURIComponent(search)}` : '';
      const { res, body } = await adminFetch(`/api/members${qs}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">Admin key invalid.</td></tr>';
        if (memberListStatus) memberListStatus.textContent = 'Admin key invalid.';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load members');
        throw new Error(msg);
      }
      const rows = Array.isArray(body) ? body : [];
      memberTableBody.innerHTML = '';
      if (!rows.length) {
        memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">No members found.</td></tr>';
        if (memberListStatus) memberListStatus.textContent = 'No members found.';
        return;
      }
      for (const row of rows) {
        const tr = document.createElement('tr');
        const idCell = document.createElement('td');
        idCell.textContent = row.userId;
        tr.appendChild(idCell);
        const nameCell = document.createElement('td');
        nameCell.textContent = row.name || '';
        tr.appendChild(nameCell);
        const dobCell = document.createElement('td');
        dobCell.textContent = formatDobFriendly(row.dob || '') || '';
        tr.appendChild(dobCell);
        const sexCell = document.createElement('td');
        sexCell.textContent = normalizeSexValue(row.sex) || row.sex || '';
        tr.appendChild(sexCell);
        const actions = document.createElement('td');
        actions.style.display = 'flex';
        actions.style.flexWrap = 'wrap';
        actions.style.gap = '6px';
        actions.style.alignItems = 'center';

        const editBtn = document.createElement('button');
        editBtn.textContent = 'Edit';
        editBtn.addEventListener('click', () => editMember(row));
        actions.appendChild(editBtn);

        const deleteBtn = document.createElement('button');
        deleteBtn.textContent = 'Delete';
        deleteBtn.addEventListener('click', () => deleteMember(row));
        actions.appendChild(deleteBtn);

        tr.appendChild(actions);
        memberTableBody.appendChild(tr);
      }
      if (memberListStatus) {
        const count = rows.length;
        memberListStatus.textContent = `Showing ${count} member${count === 1 ? '' : 's'}.`;
      }
    } catch (err) {
      console.error(err);
      memberTableBody.innerHTML = `<tr><td colspan="5" class="muted">${err.message || 'Failed to load members.'}</td></tr>`;
      if (memberListStatus) memberListStatus.textContent = err.message || 'Failed to load members.';
    }
  }

  $('btnMemberReload')?.addEventListener('click', loadMembersList);

  let memberSearchTimer = null;
  memberSearchInput?.addEventListener('input', () => {
    clearTimeout(memberSearchTimer);
    memberSearchTimer = setTimeout(loadMembersList, 250);
  });

  function parseTokenFromScan(data) {
    try {
      if (!data) return null;
      let token = data.trim();
      let url = '';
      if (token.startsWith('http')) {
        const urlObj = new URL(token);
        if (urlObj.searchParams.get('t')) token = urlObj.searchParams.get('t');
        url = urlObj.toString();
      }
      const part = token.split('.')[0];
      const padded = part.replace(/-/g, '+').replace(/_/g, '/');
      const mod = padded.length % 4;
      const base = mod ? padded + '='.repeat(4 - mod) : padded;
      const payload = JSON.parse(atob(base));
      if (!url && typeof window !== 'undefined' && window.location) {
        url = `${window.location.origin}/scan?t=${encodeURIComponent(token)}`;
      }
      return { token, payload, url };
    } catch (e) {
      console.error('parse token failed', e);
      return null;
    }
  }

  async function generateIssueQr() {
    const userId = requireMemberId();
    if (!userId) return;
    const amount = Number($('issueAmount').value);
    const note = $('issueNote').value.trim();
    if (!Number.isFinite(amount) || amount <= 0) {
      toast('Enter a positive amount', 'error');
      return;
    }
    $('issueStatus').textContent = 'Generating QR...';
    try {
      const { res, body } = await adminFetch('/api/tokens/give', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, amount, note: note || undefined })
      });
      const data = body && typeof body === 'object' ? body : {};
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        $('issueStatus').textContent = 'Admin key invalid.';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'request failed');
        throw new Error(msg);
      }
      renderQr('qrIssue', data.qrText);
      $('issueLink').value = data.qrText || '';
      $('issueStatus').textContent = `QR expires in 2 minutes. Amount ${data.amount ?? '?'} points.`;
      toast('QR ready');
    } catch (err) {
      console.error(err);
      $('issueStatus').textContent = 'Failed to generate QR.';
      toast(err.message || 'Failed', 'error');
    }
  }
  $('btnIssueGenerate')?.addEventListener('click', generateIssueQr);

  $('btnIssueCopy')?.addEventListener('click', () => {
    const text = $('issueLink').value;
    if (!text) return toast('Nothing to copy', 'error');
    navigator.clipboard?.writeText(text).then(() => toast('Link copied')).catch(() => toast('Copy failed', 'error'));
  });

  // ===== Holds =====
  const holdsTable = $('holdsTable')?.querySelector('tbody');
  async function loadHolds() {
    if (!holdsTable) return;
    const status = $('holdFilter')?.value || 'pending';
    const memberInfo = normalizeMemberInput() || {};
    const rawUserId = (memberInfo.raw || '').trim();
    const normalizedUser = (memberInfo.normalized || '').trim();
    holdsTable.innerHTML = '';
    if (!normalizedUser) {
      const msg = 'Enter a user ID in Member Management to view holds.';
      $('holdsStatus').textContent = msg;
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 6;
      cell.className = 'muted';
      cell.textContent = msg;
      row.appendChild(cell);
      holdsTable.appendChild(row);
      return;
    }
    $('holdsStatus').textContent = 'Loading...';
    try {
      const params = new URLSearchParams({ status });
      if (normalizedUser) params.set('userId', normalizedUser);
      const { res, body } = await adminFetch(`/api/holds?${params.toString()}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        $('holdsStatus').textContent = 'Admin key invalid.';
        holdsTable.innerHTML = '<tr><td colspan="6" class="muted">Admin key invalid.</td></tr>';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'failed');
        throw new Error(msg);
      }
      const rows = Array.isArray(body) ? body : [];
      if (!rows.length) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 6;
        cell.className = 'muted';
        cell.textContent = `No holds for "${rawUserId}".`;
        row.appendChild(cell);
        holdsTable.appendChild(row);
        $('holdsStatus').textContent = `No holds for "${rawUserId}".`;
        return;
      }
      const frag = document.createDocumentFragment();
      for (const row of rows) {
        const tr = document.createElement('tr');
        tr.dataset.holdId = row.id;
        tr.innerHTML = `
          <td>${formatTime(row.createdAt)}</td>
          <td>${row.userId}</td>
          <td>${row.itemName || ''}</td>
          <td>${row.quotedCost ?? ''}</td>
          <td>${row.status}</td>
          <td class="actions"></td>
        `;
        const actions = tr.querySelector('.actions');
        if (row.status === 'pending') {
          const cancelBtn = document.createElement('button');
          cancelBtn.textContent = 'Cancel';
          cancelBtn.addEventListener('click', () => cancelHold(row.id));
          actions.appendChild(cancelBtn);
        } else {
          actions.textContent = '-';
        }
        frag.appendChild(tr);
      }
      holdsTable.appendChild(frag);
      const count = rows.length;
      const suffix = count === 1 ? '' : 's';
      $('holdsStatus').textContent = `Showing ${count} hold${suffix} for "${rawUserId}".`;
    } catch (err) {
      console.error(err);
      $('holdsStatus').textContent = err.message || 'Failed to load holds';
    }
  }
  $('btnReloadHolds')?.addEventListener('click', loadHolds);
  $('holdFilter')?.addEventListener('change', loadHolds);
  document.addEventListener('DOMContentLoaded', loadHolds);

  memberEditModal?.addEventListener('click', (event) => {
    const target = event.target;
    if (target?.dataset?.close !== undefined || target === memberEditModal || target?.classList?.contains('modal-backdrop')) {
      event.preventDefault();
      closeMemberEditModal();
    }
  });

  refundModal?.addEventListener('click', (event) => {
    const target = event.target;
    if (target?.dataset?.close !== undefined || target === refundModal || target?.classList?.contains('modal-backdrop')) {
      event.preventDefault();
      closeRefundModal();
    }
  });

  window.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && memberEditModal && !memberEditModal.classList.contains('hidden')) {
      closeMemberEditModal();
    }
    if (event.key === 'Escape' && refundModal && !refundModal.classList.contains('hidden')) {
      closeRefundModal();
    }
  });

  refundForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!activeRefundContext) {
      toast('Select a redeem entry to refund.', 'error');
      return;
    }
    const userId = activeRefundContext.userId || activeRefundContext.user_id || '';
    const remaining = Math.max(0, Number(activeRefundContext.remaining_refundable || 0));
    const amount = Number(refundAmountInput?.value || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      toast('Enter a positive refund amount.', 'error');
      return;
    }
    if (remaining && amount > remaining + 0.0001) {
      toast(`Amount exceeds remaining refundable tokens (${remaining}).`, 'error');
      return;
    }
    const payload = {
      user_id: userId,
      redeem_tx_id: activeRefundContext.id,
      amount,
      reason: refundReasonSelect?.value || 'duplicate',
      notes: (refundNotesInput?.value || '').trim() || undefined,
      idempotency_key: generateIdempotencyKey()
    };
    refundConfirmBtn.disabled = true;
    refundConfirmBtn.textContent = 'Processing…';
    try {
      const { res, body } = await adminFetch('/ck/refund', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        refundConfirmBtn.disabled = false;
        refundConfirmBtn.textContent = 'Confirm refund';
        return;
      }
      if (res.status === 429) {
        const retry = Number(body?.retry_after_ms || 0);
        const seconds = retry ? Math.ceil(retry / 1000) : null;
        const msg = seconds ? `Too many refunds. Try again in ${seconds} seconds.` : 'Too many refunds right now. Try again soon.';
        throw new Error(msg);
      }
      if (res.status === 409 && body && typeof body === 'object') {
        toast(presentError(body.error || 'REFUND_EXISTS', 'This refund was already recorded.'), 'warning');
        closeRefundModal();
        await refreshMemberLedger(userId);
        applyStateHints(body.hints || null);
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Refund failed');
        throw new Error(msg);
      }
      toast(`${amount} tokens returned.`, 'success');
      closeRefundModal();
      await refreshMemberLedger(userId);
      applyStateHints(body?.hints || null);
      loadHolds();
    } catch (err) {
      console.error(err);
      toast(presentError(err.message, 'Refund failed'), 'error');
      refundConfirmBtn.disabled = false;
      refundConfirmBtn.textContent = 'Confirm refund';
    }
  });

  async function cancelHold(id) {
    if (!confirm('Cancel this hold?')) return;
    try {
      const { res, body } = await adminFetch(`/api/holds/${id}/cancel`, { method: 'POST' });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Cancel failed');
        throw new Error(msg);
      }
      toast('Hold released');
      applyStateHints(body?.hints || null);
      loadHolds();
    } catch (err) {
      toast(presentError(err.message, 'Cancel failed'), 'error');
    }
  }

  function renderActivity(rows = [], { emptyMessage = 'No activity yet.' } = {}) {
    if (!activityTableBody) return;
    activityRowIndex.clear();
    activityTableBody.innerHTML = '';
    if (!rows.length) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 9;
      td.className = 'muted';
      td.textContent = emptyMessage;
      tr.appendChild(td);
      activityTableBody.appendChild(tr);
      return;
    }
    const frag = document.createDocumentFragment();
    for (const row of rows) {
      const tr = document.createElement('tr');
      const txId = row.id ? String(row.id) : '';
      if (txId) {
        tr.dataset.txId = txId;
        activityRowIndex.set(txId, tr);
      }
      if (row.parent_tx_id) {
        tr.dataset.parentId = String(row.parent_tx_id);
      }
      const deltaNum = Number(row.delta || 0);
      const deltaText = `${deltaNum > 0 ? '+' : ''}${formatTokenValue(deltaNum)}`;
      const balanceText = formatTokenValue(row.balance_after ?? row.balanceAfter ?? 0);
      const cells = [
        formatTime(row.at),
        row.userId || '',
        row.verb || '',
        row.action || '',
        deltaText,
        balanceText,
        row.actor || '',
        row.parent_tx_id || '',
        txId
      ];
      cells.forEach((value, idx) => {
        const td = document.createElement('td');
        if ((idx === 7 || idx === 8) && value) {
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'link-button';
          btn.dataset.linkTarget = String(value);
          btn.textContent = value;
          td.appendChild(btn);
        } else {
          td.textContent = value ?? '';
        }
        tr.appendChild(td);
      });
      frag.appendChild(tr);
    }
    activityTableBody.appendChild(frag);
    activityTableBody.querySelectorAll('.link-button').forEach(btn => {
      btn.addEventListener('click', () => {
        const targetId = btn.getAttribute('data-link-target');
        if (!targetId) return;
        const target = activityRowIndex.get(targetId);
        if (target) {
          target.classList.add('activity-highlight');
          target.scrollIntoView({ behavior: 'smooth', block: 'center' });
          setTimeout(() => target.classList.remove('activity-highlight'), 1500);
        } else {
          toast('Transaction not in the current view.', 'info');
        }
      });
    });
  }

  const triggerActivityLoad = debounce(() => loadActivity(), 300);

  function resetActivityView(message = 'Enter a user ID to view activity.') {
    renderActivity([], { emptyMessage: message });
    if (activityStatus) activityStatus.textContent = message;
  }

  async function loadActivity() {
    if (!activityTableBody) return;
    const memberInfo = getMemberIdInfo();
    const user = (memberInfo?.normalized || '').trim();
    if (!user) {
      if (activityStatus) activityStatus.textContent = 'Enter a user ID to view activity.';
      renderActivity([], { emptyMessage: 'Enter a user ID to view activity.' });
      return;
    }
    const params = new URLSearchParams();
    const verb = (activityVerb?.value || 'all').toLowerCase();
    if (verb && verb !== 'all') params.set('verb', verb);
    params.set('userId', user);
    const actor = (activityActor?.value || '').trim();
    if (actor) params.set('actor', actor);
    if (activityFrom?.value) params.set('from', activityFrom.value);
    if (activityTo?.value) params.set('to', activityTo.value);
    params.set('limit', '100');
    params.set('offset', '0');
    const qs = params.toString() ? `?${params.toString()}` : '';
    if (activityStatus) activityStatus.textContent = 'Loading activity…';
    activityTableBody.innerHTML = '';
    try {
      const { res, body } = await adminFetch(`/api/history${qs}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        if (activityStatus) activityStatus.textContent = 'Admin key invalid.';
        renderActivity([], { emptyMessage: 'Admin key invalid.' });
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Activity load failed');
        throw new Error(msg);
      }
      const rows = Array.isArray(body?.rows) ? body.rows : [];
      renderActivity(rows);
      if (activityStatus) {
        activityStatus.textContent = rows.length
          ? `Showing ${rows.length} item${rows.length === 1 ? '' : 's'}.`
          : 'No activity matches your filters.';
      }
    } catch (err) {
      console.error(err);
      if (activityStatus) activityStatus.textContent = presentError(err.message, 'Activity unavailable.');
      renderActivity([], { emptyMessage: 'Activity unavailable.' });
    }
  }

  function formatTime(ms) {
    if (!ms) return '';
    try {
      return new Date(ms).toLocaleString();
    } catch {
      return ms;
    }
  }

  // ===== Scanner helpers =====
  function setupScanner({ buttonId, videoId, canvasId, statusId, onToken }) {
    const btn = $(buttonId);
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
        say('Camera not available');
        return;
      }
      try {
        stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' } } });
        video.srcObject = stream;
        video.style.display = 'block';
        await video.play();
        say('Point camera at QR');
        tick();
      } catch (err) {
        console.error(err);
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
          onToken(code.data).finally(() => {
            busy = false;
            say('Ready for next scan');
          });
        }
      }
      raf = requestAnimationFrame(tick);
    }

    btn.addEventListener('click', () => {
      if (stream) stop(); else start();
    });
  }

// Hold scanner — APPROVE spend token
setupScanner({
  buttonId: 'btnHoldCamera',
  videoId: 'holdVideo',
  canvasId: 'holdCanvas',
  statusId: 'holdScanStatus',
  onToken: async (raw) => {
    const parsed = parseTokenFromScan(raw);
    if (!parsed || parsed.payload.typ !== 'spend') {
      toast('Not a spend token', 'error');
      return;
    }

    const holdId = parsed.payload.data?.holdId;
    if (!holdId) {
      toast('Hold id missing', 'error');
      return;
    }

    const targetUrl = parsed.url || `${window.location.origin}/scan?t=${encodeURIComponent(parsed.token)}`;
    say('Opening approval page...');
    const opened = window.open(targetUrl, '_blank', 'noopener');
    if (!opened) {
      window.location.href = targetUrl;
    }
  },  // ← keep this comma
});   // ← and this closer

// Earn scanner — GENERATE earn/give token
setupScanner({
  buttonId: 'btnEarnCamera',
  videoId: 'earnVideo',
  canvasId: 'earnCanvas',
  statusId: 'earnScanStatus',
  onToken: async (raw) => {
    const parsed = parseTokenFromScan(raw);
    if (!parsed || !['earn', 'give'].includes(parsed.payload.typ)) {
      toast('Unsupported token', 'error');
      return;
    }
    try {
      const { res, body } = await adminFetch('/api/earn/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: parsed.token })
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Scan failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      toast(`Credited ${data.amount} to ${data.userId}`);
    } catch (err) {
      toast(err.message || 'Scan failed', 'error');
    }
  },
}); // must close the call

  // ===== Rewards =====
  function applyUrlToggle(show) {
    document.body.classList.toggle('hide-urls', !show);
  }

  function appendMediaUrl(container, label, url) {
    if (!container || !url) return;
    const row = document.createElement('div');
    row.className = 'muted mono media-url';
    row.style.flex = '1 1 100%';
    row.style.minWidth = '0';
    row.style.display = 'flex';
    row.style.alignItems = 'baseline';
    row.style.gap = '4px';
    const strong = document.createElement('strong');
    strong.textContent = `${label}:`;
    row.appendChild(strong);
    const value = document.createElement('span');
    value.textContent = url;
    value.style.flex = '1';
    value.style.minWidth = '0';
    value.style.wordBreak = 'break-all';
    row.appendChild(value);
    container.appendChild(row);
  }
  const SHOW_URLS_KEY = 'ck_show_urls';
  (function initToggle() {
    const toggle = $('adminShowUrls');
    if (!toggle) return;
    const saved = storageGet(SHOW_URLS_KEY);
    const show = saved === '1';
    toggle.checked = show;
    applyUrlToggle(show);
    toggle.addEventListener('change', () => {
      storageSet(SHOW_URLS_KEY, toggle.checked ? '1' : '0');
      applyUrlToggle(toggle.checked);
      loadRewards();
    });
  })();

  function editReward(item) {
    const nameInput = prompt('Reward name', item.name || '');
    if (nameInput === null) return;
    const name = nameInput.trim();
    if (!name) {
      toast('Reward name required', 'error');
      return;
    }

    const costPrompt = prompt('Cost (points)', Number.isFinite(item.cost) ? String(item.cost) : '');
    if (costPrompt === null) return;
    const cost = Number(costPrompt.trim());
    if (!Number.isFinite(cost) || cost < 0) {
      toast('Cost must be a non-negative number', 'error');
      return;
    }

    const imagePrompt = prompt('Image URL (optional)', item.imageUrl || '');
    if (imagePrompt === null) return;
    const imageUrl = imagePrompt.trim();

    const youtubePrompt = prompt('YouTube URL (optional)', item.youtubeUrl || '');
    if (youtubePrompt === null) return;
    const youtubeUrl = youtubePrompt.trim();

    const descPrompt = prompt('Description (optional)', item.description || '');
    if (descPrompt === null) return;
    const description = descPrompt.trim();

    const payload = { name, cost, description };
    payload.imageUrl = imageUrl || null;
    payload.youtubeUrl = youtubeUrl || null;
    updateReward(item.id, payload);
  }

  const rewardsInactiveBtn = $('btnShowInactiveRewards');
  let rewardsStatusFilter = 'active';
  let rewardsToggleInitialized = false;

  function updateRewardsToggleButton() {
    if (!rewardsInactiveBtn) return;
    const showingDisabled = rewardsStatusFilter === 'disabled';
    if (!rewardsToggleInitialized) {
      rewardsInactiveBtn.hidden = true;
      return;
    }
    rewardsInactiveBtn.hidden = false;
    rewardsInactiveBtn.disabled = false;
    rewardsInactiveBtn.setAttribute('aria-pressed', showingDisabled ? 'true' : 'false');
    rewardsInactiveBtn.classList.toggle('is-selected', showingDisabled);
    rewardsInactiveBtn.textContent = showingDisabled ? 'Show Active Rewards' : 'Show Deactivated Rewards';
  }

  async function loadRewards() {
    const list = $('rewardsList');
    if (!list) return;
    const statusEl = $('rewardsStatus');
    const filterValue = $('filterRewards')?.value?.toLowerCase?.() || '';
    list.innerHTML = '<div class="muted">Loading...</div>';
    if (statusEl) statusEl.textContent = '';
    updateRewardsToggleButton();
    try {
      const params = new URLSearchParams();
      if (rewardsStatusFilter === 'active') {
        params.set('active', '1');
      } else if (rewardsStatusFilter === 'disabled') {
        params.set('status', 'disabled');
      }
      const qs = params.toString() ? `?${params.toString()}` : '';
      const { res, body } = await adminFetch(`/api/rewards${qs}`);
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        list.innerHTML = '<div class="muted">Admin key invalid.</div>';
        rewardsToggleInitialized = false;
        updateRewardsToggleButton();
        return;
      }
      if (!res.ok){
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load rewards');
        throw new Error(msg);
      }
      const items = Array.isArray(body) ? body : [];
      list.innerHTML = '';
      const normalized = items.map(item => ({
        id: item.id,
        name: (item.name || item.title || '').trim(),
        cost: Number.isFinite(Number(item.cost)) ? Number(item.cost) : Number(item.price || 0),
        description: item.description || '',
        imageUrl: item.imageUrl || item.image_url || '',
        image_url: item.image_url || item.imageUrl || '',
        youtubeUrl: item.youtubeUrl || item.youtube_url || '',
        youtube_url: item.youtube_url || item.youtubeUrl || '',
        status: (item.status || (item.active ? 'active' : 'disabled') || 'active').toString().toLowerCase(),
        active: Number(item.active ?? (item.status === 'disabled' ? 0 : 1)) ? 1 : 0
      }));
      rewardsToggleInitialized = true;
      updateRewardsToggleButton();
      const filtered = normalized.filter(it => {
        const matchesFilter = !filterValue || it.name.toLowerCase().includes(filterValue);
        if (!matchesFilter) return false;
        if (rewardsStatusFilter === 'active') return it.status !== 'disabled';
        if (rewardsStatusFilter === 'disabled') return it.status === 'disabled';
        return true;
      });
      const showUrls = document.getElementById('adminShowUrls')?.checked;
      for (const item of filtered) {
        const card = document.createElement('div');
        card.className = 'reward-card';
        card.style.display = 'flex';
        card.style.alignItems = 'center';
        card.style.gap = '12px';
        card.style.background = '#fff';
        card.style.border = '1px solid var(--line)';
        card.style.borderRadius = '10px';
        card.style.padding = '12px';

        const thumb = document.createElement('img');
        thumb.className = 'reward-thumb';
        thumb.src = item.imageUrl || '';
        thumb.alt = '';
        thumb.loading = 'lazy';
        thumb.width = 96;
        thumb.height = 96;
        thumb.style.objectFit = 'cover';
        thumb.style.aspectRatio = '1/1';
        thumb.addEventListener('click', () => { if (thumb.src) openImageModal(thumb.src); });
        if (thumb.src){
          card.appendChild(thumb);
        } else {
          const spacer = document.createElement('div');
          spacer.style.width = '96px';
          spacer.style.height = '96px';
          spacer.style.flex = '0 0 auto';
          card.appendChild(spacer);
        }

        const youtubeThumbUrl = getYouTubeThumbnail(item.youtubeUrl);
        if (youtubeThumbUrl) {
          const ytThumb = document.createElement('img');
          ytThumb.src = youtubeThumbUrl;
          ytThumb.alt = 'YouTube preview';
          ytThumb.loading = 'lazy';
          ytThumb.width = 72;
          ytThumb.height = 54;
          ytThumb.style.objectFit = 'cover';
          ytThumb.style.aspectRatio = '4 / 3';
          ytThumb.style.borderRadius = '8px';
          ytThumb.style.flex = '0 0 auto';
          ytThumb.style.cursor = 'pointer';
          ytThumb.style.boxShadow = '0 2px 6px rgba(0,0,0,0.15)';
          ytThumb.title = 'Open YouTube video';
          ytThumb.dataset.youtube = item.youtubeUrl;
          ytThumb.addEventListener('click', () => {
            const url = ytThumb.dataset.youtube;
            if (url) {
              openVideoModal(url);
            }
          });
          ytThumb.addEventListener('error', () => ytThumb.remove());
          card.appendChild(ytThumb);
        }

        const info = document.createElement('div');
        info.style.flex = '1 1 auto';
        const title = document.createElement('div');
        title.style.fontWeight = '600';
        title.textContent = item.name || 'Reward';
        info.appendChild(title);

        const cost = document.createElement('div');
        cost.className = 'muted';
        cost.textContent = `${item.cost || 0} points`;
        info.appendChild(cost);

        if (item.description) {
          const desc = document.createElement('div');
          desc.className = 'muted';
          desc.textContent = item.description;
          info.appendChild(desc);
        }

        if (!item.active) {
          const badge = document.createElement('div');
          badge.className = 'muted';
          badge.textContent = 'Inactive';
          info.appendChild(badge);
          card.style.opacity = '0.6';
        }

        card.appendChild(info);

        if (showUrls){
          if (item.imageUrl) appendMediaUrl(card, 'Image', item.imageUrl);
          if (item.youtubeUrl) appendMediaUrl(card, 'YouTube', item.youtubeUrl);
        }

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.flexDirection = 'column';
        actions.style.gap = '6px';
        actions.style.flex = '0 0 auto';
        actions.style.marginLeft = 'auto';

        if (item.youtubeUrl) {
          const watchBtn = document.createElement('button');
          watchBtn.type = 'button';
          watchBtn.className = 'btn btn-sm';
          watchBtn.textContent = 'Watch clip';
          watchBtn.dataset.youtube = item.youtubeUrl;
          watchBtn.addEventListener('click', () => {
            const url = watchBtn.dataset.youtube;
            if (url) openVideoModal(url);
          });
          actions.appendChild(watchBtn);
        }

        const editBtn = document.createElement('button');
        editBtn.textContent = 'Edit';
        editBtn.addEventListener('click', () => editReward(item));
        actions.appendChild(editBtn);

        const isDisabled = item.status === 'disabled' || !item.active;
        if (rewardsStatusFilter === 'disabled') {
          const reactivateBtn = document.createElement('button');
          reactivateBtn.textContent = 'Reactivate';
          reactivateBtn.addEventListener('click', () => updateReward(item.id, { active: 1 }));
          actions.appendChild(reactivateBtn);

          const deleteBtn = document.createElement('button');
          deleteBtn.textContent = 'Delete permanently';
          deleteBtn.addEventListener('click', () => deleteReward(item.id));
          actions.appendChild(deleteBtn);
        } else {
          const toggleBtn = document.createElement('button');
          toggleBtn.textContent = isDisabled ? 'Activate' : 'Deactivate';
          toggleBtn.addEventListener('click', () => updateReward(item.id, { active: isDisabled ? 1 : 0 }));
          actions.appendChild(toggleBtn);
        }

        card.appendChild(actions);

        list.appendChild(card);
      }
      if (!filtered.length) {
        const emptyLabel = rewardsStatusFilter === 'disabled' ? 'No deactivated rewards.' : 'No rewards match.';
        list.innerHTML = `<div class="muted">${emptyLabel}</div>`;
      }
      if (statusEl) {
        const label = rewardsStatusFilter === 'disabled' ? 'deactivated' : 'active';
        statusEl.textContent = `Showing ${filtered.length} ${label} reward${filtered.length === 1 ? '' : 's'}.`;
      }
    } catch (err) {
      const msg = err.message || 'Failed to load rewards';
      if (statusEl) statusEl.textContent = msg;
      if (list) list.innerHTML = `<div class="muted">${msg}</div>`;
      rewardsToggleInitialized = false;
      updateRewardsToggleButton();
    }
  }
  $('btnLoadRewards')?.addEventListener('click', () => {
    rewardsStatusFilter = 'active';
    updateRewardsToggleButton();
    loadRewards();
  });
  $('filterRewards')?.addEventListener('input', loadRewards);

  rewardsInactiveBtn?.addEventListener('click', () => {
    rewardsStatusFilter = rewardsStatusFilter === 'disabled' ? 'active' : 'disabled';
    updateRewardsToggleButton();
    loadRewards();
  });

  async function updateReward(id, body) {
    try {
      const { res, body: respBody } = await adminFetch(`/api/rewards/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (respBody && respBody.error) || (typeof respBody === 'string' ? respBody : 'update failed');
        throw new Error(msg);
      }
      toast('Reward updated');
      loadRewards();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function deleteReward(id) {
    if (!confirm('Delete this reward permanently?')) return;
    try {
      const { res, body } = await adminFetch(`/api/rewards/${id}`, { method: 'DELETE' });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (res.status === 409) {
        toast('Reward is referenced by existing records.', 'error');
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Delete failed');
        throw new Error(msg);
      }
      toast('Reward deleted');
      loadRewards();
    } catch (err) {
      console.error(err);
      toast(err.message || 'Delete failed', 'error');
    }
  }

  document.getElementById('btnCreateReward')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    const nameEl = document.getElementById('rewardName');
    const costEl = document.getElementById('rewardCost');
    const imageEl = document.getElementById('rewardImage');
    const youtubeEl = document.getElementById('rewardYoutube');
    const descEl = document.getElementById('rewardDesc');

    const name = nameEl?.value?.trim() || '';
    const cost = Number(costEl?.value || NaN);
    const imageUrl = imageEl?.value?.trim() || null;
    const youtubeUrl = youtubeEl?.value?.trim() || null;
    const description = descEl?.value?.trim() || '';
    if (!name || Number.isNaN(cost)) { toast('Name and numeric cost required', 'error'); return; }

    const { res, body } = await adminFetch('/api/rewards', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ name, cost, imageUrl, youtubeUrl, description }),
    });

    if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
    if (!res.ok){ toast((typeof body === 'string' ? body : body?.error) || 'Create failed', 'error'); return; }

    toast('Reward created');
    if (nameEl) nameEl.value = '';
    if (costEl) costEl.value = '1';
    if (imageEl) imageEl.value = '';
    if (youtubeEl) youtubeEl.value = '';
    if (descEl) descEl.value = '';
    loadRewards?.(); // refresh the list if available
  });

  updateRewardsToggleButton();

  // image upload
  const drop = $('drop');
  const fileInput = $('file');
  const uploadStatus = $('uploadStatus');
  if (drop && fileInput) {
    drop.addEventListener('click', () => fileInput.click());
    drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('drag'); });
    drop.addEventListener('dragleave', () => drop.classList.remove('drag'));
    drop.addEventListener('drop', (e) => {
      e.preventDefault(); drop.classList.remove('drag');
      if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
    });
    fileInput.addEventListener('change', () => {
      if (fileInput.files[0]) handleFile(fileInput.files[0]);
    });
  }

  async function handleFile(file) {
    try {
      uploadStatus.textContent = 'Uploading...';
      const base64 = await fileToDataUrl(file);
      const { res, body } = await adminFetch('/admin/upload-image64', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image64: base64 })
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); uploadStatus.textContent = 'Admin key invalid.'; return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'upload failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      $('rewardImage').value = data.url || '';
      uploadStatus.textContent = data.url ? `Uploaded: ${data.url}` : 'Uploaded';
    } catch (err) {
      uploadStatus.textContent = 'Upload failed';
      toast(err.message || 'Upload failed', 'error');
    }
  }

  function fileToDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(reader.error || new Error('read failed'));
      reader.readAsDataURL(file);
    });
  }

  // ===== Earn templates =====
  const earnTableBody = $('earnTable')?.querySelector('tbody');
  const inactiveModal = $('inactiveTemplatesModal');
  const inactiveTableBody = $('inactiveTemplatesTable')?.querySelector('tbody');
  const inactiveEmpty = $('inactiveTemplatesEmpty');
  let earnTemplates = [];

  async function loadTemplates() {
    try {
      const { res, body } = await adminFetch('/api/earn-templates?sort=sort_order');
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok){
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'failed');
        throw new Error(msg);
      }
      const data = Array.isArray(body) ? body : [];
      earnTemplates = data;
      renderTemplates();
      populateQuickTemplates();
      renderInactiveTemplates();
    } catch (err) {
      toast(err.message || 'Load templates failed', 'error');
    }
  }
  $('btnReloadTemplates')?.addEventListener('click', loadTemplates);
  document.addEventListener('DOMContentLoaded', loadTemplates);

  function renderTemplates() {
    if (!earnTableBody) return;
    const query = $('templateSearch').value.trim().toLowerCase();
    earnTableBody.innerHTML = '';
    const rows = earnTemplates.filter(t => !query || t.title.toLowerCase().includes(query) || (t.description || '').toLowerCase().includes(query));
    for (const tpl of rows) {
      const tr = document.createElement('tr');
      if (!tpl.active) tr.classList.add('inactive');
      tr.innerHTML = `
        <td>${tpl.id}</td>
        <td>${tpl.title}</td>
        <td>${tpl.points}</td>
        <td>${tpl.description || ''}</td>
        <td>${tpl.youtube_url ? `<a class="video-link" href="${tpl.youtube_url}" target="_blank" rel="noopener" title="Open video"><span aria-hidden="true">🎬</span><span class="sr-only">Video</span></a>` : ''}</td>
        <td>${tpl.active ? 'Yes' : 'No'}</td>
        <td>${tpl.sort_order}</td>
        <td>${formatTime(tpl.updated_at * 1000)}</td>
        <td class="actions"></td>
      `;
      const videoLink = tr.querySelector('a[href]');
      if (videoLink) {
        videoLink.dataset.youtube = videoLink.href;
        videoLink.addEventListener('click', (event) => {
          event.preventDefault();
          const url = videoLink.dataset.youtube;
          if (url) openVideoModal(url);
        });
      }
      const actions = tr.querySelector('.actions');
      const editBtn = document.createElement('button');
      editBtn.textContent = 'Edit';
      editBtn.addEventListener('click', () => editTemplate(tpl));
      const toggleBtn = document.createElement('button');
      toggleBtn.textContent = tpl.active ? 'Deactivate' : 'Activate';
      toggleBtn.addEventListener('click', () => updateTemplate(tpl.id, { active: tpl.active ? 0 : 1 }));
      const delBtn = document.createElement('button');
      delBtn.textContent = 'Delete';
      delBtn.addEventListener('click', () => deleteTemplate(tpl.id));
      actions.append(editBtn, toggleBtn, delBtn);
      earnTableBody.appendChild(tr);
    }
  }
  $('templateSearch')?.addEventListener('input', renderTemplates);

  function renderInactiveTemplates() {
    if (!inactiveTableBody) return;
    const rows = earnTemplates.filter(t => !t.active);
    inactiveTableBody.innerHTML = '';
    if (inactiveEmpty) inactiveEmpty.hidden = rows.length !== 0;
    if (!rows.length) return;
    for (const tpl of rows) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${tpl.id}</td>
        <td>${tpl.title}</td>
        <td>${tpl.points}</td>
        <td>${tpl.description || ''}</td>
        <td>${tpl.sort_order}</td>
        <td>${formatTime(tpl.updated_at * 1000)}</td>
        <td class="actions"></td>
      `;
      const videoLink = tr.querySelector('a[href]');
      if (videoLink) {
        videoLink.dataset.youtube = videoLink.href;
        videoLink.addEventListener('click', (event) => {
          event.preventDefault();
          const url = videoLink.dataset.youtube;
          if (url) openVideoModal(url);
        });
      }
      const actions = tr.querySelector('.actions');
      if (actions) {
        const reactivateBtn = document.createElement('button');
        reactivateBtn.textContent = 'Reactivate';
        reactivateBtn.addEventListener('click', async () => {
          await updateTemplate(tpl.id, { active: 1 });
          renderInactiveTemplates();
        });
        actions.appendChild(reactivateBtn);
      }
      inactiveTableBody.appendChild(tr);
    }
  }

  function openInactiveTemplatesModal() {
    if (!inactiveModal) return;
    renderInactiveTemplates();
    inactiveModal.classList.add('open');
    inactiveModal.setAttribute('aria-hidden', 'false');
  }

  function closeInactiveTemplatesModal() {
    if (!inactiveModal) return;
    inactiveModal.classList.remove('open');
    inactiveModal.setAttribute('aria-hidden', 'true');
  }

  $('btnShowInactiveTemplates')?.addEventListener('click', openInactiveTemplatesModal);
  $('btnInactiveTemplatesClose')?.addEventListener('click', closeInactiveTemplatesModal);
  inactiveModal?.addEventListener('click', (event) => {
    if (event.target === inactiveModal) closeInactiveTemplatesModal();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && inactiveModal?.classList.contains('open')) {
      closeInactiveTemplatesModal();
    }
  });

  async function addTemplate() {
    const title = prompt('Template title');
    if (!title) return;
    const points = Number(prompt('Points value')); if (!Number.isFinite(points) || points <= 0) return toast('Invalid points', 'error');
    const description = prompt('Description (optional)') || '';
    const youtube_url = prompt('YouTube URL (optional)') || null;
    const sort_order = Number(prompt('Sort order (optional)', '0')) || 0;
    try {
      const { res, body } = await adminFetch('/api/earn-templates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, points, description, youtube_url, sort_order })
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'create failed');
        throw new Error(msg);
      }
      toast('Template added');
      loadTemplates();
    } catch (err) {
      toast(err.message || 'Create failed', 'error');
    }
  }
  $('btnAddTemplate')?.addEventListener('click', addTemplate);

  async function editTemplate(tpl) {
    const title = prompt('Title', tpl.title);
    if (!title) return;
    const points = Number(prompt('Points', tpl.points));
    if (!Number.isFinite(points) || points <= 0) return toast('Invalid points', 'error');
    const description = prompt('Description', tpl.description || '') || '';
    const youtube_url = prompt('YouTube URL', tpl.youtube_url || '') || null;
    const sort_order = Number(prompt('Sort order', tpl.sort_order));
    try {
      const { res, body } = await adminFetch(`/api/earn-templates/${tpl.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, points, description, youtube_url, sort_order })
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'update failed');
        throw new Error(msg);
      }
      toast('Template updated');
      loadTemplates();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function updateTemplate(id, body) {
    try {
      const { res, body: respBody } = await adminFetch(`/api/earn-templates/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (respBody && respBody.error) || (typeof respBody === 'string' ? respBody : 'update failed');
        throw new Error(msg);
      }
      toast('Template saved');
      await loadTemplates();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function deleteTemplate(id) {
    if (!confirm('Delete this template?')) return;
    try {
      const { res, body } = await adminFetch(`/api/earn-templates/${id}`, { method: 'DELETE' });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'delete failed');
        throw new Error(msg);
      }
      toast('Template deleted');
      loadTemplates();
    } catch (err) {
      toast(err.message || 'Delete failed', 'error');
    }
  }

  function populateQuickTemplates() {
    const select = $('quickTemplate');
    if (!select) return;
    select.innerHTML = '<option value="">Select template</option>';
    for (const tpl of earnTemplates.filter(t => t.active)) {
      const opt = document.createElement('option');
      opt.value = tpl.id;
      opt.textContent = `${tpl.title} (+${tpl.points})`;
      select.appendChild(opt);
    }
  }

  $('btnQuickAward')?.addEventListener('click', async () => {
    const templateId = $('quickTemplate').value;
    const userId = $('quickUser').value.trim();
    if (!templateId || !userId) return toast('Select template and user', 'error');
    try {
      const { res, body } = await adminFetch('/api/earn/quick', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ templateId, userId })
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Quick award failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      const amount = data.amount ?? '??';
      const user = data.userId || userId;
      toast(`Awarded ${amount} to ${user}`);
      $('quickUser').value = '';
      applyStateHints(data.hints || latestHints);
      const info = normalizeMemberInput();
      if (info?.normalized && info.normalized === String(user).toLowerCase()) {
        await refreshMemberLedger(info.normalized, { showPanels: true });
      }
    } catch (err) {
      toast(presentError(err.message, 'Quick award failed'), 'error');
    }
  });

  // ===== History modal =====
  const historyModal = $('historyModal');
  const historyTable = $('historyTable')?.querySelector('tbody');
  function openHistory(preset = {}) {
    if (!historyModal) return;
    historyModal.style.display = 'flex';
    if (preset.type) $('historyType').value = preset.type;
    if (preset.userId) $('historyUser').value = preset.userId;
    if (preset.source) $('historySource').value = preset.source;
    loadHistory();
  }
  function closeHistory() {
    if (historyModal) historyModal.style.display = 'none';
  }
  $('btnHistoryClose')?.addEventListener('click', closeHistory);
  $('btnHistoryRefresh')?.addEventListener('click', loadHistory);
  $('btnHistoryCsv')?.addEventListener('click', () => {
    const params = buildHistoryParams();
    const qs = new URLSearchParams({ ...params, format: 'csv' }).toString();
    window.open(`/api/history?${qs}`, '_blank');
  });

  function buildHistoryParams() {
    const type = $('historyType').value;
    const source = $('historySource').value;
    const userId = $('historyUser').value.trim();
    const fromDate = $('historyFrom').value;
    const toDate = $('historyTo').value;
    const params = { limit: '50' };
    if (type !== 'all') params.type = type;
    if (source !== 'all') params.source = source;
    if (userId) params.userId = userId;
    if (fromDate) params.from = fromDate;
    if (toDate) params.to = toDate;
    return params;
  }

  async function loadHistory() {
    if (!historyTable) return;
    historyTable.innerHTML = '<tr><td colspan="17" class="muted">Loading...</td></tr>';
    try {
      const params = buildHistoryParams();
      const qs = new URLSearchParams(params).toString();
      const { res, body } = await adminFetch(`/api/history?${qs}`);
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        historyTable.innerHTML = '<tr><td colspan="17" class="muted">Admin key invalid.</td></tr>';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'history failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      historyTable.innerHTML = '';
      for (const row of data.rows || []) {
        const tr = document.createElement('tr');
        const values = [
          formatTime(row.at),
          row.userId || '',
          row.verb || '',
          row.action || '',
          row.delta ?? '',
          row.balance_after ?? '',
          row.note || '',
          row.notes || '',
          row.templates ? JSON.stringify(row.templates) : '',
          row.itemId || '',
          row.holdId || '',
          row.parent_tx_id || '',
          row.finalCost ?? '',
          row.refund_reason || '',
          row.refund_notes || '',
          row.actor || '',
          row.idempotency_key || ''
        ];
        for (const value of values) {
          const td = document.createElement('td');
          td.textContent = value === undefined || value === null ? '' : String(value);
          tr.appendChild(td);
        }
        historyTable.appendChild(tr);
      }
      if (!historyTable.children.length) {
        historyTable.innerHTML = '<tr><td colspan="17" class="muted">No history</td></tr>';
      }
    } catch (err) {
      historyTable.innerHTML = `<tr><td colspan="17" class="muted">${err.message || 'Failed'}</td></tr>`;
    }
  }

  document.querySelectorAll('.view-history').forEach(btn => {
    btn.addEventListener('click', () => {
      const preset = {};
      const type = btn.dataset.historyType;
      if (type && type !== 'all') {
        preset.type = type;
      }
      const scope = btn.dataset.historyScope;
      if (scope === 'member') {
        const userId = requireMemberId();
        if (!userId) return;
        preset.userId = userId;
      } else if (btn.dataset.historyUser) {
        preset.userId = btn.dataset.historyUser;
      }
      openHistory(preset);
    });
  });

  if (btnActivityRefresh) btnActivityRefresh.addEventListener('click', () => loadActivity());
  if (activityVerb) activityVerb.addEventListener('change', () => loadActivity());
  if (activityActor) activityActor.addEventListener('input', triggerActivityLoad);
  [activityFrom, activityTo].forEach(el => {
    if (el) el.addEventListener('change', () => loadActivity());
  });
  if (activityTableBody) {
    renderActivity([], { emptyMessage: 'Enter a user ID to view activity.' });
    if (activityStatus) activityStatus.textContent = 'Enter a user ID to view activity.';
  }

  loadFeatureFlagsFromServer();
}

console.info('admin.js loaded ok');
