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

window.getYouTubeId = getYouTubeId;
window.getYouTubeThumbnail = getYouTubeThumbnail;
window.getYouTubeEmbed = getYouTubeEmbed;

(() => {
  const $ = (id) => document.getElementById(id);
  const LS_FILTER = 'ck_child_filters';
  const RECENT_REDEEM_LIMIT = 50;
  const RECENT_REDEEM_DISPLAY = 5;
  const FULL_REDEEM_LIMIT = 200;
  let lastRedeemEntry = null;
  let recentRedeemsVisible = false;
  let fullRedeemsVisible = false;

  const memoryStore = {};

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

  (function setupVideoModal() {
    const modal = document.getElementById("videoModal");
    const frame = document.getElementById("videoFrame");
    if (!modal || !frame) return;

    window.openVideoModal = function (url) {
      const embedUrl = getYouTubeEmbed(url);
      if (!embedUrl) return window.open(url, '_blank', 'noopener');

      modal.hidden = false;
      frame.src = embedUrl;
    };

    window.closeVideoModal = function () {
      frame.src = '';
      modal.hidden = true;
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
      if (!modal.hidden && e.key === "Escape") closeVideoModal();
    });
  })();

  function getUserId() {
    return $('childUserId').value.trim();
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
      updateRedeemNotice(null, { fallbackText: 'Enter your user ID to see recent redeemed rewards.' });
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
      alert('Enter user id');
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
      alert('Enter user id');
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
    if (!userId) { alert('Enter user id'); return; }
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
    if (!userId) { alert('Enter user id'); return; }
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
    if (!userId) { alert('Enter user id'); return; }
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
  async function loadEarnTemplates() {
    try {
      const res = await fetch('/api/earn-templates?active=true&sort=sort_order');
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'failed');
      templates = data;
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
      if (videoSlot && tpl.youtube_url) {
        const watchBtn = document.createElement('button');
        watchBtn.type = 'button';
        watchBtn.className = 'btn btn-sm';
        watchBtn.textContent = 'Watch clip';
        watchBtn.addEventListener('click', (event) => {
          event.preventDefault();
          event.stopPropagation();
          openVideoModal(tpl.youtube_url);
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
    if (!userId) { alert('Enter user id'); return; }
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

  function renderQr(elId, text) {
    const el = $(elId);
    if (!el) return;
    el.innerHTML = '';
    if (!text) return;
    new QRCode(el, { text, width: 220, height: 220 });
  }

  loadEarnTemplates();

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

  function setQR(text = '', url = '') {
    const msg = $('shopMsg');
    if (msg) msg.textContent = text || '';
    const box = $('shopQrBox');
    if (!box) return;
    box.innerHTML = '';
    if (url) {
      const img = new Image();
      img.src = url;
      img.alt = 'Reward QR code';
      img.loading = 'lazy';
      img.style.maxWidth = '220px';
      img.style.maxHeight = '220px';
      img.style.background = '#fff';
      img.style.padding = '8px';
      img.style.borderRadius = '12px';
      box.appendChild(img);
    }
  }

  function openImageModal(src){
    const m = document.createElement('div');
    Object.assign(m.style,{position:'fixed',inset:0,background:'rgba(0,0,0,.7)',display:'grid',placeItems:'center',zIndex:9999});
    m.addEventListener('click',()=>m.remove());
    const big = new Image(); big.src = src; big.style.maxWidth='90vw'; big.style.maxHeight='90vh'; big.style.boxShadow='0 8px 24px rgba(0,0,0,.5)';
    m.appendChild(big); document.body.appendChild(m);
  }

  document.getElementById('btnLoadItems')?.addEventListener('click', loadRewards);
  $('btnRecentRedeems')?.addEventListener('click', toggleRecentRedeems);
  $('btnFullRedeems')?.addEventListener('click', toggleFullRedeems);
  updateRecentButton();
  updateFullButton();

  async function loadRewards(){
    const list = $('shopList');
    if (list) list.innerHTML = '<div class="muted">Loading...</div>';
    const empty = $('shopEmpty');
    if (empty) empty.style.display = 'none';
    try{
      const res = await fetch('/api/rewards?active=1');
      const data = await res.json();
      if (!res.ok) throw new Error(data?.error || 'Failed to load rewards');
      renderRewards(data || []);
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
      youtube_url: item.youtube_url || item.youtubeUrl || '',
      youtubeUrl: item.youtubeUrl || item.youtube_url || '',
    }));
    if (!normalized.length){
      $('shopEmpty').style.display = 'block';
      $('shopMsg').textContent = '';
      setQR('');
      return;
    }
    $('shopEmpty').style.display = 'none';
    $('shopMsg').textContent = getUserId() ? 'Tap Redeem to request a reward.' : 'Enter your user ID, then tap Redeem.';
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
        ytThumb.addEventListener('click', () => openVideoModal(youtubeUrl));
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
        watchBtn.addEventListener('click', () => openVideoModal(youtubeUrl));
        actions.appendChild(watchBtn);
      }

      const btn = document.createElement('button');
      btn.textContent = 'Redeem';
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
    if (!userId) { alert('Enter user id'); return; }
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
