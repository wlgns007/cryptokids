(() => {
  const $ = (id) => document.getElementById(id);
  const LS_FILTER = 'ck_child_filters';

  function getUserId() {
    return $('childUserId').value.trim();
  }

  function saveFilters(filters) {
    localStorage.setItem(LS_FILTER, JSON.stringify(filters));
  }
  function loadFilters() {
    try {
      return JSON.parse(localStorage.getItem(LS_FILTER) || '{}');
    } catch { return {}; }
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
      renderHistory(data.rows || [], filters);
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
        ${tpl.youtube_url ? `<a class="video" target="_blank" href="${tpl.youtube_url}">Watch video</a>` : ''}
        <div style="display:flex; align-items:center; gap:8px;">
          <input type="checkbox" data-id="${tpl.id}" data-points="${tpl.points}">
          <span class="muted">Include</span>
        </div>
      `;
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

      const btn = document.createElement('button');
      btn.textContent = 'Redeem';
      btn.style.marginLeft = 'auto';
      btn.style.flex = '0 0 auto';
      btn.addEventListener('click', () => createHold(item));
      card.appendChild(btn);

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
