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

  // ===== Shop =====
  $('btnLoadShop')?.addEventListener('click', loadShop);
  async function loadShop() {
    const userId = getUserId();
    if (!userId) { alert('Enter user id'); return; }
    $('shopMsg').textContent = 'Loading...';
    $('shopList').innerHTML = '';
    $('shopEmpty').style.display = 'none';
    $('shopQrBox').innerHTML = '';
    try {
      const [balanceRes, rewardsRes] = await Promise.all([
        fetch(`/summary/${encodeURIComponent(userId)}`),
        fetch('/api/rewards')
      ]);
      const balanceData = await balanceRes.json();
      const rewards = await rewardsRes.json();
      if (!balanceRes.ok) throw new Error(balanceData.error || 'balance failed');
      if (!rewardsRes.ok) throw new Error(rewards.error || 'rewards failed');
      $('shopMsg').textContent = `Balance: ${balanceData.balance} points`;
      renderShop(rewards, balanceData.balance);
    } catch (err) {
      $('shopMsg').textContent = err.message || 'Failed to load shop';
    }
  }

  function renderShop(items, balance) {
    const list = $('shopList');
    list.innerHTML = '';
    if (!items.length) {
      $('shopEmpty').style.display = 'block';
      return;
    }
    items.forEach((item, index) => {
      const canAfford = balance >= item.price;
      const row = document.createElement('div');
      row.className = 'shop-item';
      if (item.imageUrl) {
        const img = document.createElement('img');
        img.className = 'reward-thumb';
        img.src = item.imageUrl;
        img.alt = '';
        img.loading = 'lazy';
        img.width = 96; img.height = 96;
        img.style.objectFit = 'cover';
        img.style.aspectRatio = '1 / 1';
        img.onerror = () => img.remove();
        row.appendChild(img);
      } else {
        const placeholder = document.createElement('div');
        placeholder.style.width = '96px';
        placeholder.style.height = '96px';
        row.appendChild(placeholder);
      }

      const info = document.createElement('div');
      const title = document.createElement('div');
      title.className = 'price';
      title.textContent = `${index + 1}. ${item.title}`;
      info.appendChild(title);

      const price = document.createElement('div');
      price.className = 'muted';
      price.textContent = `${item.price} points`;
      info.appendChild(price);

      const description = document.createElement('div');
      description.className = 'muted';
      description.textContent = item.description || '';
      info.appendChild(description);
      row.appendChild(info);

      const btn = document.createElement('button');
      btn.textContent = canAfford ? 'Redeem' : 'Not enough';
      btn.disabled = !canAfford;
      if (canAfford) btn.addEventListener('click', () => createHold(item));
      row.appendChild(btn);
      list.appendChild(row);
    });
  }

  async function createHold(item) {
    const userId = getUserId();
    if (!userId) { alert('Enter user id'); return; }
    $('shopMsg').textContent = 'Creating hold...';
    try {
      const res = await fetch('/api/holds', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, itemId: item.id })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'hold failed');
      $('shopMsg').textContent = `Show this QR to an adult to pick up ${item.title}.`;
      renderQr('shopQrBox', data.qrText);
      checkBalance();
      loadHistory();
    } catch (err) {
      $('shopMsg').textContent = err.message || 'Failed to create hold';
    }
  }

})();
