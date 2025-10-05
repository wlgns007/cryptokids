(function () {
  if (window.__CK_ADMIN_READY__) return;
  window.__CK_ADMIN_READY__ = true;

  const $ = (id) => document.getElementById(id);
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

  const ADMIN_KEY_STORAGE = 'ck_admin_key';
  function loadAdminKey() {
    return localStorage.getItem(ADMIN_KEY_STORAGE) || '';
  }
  function saveAdminKey(value) {
    localStorage.setItem(ADMIN_KEY_STORAGE, value || '');
  }
  function getAdminKey() {
    return $('adminKey')?.value?.trim() || loadAdminKey();
  }

  $('saveAdminKey')?.addEventListener('click', () => {
    const value = $('adminKey').value.trim();
    saveAdminKey(value);
    toast('Admin key saved');
  });

  document.addEventListener('DOMContentLoaded', () => {
    const saved = loadAdminKey();
    if (saved && $('adminKey')) $('adminKey').value = saved;
  });

  function adminFetch(path, init = {}) {
    const headers = new Headers(init.headers || {});
    const key = getAdminKey();
    if (key) headers.set('x-admin-key', key);
    return fetch(path, { ...init, headers });
  }

  function renderQr(elId, text) {
    const el = $(elId);
    if (!el) return;
    el.innerHTML = '';
    if (!text) return;
    new QRCode(el, { text, width: 200, height: 200 });
  }

  function parseTokenFromScan(data) {
    try {
      if (!data) return null;
      let token = data.trim();
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
    } catch (e) {
      console.error('parse token failed', e);
      return null;
    }
  }

  async function generateIssueQr() {
    const userId = $('issueUserId').value.trim();
    const amount = Number($('issueAmount').value);
    const note = $('issueNote').value.trim();
    if (!userId || !Number.isFinite(amount) || amount <= 0) {
      toast('Enter user and positive amount', 'error');
      return;
    }
    $('issueStatus').textContent = 'Generating QR...';
    try {
      const res = await adminFetch('/api/tokens/give', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, amount, note: note || undefined })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'request failed');
      renderQr('qrIssue', data.qrText);
      $('issueLink').value = data.qrText || '';
      $('issueStatus').textContent = `QR expires in 2 minutes. Amount ${data.amount} points.`;
      toast('QR ready');
    } catch (err) {
      console.error(err);
      $('issueStatus').textContent = 'Failed to generate QR.';
      toast(err.message || 'Failed', 'error');
    }
  }
  $('btnIssueGenerate')?.addEventListener('click', generateIssueQr);

  $('btnIssueBalance')?.addEventListener('click', async () => {
    const userId = $('issueUserId').value.trim();
    if (!userId) return toast('Enter user id', 'error');
    try {
      const res = await fetch(`/balance/${encodeURIComponent(userId)}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed');
      $('issueStatus').textContent = `Balance: ${data.balance} points`;
    } catch (err) {
      toast(err.message || 'Balance failed', 'error');
    }
  });

  $('btnIssueCopy')?.addEventListener('click', () => {
    const text = $('issueLink').value;
    if (!text) return toast('Nothing to copy', 'error');
    navigator.clipboard?.writeText(text).then(() => toast('Link copied')).catch(() => toast('Copy failed', 'error'));
  });

  // ===== Holds =====
  const holdsTable = $('holdsTable')?.querySelector('tbody');
  async function loadHolds() {
    if (!holdsTable) return;
    const status = $('holdFilter').value;
    $('holdsStatus').textContent = 'Loading...';
    holdsTable.innerHTML = '';
    try {
      const res = await adminFetch(`/api/holds?status=${encodeURIComponent(status)}`);
      const rows = await res.json();
      if (!res.ok) throw new Error(rows.error || 'failed');
      if (!rows.length) {
        holdsTable.innerHTML = '<tr><td colspan="6" class="muted">No holds</td></tr>';
        $('holdsStatus').textContent = '';
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
      $('holdsStatus').textContent = '';
    } catch (err) {
      console.error(err);
      $('holdsStatus').textContent = err.message || 'Failed to load holds';
    }
  }
  $('btnReloadHolds')?.addEventListener('click', loadHolds);
  $('holdFilter')?.addEventListener('change', loadHolds);
  document.addEventListener('DOMContentLoaded', loadHolds);

  async function cancelHold(id) {
    if (!confirm('Cancel this hold?')) return;
    try {
      const res = await adminFetch(`/api/holds/${id}/cancel`, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'failed');
      }
      toast('Hold canceled');
      loadHolds();
    } catch (err) {
      toast(err.message || 'Cancel failed', 'error');
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

    const override = $('holdOverride').value;
    try {
      const res = await adminFetch(`/api/holds/${holdId}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: parsed.token,
          finalCost: override ? Number(override) : undefined
        })
      });

      const isJson = res.headers.get('content-type')?.includes('application/json');
      const data = isJson ? await res.json() : { error: await res.text() };
      if (!res.ok) throw new Error(data.error || 'Approve failed');

      toast(`Redeemed ${data.finalCost ?? '??'} points`);
      $('holdOverride').value = '';
      loadHolds();
    } catch (err) {
      toast(err.message || 'Redeem failed', 'error');
    }
  },   // ← IMPORTANT: comma ends the onToken property
});     // ← IMPORTANT: closes setupScanner(...) call


      const isJson = res.headers.get('content-type')?.includes('application/json');
      const data = isJson ? await res.json() : { error: await res.text() };
      if (!res.ok) throw new Error(data.error || 'Approve failed');

      toast(`Redeemed ${data.finalCost ?? '??'} points`);
      $('holdOverride').value = '';
      loadHolds();
    } catch (err) {
      toast(err.message || 'Redeem failed', 'error');
    }
  },
}); // <-- important: comma after onToken AND this call closer


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
        const res = await adminFetch('/api/earn/scan', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token: parsed.token })
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Scan failed');
        toast(`Credited ${data.amount} to ${data.userId}`);
      } catch (err) {
        toast(err.message || 'Scan failed', 'error');
      }
    }

  // ===== Rewards =====
  function applyUrlToggle(show) {
    document.body.classList.toggle('hide-urls', !show);
  }
  const SHOW_URLS_KEY = 'ck_show_urls';
  (function initToggle() {
    const toggle = $('toggleUrls');
    if (!toggle) return;
    const saved = localStorage.getItem(SHOW_URLS_KEY);
    const show = saved === '1';
    toggle.checked = show;
    applyUrlToggle(show);
    toggle.addEventListener('change', () => {
      localStorage.setItem(SHOW_URLS_KEY, toggle.checked ? '1' : '0');
      applyUrlToggle(toggle.checked);
    });
  })();

  async function loadRewards() {
    const list = $('rewardsList');
    const filter = $('filterRewards').value.toLowerCase();
    list.innerHTML = '<div class="muted">Loading...</div>';
    try {
      const res = await fetch('/api/rewards');
      const items = await res.json();
      if (!res.ok) throw new Error(items.error || 'failed');
      list.innerHTML = '';
      const filtered = items.filter(it => !filter || it.title.toLowerCase().includes(filter));
      for (const r of filtered) {
        const card = document.createElement('div');
        card.style.background = '#fff';
        card.style.border = '1px solid var(--line)';
        card.style.borderRadius = '10px';
        card.style.padding = '12px';
        card.style.display = 'grid';
        card.style.gridTemplateColumns = '80px 1fr auto';
        card.style.gap = '12px';
        card.style.alignItems = 'center';

        const img = document.createElement('img');
        img.src = r.imageUrl || '';
        img.alt = '';
        img.style.width = '80px';
        img.style.height = '80px';
        img.style.objectFit = 'cover';
        img.style.borderRadius = '10px';
        img.onerror = () => img.remove();
        if (r.imageUrl) card.appendChild(img); else card.appendChild(document.createElement('div'));

        const meta = document.createElement('div');
        meta.innerHTML = `<div style="font-weight:600;">${r.title}</div><div class="muted">${r.price} points</div>`;
        if (r.description) {
          const desc = document.createElement('div');
          desc.className = 'muted';
          desc.textContent = r.description;
          meta.appendChild(desc);
        }
        if (r.imageUrl) {
          const url = document.createElement('div');
          url.className = 'muted url';
          url.textContent = r.imageUrl;
          meta.appendChild(url);
        }
        card.appendChild(meta);

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.flexDirection = 'column';
        actions.style.gap = '6px';
        const disableBtn = document.createElement('button');
        disableBtn.textContent = 'Deactivate';
        disableBtn.addEventListener('click', () => updateReward(r.id, { active: 0 }));
        actions.appendChild(disableBtn);
        card.appendChild(actions);

        list.appendChild(card);
      }
      if (!filtered.length) list.innerHTML = '<div class="muted">No rewards match.</div>';
    } catch (err) {
      $('rewardsStatus').textContent = err.message || 'Failed to load rewards';
    }
  }
  $('btnLoadRewards')?.addEventListener('click', loadRewards);
  $('filterRewards')?.addEventListener('input', loadRewards);

  async function updateReward(id, body) {
    try {
      const res = await adminFetch(`/api/rewards/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'update failed');
      }
      toast('Reward updated');
      loadRewards();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function createReward() {
    const name = $('rewardName').value.trim();
    const price = Number($('rewardCost').value);
    const imageUrl = $('rewardImage').value.trim();
    const description = $('rewardDescription').value.trim();
    if (!name || !Number.isFinite(price) || price <= 0) {
      toast('Enter name and positive price', 'error');
      return;
    }
    try {
      const res = await adminFetch('/api/rewards', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, price, imageUrl, description })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'create failed');
      toast('Reward added');
      $('rewardName').value = '';
      $('rewardCost').value = '';
      $('rewardImage').value = '';
      $('rewardDescription').value = '';
      loadRewards();
    } catch (err) {
      toast(err.message || 'Create failed', 'error');
    }
  }
  $('btnCreateReward')?.addEventListener('click', createReward);

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
      const res = await adminFetch('/admin/upload-image64', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image64: base64 })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'upload failed');
      $('rewardImage').value = data.url;
      uploadStatus.textContent = `Uploaded: ${data.url}`;
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
  let earnTemplates = [];

  async function loadTemplates() {
    try {
      const res = await fetch('/api/earn-templates?sort=sort_order');
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'failed');
      earnTemplates = data;
      renderTemplates();
      populateQuickTemplates();
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
        <td>${tpl.youtube_url ? `<a href="${tpl.youtube_url}" target="_blank">Video</a>` : ''}</td>
        <td>${tpl.active ? 'Yes' : 'No'}</td>
        <td>${tpl.sort_order}</td>
        <td>${formatTime(tpl.updated_at * 1000)}</td>
        <td class="actions"></td>
      `;
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

  async function addTemplate() {
    const title = prompt('Template title');
    if (!title) return;
    const points = Number(prompt('Points value')); if (!Number.isFinite(points) || points <= 0) return toast('Invalid points', 'error');
    const description = prompt('Description (optional)') || '';
    const youtube_url = prompt('YouTube URL (optional)') || null;
    const sort_order = Number(prompt('Sort order (optional)', '0')) || 0;
    try {
      const res = await adminFetch('/api/earn-templates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, points, description, youtube_url, sort_order })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'create failed');
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
      const res = await adminFetch(`/api/earn-templates/${tpl.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, points, description, youtube_url, sort_order })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'update failed');
      toast('Template updated');
      loadTemplates();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function updateTemplate(id, body) {
    try {
      const res = await adminFetch(`/api/earn-templates/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'update failed');
      toast('Template saved');
      loadTemplates();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function deleteTemplate(id) {
    if (!confirm('Delete this template?')) return;
    try {
      const res = await adminFetch(`/api/earn-templates/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'delete failed');
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
      const res = await adminFetch('/api/earn/quick', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ templateId, userId })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'quick failed');
      toast(`Awarded ${data.amount} to ${data.userId}`);
      $('quickUser').value = '';
    } catch (err) {
      toast(err.message || 'Quick award failed', 'error');
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
    historyTable.innerHTML = '<tr><td colspan="11" class="muted">Loading...</td></tr>';
    try {
      const params = buildHistoryParams();
      const qs = new URLSearchParams(params).toString();
      const res = await adminFetch(`/api/history?${qs}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'history failed');
      historyTable.innerHTML = '';
      for (const row of data.rows || []) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${formatTime(row.at)}</td>
          <td>${row.userId}</td>
          <td>${row.action}</td>
          <td>${row.delta}</td>
          <td>${row.balance_after}</td>
          <td>${row.note || ''}</td>
          <td>${row.templates ? JSON.stringify(row.templates) : ''}</td>
          <td>${row.itemId || ''}</td>
          <td>${row.holdId || ''}</td>
          <td>${row.finalCost ?? ''}</td>
          <td>${row.actor || ''}</td>
        `;
        historyTable.appendChild(tr);
      }
      if (!historyTable.children.length) {
        historyTable.innerHTML = '<tr><td colspan="11" class="muted">No history</td></tr>';
      }
    } catch (err) {
      historyTable.innerHTML = `<tr><td colspan="11" class="muted">${err.message || 'Failed'}</td></tr>`;
    }
  }

  document.querySelectorAll('.view-history').forEach(btn => {
    btn.addEventListener('click', () => {
      const type = btn.dataset.historyType;
      if (type === 'spend') openHistory({ type: 'spend' });
      else openHistory({ type: 'earn' });
    });
  });

})();
  console.info('admin.js loaded ok');
