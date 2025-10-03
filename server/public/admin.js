// admin.js — CryptoKids Admin (public rewards API version)

if (window.__CK_ADMIN_INIT__) {
  console.warn('[admin] already initialized — skipping rebinds');
} else {
  window.__CK_ADMIN_INIT__ = true;
  (function () {
    // ---------- helpers ----------
    const $ = (id) => document.getElementById(id);
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const escapeHTML = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));
function getAdminKey() {
  // Prefer saved key, fallback to input box if present
  const k = localStorage.getItem('adminKey') || ($('adminKey')?.value || '').trim();
  return k || '';
}

function formatDate(ts) {
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}
// ADD: attach a drop/click uploader that writes the returned URL into target input
function attachInlineUploader(dropEl, fileEl, statusEl, targetInputEl) {
  if (!dropEl || !fileEl || !targetInputEl) return;

  const setBusy = (b, t='') => {
    dropEl.classList.toggle('drag', b);
    if (statusEl) statusEl.textContent = t;
  };
  const pick = () => fileEl.click();

  dropEl.addEventListener('click', pick);
  ['dragenter','dragover'].forEach(ev => dropEl.addEventListener(ev, e => { e.preventDefault(); setBusy(true, 'Drop to upload'); }));
  ['dragleave','drop'].forEach(ev => dropEl.addEventListener(ev, e => { e.preventDefault(); setBusy(false); }));
  dropEl.addEventListener('drop', (e) => handleFiles(e.dataTransfer.files));
  fileEl.addEventListener('change', (e) => handleFiles(e.target.files));

  async function handleFiles(list) {
    const f = list && list[0]; if (!f) return;
    try {
      setBusy(true, 'Uploading…');
      const url = await api.uploadFile(f);
      if (!url) throw new Error('No URL returned');
      targetInputEl.value = url;
      if (statusEl) statusEl.textContent = `Uploaded ✓ ${url}`;
    } catch (e) {
      console.error(e);
      if (statusEl) statusEl.textContent = 'Upload failed.';
      toast('Failed to upload image', 'error', 3000);
    } finally {
      setBusy(false);
    }
  }
}

// --- Toasts ---
function toast(msg, type='success', ms=2200) {
  const host = document.getElementById('toastHost');
  if (!host) return alert(msg);
  const t = document.createElement('div');
  t.className = `toast ${type}`; t.textContent = msg;
  host.appendChild(t);
  requestAnimationFrame(()=> t.classList.add('show'));
  setTimeout(()=>{ t.classList.remove('show'); setTimeout(()=>t.remove(),200); }, ms);
}

// --- Show URLs toggle persistence (default OFF) ---
const SHOW_URLS_KEY = 'ck_showUrls'; // '1' show, '0' hide (default)

function applyUrlToggle(show) {
  document.body.classList.toggle('hide-urls', !show);
}

function initUrlToggle() {
  const el = document.getElementById('toggleUrls');
  if (!el) return; // older HTML
  const saved = localStorage.getItem(SHOW_URLS_KEY);
  const show = saved === '1' ? true : false; // default OFF
  el.checked = show;
  applyUrlToggle(show);
  el.addEventListener('change', () => {
    const next = el.checked ? '1' : '0';
    localStorage.setItem(SHOW_URLS_KEY, next);
    applyUrlToggle(el.checked);
  });
}
document.addEventListener('DOMContentLoaded', initUrlToggle);

    // Persisted admin key (used by legacy issue endpoints if needed)
    const LS_KEY = 'ck_admin_key';
    function loadKey() { return localStorage.getItem(LS_KEY) || ''; }
    function saveKey(v) { localStorage.setItem(LS_KEY, v || ''); }

    // Admin fetch (for /earn /spend /balance which may require header)
    async function adminFetch(path, init = {}) {
      const headers = new Headers(init.headers || {});
      const k = $('adminKey')?.value || loadKey();
      if (k) headers.set('x-admin-key', k);
      const res = await fetch(path, { ...init, headers });
      if (!res.ok) {
        const t = await res.text().catch(()=>'');
        throw new Error(`${res.status} ${res.statusText} — ${t || 'request failed'}`);
      }
      const ct = res.headers.get('content-type') || '';
      return ct.includes('application/json') ? res.json() : res.text();
    }

// Convert a File to data: URL (base64) for /admin/upload-image64
function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const rd = new FileReader();
    rd.onload = () => resolve(rd.result);           // e.g., "data:image/png;base64,iVBORw0K..."
    rd.onerror = () => reject(rd.error || new Error('Read failed'));
    rd.readAsDataURL(file);
  });
}

// simple per-session upload cache: sha256 -> url
const __uploadCache = new Map();

async function hashFileSHA256(file) {
  const buf = await file.arrayBuffer();
  const hash = await crypto.subtle.digest('SHA-256', buf);
  return [...new Uint8Array(hash)].map(b=>b.toString(16).padStart(2,'0')).join('');
}

// ---- API adapter: tries public routes, falls back to admin routes ----
// --- Holds API client ---
async function apiGetHolds(includeClosed=false) {
  const key = getAdminKey();
  const qs = includeClosed ? '?all=1' : '';
  const res = await fetch(`/api/holds${qs}`, {
    headers: { 'x-admin-key': key }
  });
  if (!res.ok) throw new Error(`Load holds failed: ${res.status}`);
  return res.json(); // [{id,userId,itemName,points,status,createdAt,givenAt}]
}

async function apiMarkGiven(holdId, isGiven) {
  const key = getAdminKey();
  const res = await fetch(`/api/holds/${holdId}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'x-admin-key': key
    },
    body: JSON.stringify({ status: isGiven ? 'given' : 'held' })
  });
  if (!res.ok) throw new Error(`Mark given failed: ${res.status}`);
  return res.json();
}

async function apiCancelHold(holdId) {
  const key = getAdminKey();
  const res = await fetch(`/api/holds/${holdId}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'x-admin-key': key
    },
    body: JSON.stringify({ status: 'canceled' })
  });
  if (!res.ok) throw new Error(`Cancel hold failed: ${res.status}`);
  return res.json();
}
// --- Holds UI ---
async function loadHolds() {
  const statusEl = $('holdsStatus');
  const tbody = $('tblHolds').querySelector('tbody');
  const includeClosed = $('chkShowClosedHolds')?.checked;

  statusEl.textContent = 'Loading...';
  tbody.innerHTML = '';

  try {
    const rows = await apiGetHolds(!!includeClosed);
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="7" class="muted">No holds</td></tr>`;
      statusEl.textContent = '';
      return;
    }

    const frag = document.createDocumentFragment();
    for (const h of rows) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${formatDate(h.createdAt)}</td>
        <td>${h.userId}</td>
        <td>${h.itemName || h.itemId || ''}</td>
        <td>${h.points}</td>
        <td>${h.status}</td>
        <td>
          <input type="checkbox" ${h.status === 'given' ? 'checked' : ''} data-hold-id="${h.id}" class="hold-given">
        </td>
        <td>
          <button data-cancel-id="${h.id}" class="btn-cancel-hold" ${h.status !== 'held' ? 'disabled' : ''}>Cancel</button>
        </td>
      `;
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
    statusEl.textContent = '';

  } catch (err) {
    statusEl.textContent = err.message || String(err);
  }
}

// Event delegation for checkbox + cancel
function bindHoldsEvents() {
  const tbody = $('tblHolds').querySelector('tbody');

  tbody.addEventListener('change', async (e) => {
    if (e.target.matches('.hold-given')) {
      const id = e.target.getAttribute('data-hold-id');
      const checked = e.target.checked;
      try {
        await apiMarkGiven(id, checked);
        // Refresh the status cell
        const row = e.target.closest('tr');
        if (row) row.children[4].textContent = checked ? 'given' : 'held';
      } catch (err) {
        alert(err.message || 'Failed to update');
        // revert UI if API failed
        e.target.checked = !checked;
      }
    }
  });

  tbody.addEventListener('click', async (e) => {
    const btn = e.target.closest('.btn-cancel-hold');
    if (!btn) return;
    const id = btn.getAttribute('data-cancel-id');
    if (!confirm('Cancel this hold?')) return;
    try {
      await apiCancelHold(id);
      await loadHolds();
    } catch (err) {
      alert(err.message || 'Failed to cancel');
    }
  });

  $('btnLoadHolds')?.addEventListener('click', loadHolds);
  $('chkShowClosedHolds')?.addEventListener('change', loadHolds);
}

// One-time init call (do this once in your main init)
bindHoldsEvents();

const api = {
  async listRewards() {
    try {
      const res = await fetch('/rewards');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      return Array.isArray(d) ? d : (d.items || []);
    } catch (err) {
      toast('Failed to load rewards', 'error', 3000);
      throw err;
    }
  },

  async addReward({ name, price, imageUrl }) {
    try {
      await adminFetch('/admin/rewards', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, price, imageUrl })
      });
      toast('Reward added');
      return true;
    } catch (err) {
      toast('Failed to add reward', 'error', 3000);
      throw err;
    }
  },

  async updateReward({ id, name, price, imageUrl }) {
    try {
      await adminFetch('/admin/rewards/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, name, price, imageUrl })
      });
      toast('Reward updated');
      return true;
    } catch (err) {
      toast('Failed to update reward', 'error', 3000);
      throw err;
    }
  },

  async deleteReward(id) {
    try {
      await adminFetch('/admin/rewards/deactivate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id })
      });
      toast('Reward deleted');
      return true;
    } catch (err) {
      toast('Failed to delete reward', 'error', 3000);
      throw err;
    }
  },

async uploadFile(file) {
  try {
    // ---- validate file before encoding ----
    const MAX = 2 * 1024 * 1024; // 2MB
    const okTypes = ['image/jpeg','image/png','image/webp','image/gif'];
    if (!okTypes.includes(file?.type)) {
      toast('Unsupported image type', 'error', 3000);
      throw new Error('Unsupported image type');
    }
    if (file.size > MAX) {
      toast('Image too large (>2MB)', 'error', 3000);
      throw new Error('Too large');
    }

      // dedupe: if we already uploaded identical bytes, reuse URL
      const h = await hashFileSHA256(file);
      if (__uploadCache.has(h)) {
        const cached = __uploadCache.get(h);
//const rImgEl = document.getElementById('rImg');
//if (rImgEl) rImgEl.value = cached;     // fill the URL box for convenience
        toast('Reused existing upload');
        return cached;
      }

    // now do the real upload
    const dataUrl = await fileToBase64(file);
    const resp = await adminFetch('/admin/upload-image64', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ image64: dataUrl })
    });

      const url = (typeof resp === 'string') ? resp : resp?.url;
            __uploadCache.set(h, url);
      if (!url) throw new Error('No URL returned');
      toast('Image uploaded');
      return url;
    } catch (err) {
      toast('Failed to upload image', 'error', 3000);
      throw err;
    }
  }
};

    // ---------- elements ----------
    const toggleUrls   = $('toggleUrls');
    const btnLoad      = $('btnLoadRewards');
    const listEl       = $('rewardsList');
    const listMsg      = $('rewardsMsg');
const filterEl = document.getElementById('filterRewards');
const countEl  = document.getElementById('rewardsCount');
let allRewards = [];

    const rName        = $('rName');
    const rPrice       = $('rPrice');
    const rImg         = $('rImg');
    const btnAdd       = $('btnAddReward');

    const drop         = $('drop');
    const fileInput    = $('file');
    const upStatus     = $('uploadStatus');

    const adminKey     = $('adminKey');
    const btnSaveKey   = $('saveAdminKey');

        // legacy issue panel
    const issUserId    = $('issUserId');
    const issAmount    = $('issAmount');
    const issTask      = $('issTask');
    const btnMintQR    = $('btnMintQR');
    const btnSpend     = $('btnSpend');
    const btnCheck     = $('btnCheck');
    const qrBox        = $('qr');
    const issueStatus  = $('issueStatus');

    // lightbox
    const imgModal     = $('imgModal');
    const imgModalImg  = $('imgModalImg');

    // ---------- init small UI states ----------
    // admin key
    if (adminKey) adminKey.value = loadKey();
btnSaveKey?.addEventListener('click', () => {
  saveKey(adminKey.value.trim());
  toast('Admin key saved');
  btnSaveKey.disabled = true;
  setTimeout(() => (btnSaveKey.disabled = false), 400);
});



    // ---------- rewards: API calls ----------
async function loadRewards() {
  listMsg.textContent = 'Loading…';
  try {
    const items = await api.listRewards();
    allRewards = items || [];
    applyRewardsFilter();        // renders filtered view
    listMsg.textContent = '';
  } catch (err) {
    listMsg.textContent = 'Failed to load.';
    toast('Failed to load rewards', 'error', 3000);
  }
}

function applyRewardsFilter() {
  const q = (filterEl && filterEl.value ? filterEl.value : '').trim().toLowerCase();
  let view = allRewards;
  if (q) {
    view = allRewards.filter(it => {
       const name  = String(it.name || '').toLowerCase();
        const price = String(it.price ?? '');
        const url   = String(it.imageUrl || it.image_url || '').toLowerCase();
      return name.includes(q) || price.includes(q) || url.includes(q);
    });
  }
  renderRewards(view);
  if (countEl) {
  countEl.textContent = view.length ? `${view.length} shown` : '0 shown';
  }
  // empty-state text stays as "No rewards yet" for zero items overall
  const empty =  document.getElementById('emptyRewards');
  if (empty) empty.textContent = allRewards.length === 0 ? 'No rewards yet. Click “Add Reward”.'
                                                        : (q ? 'No matches.' : 'No rewards.');
}

    async function addReward() {
      const name  = (rName.value || '').trim();
      const price = Number(rPrice.value || 0);
      const imageUrl = (rImg.value || '').trim() || '';
      if (!name || !Number.isFinite(price) || price <= 0) { alert('Enter a name and a price > 0'); return; }
      try {
        await api.addReward({ name, price, imageUrl });
        rName.value = ''; rPrice.value = ''; rImg.value = '';
        await loadRewards();
      } catch (e) { console.error(e); listMsg.textContent = 'Failed to add reward.'; }
    }

    async function updateReward(it, update) {
      await api.updateReward({ id: it.id, ...update });
    }
    async function deleteReward(it) {
      await api.deleteReward(it.id);
    }

    // ---------- rewards: render + edit ----------
    // REPLACE your entire renderRewards function with this
    function renderRewards(items) {
      const empty = document.getElementById('emptyRewards');
      const list  = document.getElementById('rewardsList');
      if (!list) return;

      list.innerHTML = '';

      // empty-state handling
      if (!items || items.length === 0) {
        if (empty) empty.classList.remove('hidden');
        return;
      }
      if (empty) empty.classList.add('hidden');

      for (const it of items) {
        const imgUrl = it.image_url || it.imageUrl || '';
        const row = document.createElement('div');
        row.className = 'reward';
        row.innerHTML = `
          <img class="thumb" src="${escapeHTML(imgUrl)}" data-full="${escapeHTML(imgUrl)}"
               style="width:56px;height:56px;object-fit:cover;border-radius:8px;border:1px solid #eee;${imgUrl?'cursor:zoom-in;':''}">
          <div class="meta">
            <div class="name">${escapeHTML(it.name || '')}
              <span class="help" style="font-weight:400;color:#888">• ${Number(it.price||0)} pt</span>
            </div>
            ${imgUrl ? `<div class="url">${escapeHTML(imgUrl)}</div>` : ''}
          </div>
          <div class="actions">
            <button data-act="edit">Edit</button>
            <button data-act="del">Delete</button>
          </div>
        `;

        // zoom
        const thumb = row.querySelector('.thumb');
        if (imgUrl && thumb) thumb.addEventListener('click', () => openLightbox(imgUrl));

        // actions
        row.querySelector('[data-act="del"]')?.addEventListener('click', async () => {
          if (!confirm('Delete this reward?')) return;
          try {
            await api.deleteReward(it.id);
            await loadRewards();
          } catch (e) {
            console.error(e); alert('Delete failed');
          }
        });

        row.querySelector('[data-act="edit"]')?.addEventListener('click', () => {
          beginEditReward(it, row);
        });

        list.appendChild(row);
      }
    }


    function beginEditReward(it, row) {
      const imgUrl = it.image_url || it.imageUrl || '';
      const uid = 'u' + Date.now().toString(36) + Math.random().toString(36).slice(2,7);
      const form = document.createElement('div');
      form.className = '__editForm';
      form.style.gridColumn = '1 / -1';
      form.style.background = '#fff';
      form.style.border = '1px dashed #ccc';
      form.style.borderRadius = '8px';
      form.style.padding = '8px';
      form.style.marginTop = '6px';

      form.innerHTML = `
        <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center">
          <input id="eName-${uid}"  type="text"  value="${escapeHTML(it.name||'')}"  placeholder="Name"
                 style="flex:1;min-width:160px;padding:6px 8px;border:1px solid #ddd;border-radius:6px">
          <input id="ePrice-${uid}" type="number" value="${Number(it.price||0)}" placeholder="Price"
                 style="width:100px;padding:6px 8px;border:1px solid #ddd;border-radius:6px">
          <input id="eImg-${uid}"   type="text"  value="${escapeHTML(imgUrl)}" placeholder="Image URL"
                 style="flex:2;min-width:220px;padding:6px 8px;border:1px solid #ddd;border-radius:6px">
          <button id="eSave-${uid}"   style="padding:6px 10px;border:1px solid #ddd;border-radius:8px;background:#fff">Save</button>
          <button id="eCancel-${uid}" style="padding:6px 10px;border:1px solid #ddd;border-radius:8px;background:#fff">Cancel</button>
        </div>

        <div style="margin-top:8px;">
          <div id="eDrop-${uid}" class="drop">
            <div><b>Drop image here</b> or click to choose</div>
            <div class="help">Supported: jpg, jpeg, png, webp, gif</div>
            <input id="eFile-${uid}" type="file" accept="image/*" style="display:none">
          </div>
          <div id="eUpStatus-${uid}" class="help" style="margin-top:6px;min-height:18px;"></div>
        </div>
      `;

      row.querySelector('.__editForm')?.remove();
      row.appendChild(form);

      const nameEl  = form.querySelector(`#eName-${uid}`);
      const priceEl = form.querySelector(`#ePrice-${uid}`);
      const imgEl   = form.querySelector(`#eImg-${uid}`);
      const saveEl  = form.querySelector(`#eSave-${uid}`);
      const cancelEl= form.querySelector(`#eCancel-${uid}`);
      const dropEl  = form.querySelector(`#eDrop-${uid}`);
      const fileEl  = form.querySelector(`#eFile-${uid}`);
      const statEl  = form.querySelector(`#eUpStatus-${uid}`);

      // attach uploader into the edit form (writes URL into imgEl)
      attachInlineUploader(dropEl, fileEl, statEl, imgEl);

      const doSave = async () => {
        const name  = (nameEl.value || '').trim();
        const price = Number(priceEl.value || 0);
        const imageUrl = (imgEl.value || '').trim();
        if (!name) return alert('Name required');
        if (!Number.isFinite(price) || price <= 0) return alert('Price > 0');
        try {
          await updateReward(it, { name, price, imageUrl });
          form.remove();                 // close form immediately
          await loadRewards();           // refresh list
        } catch (e) {
          console.error(e); alert('Update failed');
        }
      };

      cancelEl.addEventListener('click', () => form.remove());
      saveEl.addEventListener('click', doSave);
      form.addEventListener('keydown', (e) => { if (e.key === 'Enter') doSave(); });
    }


    // ---------- uploader (fills #rImg) ----------
    if (drop && fileInput) {
      const setBusy = (b, t='') => {
        drop.classList.toggle('drag', b);
        if (upStatus) upStatus.textContent = t;
      };
      const pick = () => fileInput.click();

      drop.addEventListener('click', pick);
      ['dragenter','dragover'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); setBusy(true, 'Drop to upload'); }));
      ['dragleave','drop'].forEach(ev => drop.addEventListener(ev, e => { e.preventDefault(); setBusy(false); }));
      drop.addEventListener('drop', (e) => handleFiles(e.dataTransfer.files));
      fileInput.addEventListener('change', (e) => handleFiles(e.target.files));

      async function handleFiles(list) {
        const f = list && list[0]; if (!f) return;
        try {
          setBusy(true, 'Uploading…');
          const url = await api.uploadFile(f);
          if (!url) throw new Error('No URL returned');
          rImg.value = url;
          upStatus.textContent = `Uploaded ✓ ${url}`;
        } catch (e) {
          console.error(e);
          upStatus.textContent = 'Upload failed.';
        } finally {
          setBusy(false);
        }
      }

    }

    // ---------- legacy Issue panel ----------
    // Generate Earn QR
    btnMintQR?.addEventListener('click', async () => {
      const userId = (issUserId?.value || '').trim();
      const amount = Number(issAmount?.value || 0);
      const task   = (issTask?.value || '').trim();
      if (!userId || !Number.isFinite(amount) || amount <= 0) return alert('User ID and amount > 0');

      try {
        const data = await adminFetch('/earn', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ userId, amount, task })
        });
        const url = data?.url || '';
        if (!url) throw new Error('No QR URL returned');
        renderQR(url);                    // <<— use full URL
        setIssueStatus('QR ready — scan to claim', 'ok');
        toast('QR generated');

      } catch (e) {
        console.error(e); setIssueStatus('QR failed: '+e.message, 'err');
      }
    });


    // Spend
    btnSpend?.addEventListener('click', async () => {
      const userId = (issUserId?.value || '').trim();
      const amount = Number(issAmount?.value || 0);
      if (!userId || !Number.isFinite(amount) || amount <= 0) return alert('User ID and amount > 0');
      try {
        const r = await adminFetch('/spend', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ userId, amount })
        });
        setIssueStatus('Spend ok. New balance: ' + (r?.balance ?? '?'), 'ok');
        toast('Spend successful');
      } catch (e) { console.error(e); setIssueStatus('Spend failed: '+e.message, 'err'); }
    });


    // Balance
    btnCheck?.addEventListener('click', async () => {
      const userId = (issUserId?.value || '').trim();
      if (!userId) return alert('User ID required');
      try {
        const res = await fetch('/balance/' + encodeURIComponent(userId));
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const j = await res.json();
        setIssueStatus('Balance: ' + (j?.balance ?? '?'), 'ok');
        toast('Balance checked');
      } catch (e) { console.error(e); setIssueStatus('Check failed: '+e.message, 'err'); }
    });


    function setIssueStatus(text, kind) {
      if (!issueStatus) return;
      issueStatus.textContent = text;
      issueStatus.style.color = kind === 'ok' ? '#0a7a30' : '#b00020';
    }

    function renderQR(url) {
      if (!qrBox) return;
      qrBox.innerHTML = '';
      try {
        new QRCode(qrBox, { text: String(url), width:160, height:160, correctLevel: QRCode.CorrectLevel.M });
      } catch (e) { console.error('QR render error', e); qrBox.textContent = 'QR failed'; }
      const linkEl = document.getElementById('qrLink');
      const helpEl = document.getElementById('qrHelp');
      if (linkEl) linkEl.value = String(url || '');
      if (helpEl) helpEl.textContent = url ? 'Share or open this link on the child device.' : '';
    }


    // ---------- lightbox ----------
    function openLightbox(src) {
      if (!imgModal || !imgModalImg) return;
      imgModalImg.src = src;
      imgModal.style.display = 'flex';
    }
    imgModal?.addEventListener('click', () => {
      imgModal.style.display = 'none';
      imgModalImg.src = '';
    });

document.getElementById('btnCopy')?.addEventListener('click', async () => {
  const s = document.getElementById('qrLink')?.value || '';
  if (!s) return;
  try {
    await navigator.clipboard.writeText(s);
    toast('Link copied');
  } catch {
    // fallback
    const el = document.getElementById('qrLink');
    el?.select();
    document.execCommand('copy');
    toast('Link copied');
  }
});

    // ---------- wire buttons ----------
    btnLoad?.addEventListener('click', loadRewards);
    btnAdd?.addEventListener('click', addReward);
    filterEl?.addEventListener('input', () => applyRewardsFilter());

    // No auto-load: per UX, the Rewards list stays empty until you click “Load items”.
  })(); // end module IIFE
} // end init guard
