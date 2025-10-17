// admin.js — CleverKids Admin (public rewards API version)

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

// ---- API adapter: tries public routes, falls back to admin routes ----
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
      // server expects base64 JSON to /admin/upload-image64
      const dataUrl = await fileToBase64(file);
      const resp = await adminFetch('/admin/upload-image64', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image64: dataUrl })
      });
      const url = (typeof resp === 'string') ? resp : resp?.url;
      if (!url) throw new Error('No URL returned');
      toast('Image uploaded');
      return url;
    } catch (err) {
      toast('Failed to upload image', 'error', 3000);
      throw err;
    }
  }
};


function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(r.result);
    r.onerror = reject;
    r.readAsDataURL(file); // produces data:image/...;base64,XXXX
  });
}

    // ---------- elements ----------
    const toggleUrls   = $('toggleUrls');
    const btnLoad      = $('btnLoadRewards');
    const listEl       = $('rewardsList');
    const listMsg      = $('rewardsMsg');

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
renderRewards(items);
listMsg.textContent = ''; // empty-state handles the “no items” copy
      } catch (e) {
        console.error(e);
        listMsg.textContent = 'Failed to load rewards.';
      }
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
    function renderRewards(items) {
        const empty = document.getElementById('emptyRewards');
        const list  = document.getElementById('rewardsList'); // or tbody if you use a table
        if (!list) return;

        list.innerHTML = '';
        if (!items || items.length === 0) {
          if (empty) empty.classList.remove('hidden');
          return;
        }
        if (empty) empty.classList.add('hidden');

      listEl.innerHTML = '';
      if (!items || !items.length) return;

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
            await deleteReward(it);
            await loadRewards();
          } catch (e) {
            console.error(e); alert('Delete failed');
          }
        });

        row.querySelector('[data-act="edit"]')?.addEventListener('click', () => {
          beginEditReward(it, row);
        });

        listEl.appendChild(row);
      }
    }

    function beginEditReward(it, row) {
      const imgUrl = it.image_url || it.imageUrl || '';
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
          <input id="eName"  type="text"  value="${escapeHTML(it.name||'')}"  placeholder="Name"
                 style="flex:1;min-width:160px;padding:6px 8px;border:1px solid #ddd;border-radius:6px">
          <input id="ePrice" type="number" value="${Number(it.price||0)}" placeholder="Price"
                 style="width:100px;padding:6px 8px;border:1px solid #ddd;border-radius:6px">
          <input id="eImg"   type="text"  value="${escapeHTML(imgUrl)}" placeholder="Image URL"
                 style="flex:2;min-width:220px;padding:6px 8px;border:1px solid #ddd;border-radius:6px">
          <button id="eSave"  style="padding:6px 10px;border:1px solid #ddd;border-radius:8px;background:#fff">Save</button>
          <button id="eCancel"style="padding:6px 10px;border:1px solid #ddd;border-radius:8px;background:#fff">Cancel</button>
        </div>
      `;
      row.querySelector('.__editForm')?.remove();
      row.appendChild(form);

      const doSave = async () => {
        const name  = form.querySelector('#eName').value.trim();
        const price = Number(form.querySelector('#ePrice').value || 0);
        const imageUrl = form.querySelector('#eImg').value.trim();
        if (!name) return alert('Name required');
        if (!Number.isFinite(price) || price <= 0) return alert('Price > 0');
        try {
          await updateReward(it, { name, price, imageUrl });
          await loadRewards();
        } catch (e) {
          console.error(e); alert('Update failed');
        }
      };

      form.querySelector('#eCancel').addEventListener('click', () => form.remove());
      form.querySelector('#eSave').addEventListener('click', doSave);
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

    // ---------- wire buttons ----------
    btnLoad?.addEventListener('click', loadRewards);
    btnAdd?.addEventListener('click', addReward);

    // No auto-load: per UX, the Rewards list stays empty until you click “Load items”.
  })(); // end module IIFE
} // end init guard
