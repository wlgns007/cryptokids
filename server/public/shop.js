// public/shop.js — kid-facing shop (FULL FILE: paste over your current shop.js)
(() => {
  const $ = (id) => document.getElementById(id);
  const say = (s) => { const el = $('msg'); if (el) el.textContent = s; };

  // pending state to prevent double-mint
  let pendingSpend = false;

// ADD: globals for countdown
let qrExpireAtSec = 0;
let qrCountdownTimer = null;

  // REPLACE the entire setPendingUI() with this
function setPendingUI(on) {
  pendingSpend = !!on;

  // disable/enable Buy buttons
  document.querySelectorAll('#items button').forEach(b => {
    if (on) { b.dataset.prevDisabled = b.disabled ? '1' : '0'; b.disabled = true; }
    else if (b.dataset.prevDisabled === '0') { b.disabled = false; delete b.dataset.prevDisabled; }
  });

  // ensure a visible Cancel button right below the QR box
  const qr = document.getElementById('qr');
  if (!qr) return;

  let cancel = document.getElementById('cancelPending');
  if (!cancel) {
    cancel = document.createElement('button');
    cancel.type = 'button';
    cancel.id = 'cancelPending';
    cancel.textContent = 'Cancel';
    cancel.style.marginTop = '8px';

    // prefer placing AFTER the QR box so it’s always visible
    qr.insertAdjacentElement('afterend', cancel);
    // fallback inside the box if afterend fails for some reason
    if (!cancel.isConnected) qr.appendChild(cancel);

    cancel.onclick = () => {
      qr.innerHTML = '';
      qr.style.display = 'none';
      const a = document.getElementById('approveLink');
      if (a) a.style.display = 'none';
      const note = document.getElementById('watchNote');
      if (note) note.style.display = 'none';
      setPendingUI(false);
    };
  }

  cancel.style.display = on ? '' : 'none';
}


  async function getJSON(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  }
  async function postJSON(url, body) {
    const r = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || 'Request failed');
    return data;
  }

  // Load items + balance, render grid, manage empty state and QR visibility
  async function loadShop() {
    const userId = $('userId')?.value?.trim();
    const shopEmpty = $('shopEmpty');
    const itemsWrap = $('items'); // grid
    const msg = $('msg');

    if (!userId) { alert('User ID required'); return; }
    if (msg) msg.textContent = 'Loading…';

    // 1) Fetch balance
    let balance = 0;
    try {
      const rb = await fetch(`/balance/${encodeURIComponent(userId)}`);
      if (rb.ok) {
        const bj = await rb.json();
        balance = Number(bj?.balance || 0);
      }
    } catch (_) {}

    // 2) Fetch items from /rewards
    let items = [];
    try {
      const ri = await fetch('/rewards');
      if (!ri.ok) throw new Error(`HTTP ${ri.status}`);
      const data = await ri.json(); // { items: [...] }
      items = Array.isArray(data.items) ? data.items : [];
    } catch (err) {
      console.error('Load items failed:', err);
      if (msg) msg.textContent = `Failed to load items: ${err.message}`;
      items = [];
    }

    // 3) Render + empty state + hide QR/approval link when empty
    if (itemsWrap) itemsWrap.innerHTML = '';
    const isEmpty = !Number.isFinite(balance) || balance <= 0;

    const qrBox = $('qr');
    if (qrBox) {
      qrBox.innerHTML = '';
      qrBox.style.display = isEmpty ? 'none' : '';
    }
    const approveLink = $('approveLink');
    if (approveLink) approveLink.style.display = isEmpty ? 'none' : '';

    if (shopEmpty) shopEmpty.style.display = isEmpty ? 'block' : 'none';
    if (msg) msg.textContent = `Balance: ${balance} RT`;

    if (!items.length) {
      if (itemsWrap) itemsWrap.innerHTML = '<i>No items yet.</i>';
      return;
    }

    for (const it of items) {
      const canAfford = balance >= it.price;

      const card = document.createElement('div');
      card.className = 'card';
      if (!canAfford) card.style.opacity = '0.6';

// ADD: thumbnail on card (optional)
if (it.image_url) {
  const img = document.createElement('img');
  img.src = it.image_url;
  img.alt = '';
  img.style = 'width:100%;height:110px;object-fit:cover;border-radius:8px;margin-bottom:8px;border:1px solid #eee;';
  img.onerror = () => { img.remove(); }; // hide if broken URL
  card.appendChild(img);
}
      const name = document.createElement('div');
      name.className = 'name';
      name.textContent = it.name;

      const price = document.createElement('div');
      price.className = 'price';
      price.textContent = `${it.price} RT`;

// ADD: optional description
if (it.description) {
  const desc = document.createElement('div');
  desc.style = 'opacity:.8;font-size:13px;margin-top:4px;';
  desc.textContent = it.description;
  card.appendChild(desc);
}

      const btn = document.createElement('button');
      btn.textContent = canAfford ? 'Buy' : 'Not enough RT';
      btn.disabled = !canAfford;
      if (canAfford) btn.onclick = () => buy(it.id);

      card.appendChild(name);
      card.appendChild(price);
      card.appendChild(btn);
      itemsWrap && itemsWrap.appendChild(card);
    }
  }

  // Mint spend, render QR + approval link, watch for approval
  async function buy(rewardId) {
    if (pendingSpend) { alert('Please complete or cancel the current approval first.'); return; }
    setPendingUI(true);

    try {
      const userId = ($('userId')?.value || '').trim();
      if (!userId) throw new Error('Enter your ID first');

      // re-check balance just before mint (prevents race)
      const bal = await getJSON(`/balance/${encodeURIComponent(userId)}`);
      const reward = (await getJSON('/rewards')).items.find(r => r.id === rewardId);
      if (!reward) throw new Error('Reward not found. Reload and try again.');
      if (bal.balance < reward.price) {
        say(`Not enough RT. You have ${bal.balance}, need ${reward.price}.`);
        setPendingUI(false);
        return;
      }

      say('Creating approval QR…');
      const data = await postJSON('/shop/mintSpend', { userId, rewardId });

      // capture starting balance & price for watcher
      const starting = await getJSON(`/balance/${encodeURIComponent(userId)}`);
      const spendPrice = Number(data?.payload?.price ?? 0);

// ADD: start a countdown until expiresAt
qrExpireAtSec = Number(data?.expiresAt || 0);
const timerEl = $('qrTimer');
if (timerEl) {
  if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
  const tick = () => {
    const remain = Math.max(0, qrExpireAtSec - Math.floor(Date.now()/1000));
    if (remain <= 0) {
      timerEl.textContent = 'QR expired';
      timerEl.style.display = 'block';
      // auto-cancel pending UI and hide QR
      const qr = $('qr'); if (qr) { qr.innerHTML=''; qr.style.display='none'; }
      const a = $('approveLink'); if (a) a.style.display='none';
      const note = $('watchNote'); if (note) note.style.display='none';
      setPendingUI(false);
      clearInterval(qrCountdownTimer); qrCountdownTimer = null;
      return;
    }
    const m = Math.floor(remain / 60), s = remain % 60;
    timerEl.textContent = `Expires in ${m}:${String(s).padStart(2,'0')}`;
    timerEl.style.display = 'block';
  };
  tick();
  qrCountdownTimer = setInterval(tick, 1000);
}


      // show QR
      const box = $('qr'); box.innerHTML = ''; box.style.display = '';
      if (typeof QRCode !== 'function') { box.textContent = 'qrcode.min.js missing'; setPendingUI(false); return; }
      new QRCode(box, { text: data.url, width: 220, height: 220, correctLevel: QRCode.CorrectLevel.M });

      say(`Ask your parent to scan to approve: ${data.payload.item} (−${data.payload.price} RT)`);

      // approval link with stable id
      let a = $('approveLink');
      if (!a) {
        a = document.createElement('a');
        a.id = 'approveLink';
        a.style.display = 'block';
        a.style.marginTop = '8px';
        box.appendChild(a);
      }
      a.href = data.url;
      a.target = '_blank';
      a.rel = 'noopener';
      a.textContent = 'Open approval link';
      a.style.display = 'block';

      // watch for approval (poll balance up to ~60s)
      const note = $('watchNote');
      if (note) note.style.display = 'block';

      let tries = 0;
      const maxTries = 30;      // 30 * 2000ms ≈ 60s
      const intervalMs = 2000;

      const timer = setInterval(async () => {
        tries++;
        try {
          const now = await getJSON(`/balance/${encodeURIComponent(userId)}`);
          const dropped = Number(starting.balance) - Number(now.balance);
          if (spendPrice > 0 && dropped >= spendPrice) {
            clearInterval(timer);
            if (note) note.style.display = 'none';

            // cleanup QR & link
            const qr = $('qr');
            if (qr) { qr.innerHTML = ''; qr.style.display = 'none'; }
            const approveLink = $('approveLink');
            if (approveLink) approveLink.style.display = 'none';

            // refresh list and unlock UI
            say(`Approved. Balance: ${now.balance} RT`);
            setPendingUI(false);
            await loadShop();
if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
$('qrTimer') && ($('qrTimer').style.display = 'none');

          }
        } catch (_) { /* ignore transient poll errors */ }

        if (tries >= maxTries) {
          clearInterval(timer);
if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
$('qrTimer') && ($('qrTimer').style.display = 'none');
          if (note) note.style.display = 'none';
          setPendingUI(false);
        }
      }, intervalMs);
    } catch (e) {
      say(e?.message || 'Failed to create approval');
      setPendingUI(false);
if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
const qt = document.getElementById('qrTimer'); if (qt) qt.style.display = 'none';

    }
  }

  // Wire
  $('loadItemsBtn')?.addEventListener('click', () => {
    console.log('[shop] Load Items clicked');
    loadShop().catch(e => { console.error(e); say('Failed to load items'); });
  });

  console.log('[shop] shop.js loaded');
})();
