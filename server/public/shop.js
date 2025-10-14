i18n.ready(() => {
  const $ = (id) => document.getElementById(id);
  const setI18nText = (el, key, params = {}) => {
    if (!el) return;
    el.setAttribute('data-i18n', key);
    const hasParams = params && Object.keys(params).length > 0;
    if (hasParams) el.setAttribute('data-i18n-params', JSON.stringify(params));
    else el.removeAttribute('data-i18n-params');
    el.textContent = i18n.t(key, params);
  };
  const say = (key, params = {}) => {
    const el = $('msg');
    if (!el) return;
    setI18nText(el, key, params);
  };
  const sayRaw = (text = '') => {
    const el = $('msg');
    if (!el) return;
    el.textContent = text;
    el.removeAttribute('data-i18n');
    el.removeAttribute('data-i18n-params');
  };

  let pendingSpend = false;
  let qrExpireAtSec = 0;
  let qrCountdownTimer = null;

  function setPendingUI(on) {
    pendingSpend = !!on;

    document.querySelectorAll('#items button').forEach((b) => {
      if (on) {
        b.dataset.prevDisabled = b.disabled ? '1' : '0';
        b.disabled = true;
      } else if (b.dataset.prevDisabled === '0') {
        b.disabled = false;
        delete b.dataset.prevDisabled;
      }
    });

    const qr = $('qr');
    if (!qr) return;

    if (!on) {
      const label = $('qrRewardLabel');
      if (label) {
        label.textContent = '';
        label.style.display = 'none';
        label.removeAttribute('data-i18n');
        label.removeAttribute('data-i18n-params');
      }
    }

    let cancel = $('cancelPending');
    if (!cancel) {
      cancel = document.createElement('button');
      cancel.type = 'button';
      cancel.id = 'cancelPending';
      qr.insertAdjacentElement('afterend', cancel);
      if (!cancel.isConnected) qr.appendChild(cancel);
      cancel.onclick = () => {
        qr.innerHTML = '';
        qr.style.display = 'none';
        const a = $('approveLink');
        if (a) a.style.display = 'none';
        const note = $('watchNote');
        if (note) note.style.display = 'none';
        const label = $('qrRewardLabel');
        if (label) {
          label.textContent = '';
          label.style.display = 'none';
          label.removeAttribute('data-i18n');
          label.removeAttribute('data-i18n-params');
        }
        const timer = $('qrTimer');
        if (timer) {
          timer.style.display = 'none';
          timer.textContent = '';
          timer.removeAttribute('data-i18n');
          timer.removeAttribute('data-i18n-params');
        }
        if (qrCountdownTimer) {
          clearInterval(qrCountdownTimer);
          qrCountdownTimer = null;
        }
        setPendingUI(false);
      };
    }
    setI18nText(cancel, 'shop.buttons.cancel');
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
    if (!r.ok) throw new Error(data.error || i18n.t('shop.errors.requestFailed'));
    return data;
  }

  async function loadShop() {
    const userId = $('userId')?.value?.trim();
    const shopEmpty = $('shopEmpty');
    const itemsWrap = $('items');
    const rewardLabel = $('qrRewardLabel');

    if (!userId) { alert(i18n.t('shop.errors.userIdRequired')); return; }
    say('shop.messages.loading');

    let balance = 0;
    try {
      const rb = await fetch(`/balance/${encodeURIComponent(userId)}`);
      if (rb.ok) {
        const bj = await rb.json();
        balance = Number(bj?.balance || 0);
      }
    } catch (_) {}

    let items = [];
    try {
      const ri = await fetch('/rewards');
      if (!ri.ok) throw new Error(`HTTP ${ri.status}`);
      const data = await ri.json();
      items = Array.isArray(data.items) ? data.items : [];
    } catch (err) {
      console.error('Load items failed:', err);
      say('shop.errors.loadItems', { message: err.message });
      items = [];
    }

    if (itemsWrap) itemsWrap.innerHTML = '';
    const isEmpty = !Number.isFinite(balance) || balance <= 0;

    const qrBox = $('qr');
    if (qrBox) {
      qrBox.innerHTML = '';
      qrBox.style.display = isEmpty ? 'none' : '';
    }
    if (rewardLabel) {
      rewardLabel.textContent = '';
      rewardLabel.style.display = 'none';
      rewardLabel.removeAttribute('data-i18n');
      rewardLabel.removeAttribute('data-i18n-params');
    }
    const approveLink = $('approveLink');
    if (approveLink) approveLink.style.display = isEmpty ? 'none' : '';

    if (shopEmpty) shopEmpty.style.display = isEmpty ? 'block' : 'none';

    say('shop.messages.balance', { amount: balance });

    if (!items.length) {
      if (itemsWrap) {
        const empty = document.createElement('i');
        setI18nText(empty, 'shop.messages.noItems');
        itemsWrap.appendChild(empty);
      }
      return;
    }

    for (const it of items) {
      const canAfford = balance >= it.price;

      const card = document.createElement('div');
      card.className = 'card';
      if (!canAfford) card.style.opacity = '0.6';

      if (it.image_url) {
        const img = document.createElement('img');
        img.src = it.image_url;
        img.alt = '';
        img.style = 'width:100%;height:110px;object-fit:cover;border-radius:8px;margin-bottom:8px;border:1px solid #eee;';
        img.onerror = () => { img.remove(); };
        card.appendChild(img);
      }

      const name = document.createElement('div');
      name.className = 'name';
      name.textContent = it.name;

      const price = document.createElement('div');
      price.className = 'price';
      setI18nText(price, 'shop.messages.priceRt', { amount: it.price });

      if (it.description) {
        const desc = document.createElement('div');
        desc.style = 'opacity:.8;font-size:13px;margin-top:4px;';
        desc.textContent = it.description;
        card.appendChild(desc);
      }

      const btn = document.createElement('button');
      if (canAfford) {
        setI18nText(btn, 'shop.buttons.buy');
        btn.disabled = false;
        btn.onclick = () => buy(it.id);
      } else {
        setI18nText(btn, 'shop.buttons.notEnoughRt');
        btn.disabled = true;
        btn.onclick = null;
      }

      card.appendChild(name);
      card.appendChild(price);
      card.appendChild(btn);
      itemsWrap && itemsWrap.appendChild(card);
    }
  }

  async function buy(rewardId) {
    if (pendingSpend) { alert(i18n.t('shop.alerts.pending')); return; }
    setPendingUI(true);

    try {
      const userId = ($('userId')?.value || '').trim();
      if (!userId) throw new Error('ENTER_ID');

      const bal = await getJSON(`/balance/${encodeURIComponent(userId)}`);
      const reward = (await getJSON('/rewards')).items.find(r => r.id === rewardId);
      if (!reward) throw new Error('REWARD_NOT_FOUND');
      if (bal.balance < reward.price) {
        say('shop.messages.notEnoughRt', { balance: bal.balance, price: reward.price });
        setPendingUI(false);
        return;
      }

      say('shop.messages.qrCreating');
      const data = await postJSON('/shop/mintSpend', { userId, rewardId });

      const starting = await getJSON(`/balance/${encodeURIComponent(userId)}`);
      const spendPrice = Number(data?.payload?.price ?? 0);

      qrExpireAtSec = Number(data?.expiresAt || 0);
      const timerEl = $('qrTimer');
      if (timerEl) {
        if (qrCountdownTimer) {
          clearInterval(qrCountdownTimer);
          qrCountdownTimer = null;
        }
        const tick = () => {
          const remain = Math.max(0, qrExpireAtSec - Math.floor(Date.now()/1000));
          if (remain <= 0) {
            setI18nText(timerEl, 'shop.qr.expired');
            timerEl.style.display = 'block';
            const qr = $('qr'); if (qr) { qr.innerHTML=''; qr.style.display='none'; }
            const a = $('approveLink'); if (a) a.style.display='none';
            const note = $('watchNote'); if (note) note.style.display='none';
            setPendingUI(false);
            clearInterval(qrCountdownTimer);
            qrCountdownTimer = null;
            return;
          }
          const m = Math.floor(remain / 60);
          const s = String(remain % 60).padStart(2,'0');
          setI18nText(timerEl, 'shop.qr.expiresIn', { minutes: String(m), seconds: s });
          timerEl.style.display = 'block';
        };
        tick();
        qrCountdownTimer = setInterval(tick, 1000);
      }

      const box = $('qr');
      if (box) {
        box.innerHTML = '';
        box.style.display = '';
        if (typeof QRCode !== 'function') {
          setI18nText(box, 'shop.qr.missingLib');
          setPendingUI(false);
          return;
        }
        new QRCode(box, { text: data.url, width: 220, height: 220, correctLevel: QRCode.CorrectLevel.M });
      }

      const rewardLabelEl = $('qrRewardLabel');
      if (rewardLabelEl) {
        setI18nText(rewardLabelEl, 'shop.qr.rewardLabel', { reward: reward.name });
        rewardLabelEl.style.display = 'block';
      }

      say('shop.messages.approvalPrompt', { item: data.payload.item, price: data.payload.price });

      let a = $('approveLink');
      if (!a) {
        a = document.createElement('a');
        a.id = 'approveLink';
        a.style.display = 'block';
        a.style.marginTop = '8px';
        const boxEl = $('qr');
        if (boxEl) boxEl.appendChild(a);
      }
      a.href = data.url;
      a.target = '_blank';
      a.rel = 'noopener';
      setI18nText(a, 'shop.buttons.openApproval');
      a.style.display = 'block';

      const note = $('watchNote');
      if (note) note.style.display = 'block';

      let tries = 0;
      const maxTries = 30;
      const intervalMs = 2000;

      const timer = setInterval(async () => {
        tries++;
        try {
          const now = await getJSON(`/balance/${encodeURIComponent(userId)}`);
          const dropped = Number(starting.balance) - Number(now.balance);
          if (spendPrice > 0 && dropped >= spendPrice) {
            clearInterval(timer);
            if (note) note.style.display = 'none';

            const qr = $('qr');
            if (qr) { qr.innerHTML = ''; qr.style.display = 'none'; }
            const approveLink = $('approveLink');
            if (approveLink) approveLink.style.display = 'none';

            say('shop.messages.approvedBalance', { amount: now.balance });
            setPendingUI(false);
            await loadShop();
            if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
            const timerNode = $('qrTimer');
            if (timerNode) { timerNode.style.display = 'none'; timerNode.textContent = ''; timerNode.removeAttribute('data-i18n'); timerNode.removeAttribute('data-i18n-params'); }
          }
        } catch (_) {}

        if (tries >= maxTries) {
          clearInterval(timer);
          if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
          const timerNode = $('qrTimer');
          if (timerNode) { timerNode.style.display = 'none'; timerNode.textContent = ''; timerNode.removeAttribute('data-i18n'); timerNode.removeAttribute('data-i18n-params'); }
          if (note) note.style.display = 'none';
          setPendingUI(false);
        }
      }, intervalMs);
    } catch (e) {
      if (e?.message === 'ENTER_ID') {
        alert(i18n.t('shop.alerts.enterIdFirst'));
      } else if (e?.message === 'REWARD_NOT_FOUND') {
        say('shop.errors.rewardNotFound');
      } else {
        sayRaw(e?.message || i18n.t('shop.errors.createApproval'));
      }
      setPendingUI(false);
      if (qrCountdownTimer) { clearInterval(qrCountdownTimer); qrCountdownTimer = null; }
      const qt = $('qrTimer'); if (qt) { qt.style.display = 'none'; qt.textContent = ''; qt.removeAttribute('data-i18n'); qt.removeAttribute('data-i18n-params'); }
    }
  }

  $('loadItemsBtn')?.addEventListener('click', () => {
    console.log('[shop] Load Items clicked');
    loadShop().catch(e => { console.error(e); say('shop.errors.genericLoadItems'); });
  });

  i18n.registerSwitcher(document.getElementById('langSwitcher'));

  document.addEventListener('i18n:change', () => {
    const cancel = $('cancelPending');
    if (cancel && cancel.style.display !== 'none') {
      setI18nText(cancel, 'shop.buttons.cancel');
    }
    const timerEl = $('qrTimer');
    if (timerEl && timerEl.style.display !== 'none') {
      const paramsAttr = timerEl.getAttribute('data-i18n-params');
      const key = timerEl.getAttribute('data-i18n');
      if (paramsAttr && key) {
        try {
          const params = JSON.parse(paramsAttr);
          setI18nText(timerEl, key, params);
        } catch {}
      } else if (key) {
        setI18nText(timerEl, key);
      }
    }
    const userId = $('userId')?.value?.trim();
    if (userId && !pendingSpend) {
      loadShop().catch(e => { console.error(e); say('shop.errors.genericLoadItems'); });
    }
  });

  console.log('[shop] shop.js loaded');
});
