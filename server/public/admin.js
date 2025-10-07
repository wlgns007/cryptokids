(function () {
  if (window.__CK_ADMIN_READY__) return;
  window.__CK_ADMIN_READY__ = true;

  const ADMIN_KEY_DEFAULT = 'Mamapapa';
  const ADMIN_INVALID_MSG = 'Admin key invalid. Use "Mamapapa" → Save, then retry.';
  const $k = (id) => document.getElementById(id);
  const $ = $k;
  const keyInput = $k('adminKey'); // use current ID
  if (keyInput) {
    keyInput.placeholder = `enter admin key (${ADMIN_KEY_DEFAULT})`;
    const saved = localStorage.getItem('CK_ADMIN_KEY');
    if (!saved) keyInput.value = ADMIN_KEY_DEFAULT;
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

  const ADMIN_KEY_STORAGE = 'CK_ADMIN_KEY';
  function loadAdminKey() {
    return localStorage.getItem(ADMIN_KEY_STORAGE) || '';
  }
  function saveAdminKey(value) {
    localStorage.setItem(ADMIN_KEY_STORAGE, value || '');
  }

  $('saveAdminKey')?.addEventListener('click', () => {
    const value = (keyInput?.value || '').trim();
    saveAdminKey(value);
    toast('Admin key saved');
  });

  document.addEventListener('DOMContentLoaded', () => {
    const saved = loadAdminKey();
    if (saved && keyInput) keyInput.value = saved;
  });

  function getAdminKey(){
    const el = document.getElementById('adminKey');
    return (localStorage.getItem('CK_ADMIN_KEY') || el?.value || '').trim();
  }
  async function adminFetch(url, opts = {}) {
    const headers = { ...(opts.headers||{}), 'x-admin-key': getAdminKey() };
    const res = await fetch(url, { ...opts, headers });
    const ct = res.headers.get('content-type') || '';
    const body = ct.includes('application/json') ? await res.json().catch(()=>({})) : await res.text().catch(()=> '');
    return { res, body };
  }

  function renderQr(elId, text) {
    const el = $(elId);
    if (!el) return;
    el.innerHTML = '';
    if (!text) return;
    new QRCode(el, { text, width: 200, height: 200 });
  }

  const memberIdInput = $('memberUserId');
  const memberStatusEl = $('memberStatus');
  const memberInfoDetails = $('memberInfoDetails');
  const memberTableBody = $('memberTable')?.querySelector('tbody');
  const memberListStatus = $('memberListStatus');
  const memberSearchInput = $('memberSearch');
  const memberListSection = $('memberListSection');
  const memberListCard = $('secMemberList');
  const memberRegisterContainer = $('memberRegisterContainer');
  const memberRegisterFields = $('memberRegisterFields');
  const memberRegisterToggle = $('toggleMemberRegister');

  function setMemberRegisterControlsDisabled(disabled) {
    if (!memberRegisterFields) return;
    const fields = memberRegisterFields.querySelectorAll('input, select, textarea, button');
    fields.forEach((field) => {
      field.disabled = disabled;
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

  function syncMemberRegisterExpanded(expanded) {
    const isExpanded = !!expanded;
    if (memberRegisterToggle) memberRegisterToggle.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
    if (memberRegisterFields) {
      memberRegisterFields.setAttribute('aria-hidden', isExpanded ? 'false' : 'true');
      memberRegisterFields.hidden = !isExpanded;
      memberRegisterFields.style.display = isExpanded ? 'grid' : 'none';
    }
    setMemberRegisterControlsDisabled(!isExpanded);
  }

  function setMemberRegisterExpanded(expanded) {
    const isExpanded = !!expanded;
    if (memberRegisterContainer) memberRegisterContainer.open = isExpanded;
    syncMemberRegisterExpanded(isExpanded);
  }

  syncMemberRegisterExpanded(memberRegisterContainer?.open ?? false);

  memberRegisterContainer?.addEventListener('toggle', () => {
    syncMemberRegisterExpanded(memberRegisterContainer.open);
  });

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
    dobEl.textContent = `DOB: ${member.dob || '—'}`;
    memberInfoDetails.appendChild(dobEl);

    const sexEl = document.createElement('div');
    sexEl.className = 'muted';
    sexEl.textContent = `Sex: ${member.sex || '—'}`;
    memberInfoDetails.appendChild(sexEl);
  }

  function memberIdChanged() {
    normalizeMemberInput();
    setMemberStatus('');
    setMemberInfoMessage('Enter a user ID and click Member Info to view details.');
    loadHolds();
  }

  memberIdInput?.addEventListener('change', memberIdChanged);
  memberIdInput?.addEventListener('blur', normalizeMemberInput);
  memberIdInput?.addEventListener('keyup', (event) => {
    if (event.key === 'Enter') memberIdChanged();
  });

  $('btnMemberInfo')?.addEventListener('click', async () => {
    const userId = requireMemberId();
    if (!userId) return;
    setMemberStatus('');
    setMemberInfoMessage('Loading member info...');
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
      if (member) setMemberStatus(`Loaded member ${member.userId}.`);
    } catch (err) {
      console.error(err);
      setMemberInfoMessage(err.message || 'Failed to load member.');
      toast(err.message || 'Failed to load member', 'error');
    }
  });

  $('btnMemberBalance')?.addEventListener('click', async () => {
    const userId = requireMemberId();
    if (!userId) return;
    setMemberStatus('Fetching balance...');
    try {
      const { res, body } = await adminFetch(`/balance/${encodeURIComponent(userId)}`);
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to fetch balance');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      const balance = Number.isFinite(Number(data.balance)) ? Number(data.balance) : data.balance;
      setMemberStatus(`Balance: ${balance ?? 0} points.`);
    } catch (err) {
      console.error(err);
      setMemberStatus(err.message || 'Failed to fetch balance.');
      toast(err.message || 'Failed to fetch balance', 'error');
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

  async function editMember(member) {
    if (!member) return;
    const namePrompt = prompt('Member name', member.name || '');
    if (namePrompt === null) return;
    const name = namePrompt.trim();
    if (!name) {
      toast('Name required', 'error');
      return;
    }
    const dobPrompt = prompt('Date of birth (YYYY-MM-DD)', member.dob || '');
    if (dobPrompt === null) return;
    const dob = dobPrompt.trim();
    const sexPrompt = prompt('Sex', member.sex || '');
    if (sexPrompt === null) return;
    const sex = sexPrompt.trim();
    try {
      const { res, body } = await adminFetch(`/api/members/${encodeURIComponent(member.userId)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, dob: dob || undefined, sex: sex || undefined })
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
      await loadMembersList();
      const updated = body && typeof body === 'object' ? body.member || body : null;
      if (updated && memberIdInput?.value === updated.userId) {
        renderMemberInfo(updated);
      }
    } catch (err) {
      console.error(err);
      toast(err.message || 'Failed to update member', 'error');
    }
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
      if (memberListStatus) memberListStatus.textContent = 'Search for a member to view results.';
      if (memberListSection) memberListSection.hidden = true;
      if (memberListCard) memberListCard.hidden = true;
      return;
    }
    if (memberListSection) memberListSection.hidden = false;
    if (memberListCard) memberListCard.hidden = false;
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
        dobCell.textContent = row.dob || '';
        tr.appendChild(dobCell);
        const sexCell = document.createElement('td');
        sexCell.textContent = row.sex || '';
        tr.appendChild(sexCell);
        const actions = document.createElement('td');
        actions.style.display = 'flex';
        actions.style.flexWrap = 'wrap';
        actions.style.gap = '6px';
        actions.style.alignItems = 'center';

        const selectBtn = document.createElement('button');
        selectBtn.textContent = 'Select';
        selectBtn.addEventListener('click', () => {
          if (memberIdInput) {
            memberIdInput.value = row.userId;
            memberIdChanged();
          }
        });
        actions.appendChild(selectBtn);

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
      const { res, body } = await adminFetch(`/api/holds?status=${encodeURIComponent(status)}`);
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
      const filtered = rows.filter(row => String(row.userId || '').trim().toLowerCase() === normalizedUser);
      if (!filtered.length) {
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
      for (const row of filtered) {
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
      const count = filtered.length;
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

  async function cancelHold(id) {
    if (!confirm('Cancel this hold?')) return;
    try {
      const { res, body } = await adminFetch(`/api/holds/${id}/cancel`, { method: 'POST' });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'failed');
        throw new Error(msg);
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
  const SHOW_URLS_KEY = 'ck_show_urls';
  (function initToggle() {
    const toggle = $('adminShowUrls');
    if (!toggle) return;
    const saved = localStorage.getItem(SHOW_URLS_KEY);
    const show = saved === '1';
    toggle.checked = show;
    applyUrlToggle(show);
    toggle.addEventListener('change', () => {
      localStorage.setItem(SHOW_URLS_KEY, toggle.checked ? '1' : '0');
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

    const descPrompt = prompt('Description (optional)', item.description || '');
    if (descPrompt === null) return;
    const description = descPrompt.trim();

    const payload = { name, cost, description };
    payload.imageUrl = imageUrl || null;
    updateReward(item.id, payload);
  }

  async function loadRewards() {
    const list = $('rewardsList');
    if (!list) return;
    const statusEl = $('rewardsStatus');
    const filterValue = $('filterRewards')?.value?.toLowerCase?.() || '';
    list.innerHTML = '<div class="muted">Loading...</div>';
    if (statusEl) statusEl.textContent = '';
    try {
      const { res, body } = await adminFetch('/api/rewards');
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        list.innerHTML = '<div class="muted">Admin key invalid.</div>';
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
        active: Number(item.active ?? 1) ? 1 : 0
      }));
      const filtered = normalized.filter(it => !filterValue || it.name.toLowerCase().includes(filterValue));
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

        if (showUrls && item.image_url){
          const div = document.createElement('div');
          div.className = 'muted mono';
          div.textContent = item.image_url;
          card.appendChild(div);
        }

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.flexDirection = 'column';
        actions.style.gap = '6px';
        actions.style.flex = '0 0 auto';
        actions.style.marginLeft = 'auto';

        const editBtn = document.createElement('button');
        editBtn.textContent = 'Edit';
        editBtn.addEventListener('click', () => editReward(item));
        actions.appendChild(editBtn);

        const toggleBtn = document.createElement('button');
        toggleBtn.textContent = item.active ? 'Deactivate' : 'Activate';
        toggleBtn.addEventListener('click', () => updateReward(item.id, { active: item.active ? 0 : 1 }));
        actions.appendChild(toggleBtn);

        card.appendChild(actions);

        list.appendChild(card);
      }
      if (!filtered.length) list.innerHTML = '<div class="muted">No rewards match.</div>';
    } catch (err) {
      const msg = err.message || 'Failed to load rewards';
      if (statusEl) statusEl.textContent = msg;
      if (list) list.innerHTML = `<div class="muted">${msg}</div>`;
    }
  }
  $('btnLoadRewards')?.addEventListener('click', loadRewards);
  $('filterRewards')?.addEventListener('input', loadRewards);

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

  document.getElementById('btnCreateReward')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    const nameEl = document.getElementById('rewardName');
    const costEl = document.getElementById('rewardCost');
    const imageEl = document.getElementById('rewardImage');
    const descEl = document.getElementById('rewardDesc');

    const name = nameEl?.value?.trim() || '';
    const cost = Number(costEl?.value || NaN);
    const imageUrl = imageEl?.value?.trim() || null;
    const description = descEl?.value?.trim() || '';
    if (!name || Number.isNaN(cost)) { toast('Name and numeric cost required', 'error'); return; }

    const { res, body } = await adminFetch('/api/rewards', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ name, cost, imageUrl, description }),
    });

    if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
    if (!res.ok){ toast((typeof body === 'string' ? body : body?.error) || 'Create failed', 'error'); return; }

    toast('Reward created');
    if (nameEl) nameEl.value = '';
    if (costEl) costEl.value = '1';
    if (imageEl) imageEl.value = '';
    if (descEl) descEl.value = '';
    loadRewards?.(); // refresh the list if available
  });

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
      if (!res.ok) {
        const msg = (respBody && respBody.error) || (typeof respBody === 'string' ? respBody : 'update failed');
        throw new Error(msg);
      }
      toast('Template saved');
      loadTemplates();
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
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'quick failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      const amount = data.amount ?? '??';
      const user = data.userId || userId;
      toast(`Awarded ${amount} to ${user}`);
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
      const { res, body } = await adminFetch(`/api/history?${qs}`);
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        historyTable.innerHTML = '<tr><td colspan="11" class="muted">Admin key invalid.</td></tr>';
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

})();

console.info('admin.js loaded ok');
