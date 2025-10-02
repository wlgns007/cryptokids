// ===== Earn (multi-select) =====
let EARN_TEMPLATES = [];  // [{id,label,amount}]

async function loadEarnTemplates() {
  const res = await fetch('/api/earn-templates');
  EARN_TEMPLATES = await res.json() || [];
  renderEarnList();
  updateEarnSummary();
}

function renderEarnList() {
  const box = $('earnList');
  box.innerHTML = '';
  for (const t of EARN_TEMPLATES) {
    const id = `chk_${t.id}`;
    const row = document.createElement('label');
    row.className = 'earn-item';
    row.innerHTML = `
      <input type="checkbox" id="${id}" data-id="${t.id}" data-amt="${t.amount}">
      <span>${t.label}</span>
      <span class="amt">(+${t.amount} RT)</span>
    `;
    box.appendChild(row);
  }
  // re-calc on any toggle
  box.querySelectorAll('input[type="checkbox"]').forEach(chk => {
    chk.addEventListener('change', updateEarnSummary);
  });
}

function getSelectedTemplateIds() {
  return Array.from(document.querySelectorAll('#earnList input[type="checkbox"]:checked'))
    .map(el => el.dataset.id);
}

function getSelectedTotal() {
  return Array.from(document.querySelectorAll('#earnList input[type="checkbox"]:checked'))
    .reduce((sum, el) => sum + Number(el.dataset.amt || 0), 0);
}

function updateEarnSummary() {
  const total = getSelectedTotal();
  const count = document.querySelectorAll('#earnList input[type="checkbox"]:checked').length;
  $('earnSummary').textContent = `Selected: ${total} RT ${count ? `(${count} task${count>1?'s':''})` : ''}`;
}

async function generateEarnQr() {
  const userId = $('userId')?.value?.trim();
  if (!userId) { alert('Missing user id'); return; }

  const templateIds = getSelectedTemplateIds();
  if (!templateIds.length) { alert('Pick at least one task'); return; }

  // NEW: send an array of templateIds; server will sum securely
  const res = await fetch('/qr/earn', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ userId, templateIds })
  });
  const data = await res.json();
  if (!res.ok) { alert(data.error || 'Failed'); return; }

  renderQrInto('earnQrBox', data.qrUrl || data.url || '');
}

$('btnEarnQr')?.addEventListener('click', generateEarnQr);

// (ensure this still runs on page load)
loadEarnTemplates();


// helper if you want separate boxes
function renderQrInto(elId, text) {
  const el = $(elId);
  el.innerHTML = ''; // clear
  new QRCode(el, { text, width: 200, height: 200 }); // using qrcode.min.js
}

$('btnEarnQr')?.addEventListener('click', generateEarnQr);

// init on load
loadEarnTemplates();
