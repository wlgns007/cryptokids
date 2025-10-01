// --- Earn templates (pre-made by admin) ---
async function loadEarnTemplates() {
  const res = await fetch('/api/earn-templates');   // expects [{id,label,amount}]
  const tpls = await res.json();
  const sel = $('earnTemplate');
  sel.innerHTML = '';
  for (const t of tpls || []) {
    const opt = document.createElement('option');
    opt.value = t.id;
    opt.textContent = `${t.label} (+${t.amount} RT)`;
    opt.dataset.amount = t.amount;
    sel.appendChild(opt);
  }
}

async function generateEarnQr() {
  const userId = $('childId').value.trim();
  if (!userId) { alert('Missing child id'); return; }

  const sel = $('earnTemplate');
  const templateId = sel.value;
  if (!templateId) { alert('Pick a task'); return; }

  const res = await fetch('/qr/earn', {             // returns {qrUrl, token}
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ userId, templateId })
  });
  const data = await res.json();

  // Show QR in a dedicated box (or reuse your standard QR area)
  renderQrInto('earnQrBox', data.qrUrl);
}

// helper if you want separate boxes
function renderQrInto(elId, text) {
  const el = $(elId);
  el.innerHTML = ''; // clear
  new QRCode(el, { text, width: 200, height: 200 }); // using qrcode.min.js
}

$('btnEarnQr')?.addEventListener('click', generateEarnQr);

// init on load
loadEarnTemplates();
