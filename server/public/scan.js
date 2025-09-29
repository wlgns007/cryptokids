(async function () {
  const $ = (id) => document.getElementById(id);
  const status = (msg, cls) => {
    const el = $('status');
    el.className = cls || '';
    el.textContent = msg;
  };

  async function redeem(token) {
    try {
      status('Redeeming...', '');
      const res = await fetch('/redeem', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data?.error || `HTTP ${res.status}`);
      }
      status(`✓ Credited ${data.amount} RT to ${data.userId} (${data.task || 'no task'})`, 'ok');
    } catch (e) {
      status(`✗ ${e.message}`, 'err');
    }
  }

  // Manual fallback
  $('redeemBtn').addEventListener('click', () => {
    const t = $('tokenBox').value.trim();
    if (!t) return status('Paste a token first.', 'err');
    redeem(t);
  });

  // Camera scan
  const readerEl = $('reader');
  const html5QrCode = new Html5Qrcode("reader");

  function onScanSuccess(decodedText) {
    // Throttle to one redeem per scan
    html5QrCode.pause(true);  // pause camera
    redeem(decodedText).finally(() => {
      setTimeout(() => html5QrCode.resume(), 1200); // resume for next scan
    });
  }

  function onScanFailure(_) {
    // ignore per-frame failures (common)
  }

  try {
    // Prefer rear camera
    const devices = await Html5Qrcode.getCameras();
    const rear = devices.find(d => /back|rear|environment/i.test(d.label)) || devices[0];
    if (!rear) throw new Error('No camera found');

    await html5QrCode.start(
      { deviceId: { exact: rear.id } },
      { fps: 10, qrbox: { width: 250, height: 250 } },
      onScanSuccess,
      onScanFailure
    );

    status('Camera ready. Point at the QR.', '');
  } catch (e) {
    status(`Camera not available: ${e.message}. You can paste the token below.`, 'err');
  }
})();
