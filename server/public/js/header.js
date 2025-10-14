let _headerInit = false;
let _deferredPrompt = null; // holds beforeinstallprompt for Android/Chrome

export function bindPWAInstall() {
  // Capture install prompt event once
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    _deferredPrompt = e;
    const btn = document.getElementById('installBtn');
    if (btn) btn.style.display = ''; // reveal when eligible
  });

  // iOS Safari has no beforeinstallprompt; show helper instead
  const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent);
  const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;

  if (isIOS && !isStandalone) {
    const btn = document.getElementById('installBtn');
    if (btn) {
      btn.textContent = 'Add to Home Screen';
      btn.onclick = () => alert('On iPhone: Share ▸ Add to Home Screen');
      btn.style.display = ''; // always show hint on iOS
    }
  }
}

export async function handleInstallClick() {
  if (_deferredPrompt) {
    _deferredPrompt.prompt();
    const choice = await _deferredPrompt.userChoice.catch(() => null);
    _deferredPrompt = null;
  }
}

export function renderHeader({
  mountId = 'app-header',
  langs = ['en','ko'],
  onLangChange,
  variant = 'band',            // 'band' | 'plain'
  showInstall = true,
  rightSlotHTML = ''           // optional HTML string to append on right
} = {}) {
  if (_headerInit) return;
  _headerInit = true;

  const mount = document.getElementById(mountId);
  if (!mount) return;

  const bandClass = variant === 'band' ? ' ck-header--band' : '';
  mount.innerHTML = `
    <header class="ck-header${bandClass}">
      <div class="ck-header-left">
        <a class="ck-brand" href="/">CK WALLET</a>
        <div id="lang-controls" class="ck-lang-wrap"></div>
      </div>
      <div class="ck-header-right">
        ${showInstall ? `<button id="installBtn" class="btn-primary" style="display:none">Install App</button>` : ''}
        ${rightSlotHTML}
      </div>
    </header>
  `;

  // Language chips
  const wrap = mount.querySelector('#lang-controls');
  wrap.innerHTML = '';
  langs.forEach(code => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'chip lang';
    b.textContent = code.toUpperCase();
    b.dataset.lang = code;
    b.addEventListener('click', () => onLangChange && onLangChange(code));
    wrap.appendChild(b);
  });

  // Install button behavior
  const installBtn = document.getElementById('installBtn');
  if (installBtn) {
    installBtn.addEventListener('click', handleInstallClick);
    bindPWAInstall();
  }
}
