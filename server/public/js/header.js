let _headerInit = false;
let _deferredPrompt = null;

export function renderHeader({
  mountId = 'app-header',
  langs = ['en', 'ko'],
  onLangChange,
  variant = 'band',
  showInstall = true
} = {}) {
  if (_headerInit) return;
  _headerInit = true;

  const mount = document.getElementById(mountId);
  if (!mount) return;

  const bandClass = variant === 'band' ? ' ck-header--band' : '';
  mount.innerHTML = `
    <div class="ck-header-outer${bandClass}">
      <div class="container">
        <header class="ck-header">
          <div class="ck-header-left">
            <a class="ck-brand" href="/">CK WALLET</a>
            <div id="lang-controls" class="ck-lang-wrap"></div>
          </div>
          <div class="ck-header-right">
            ${showInstall ? `<button id="installBtn" class="btn-primary" style="display:none">Install App</button>` : ''}
          </div>
        </header>
      </div>
    </div>
  `;

  const wrap = mount.querySelector('#lang-controls');
  if (wrap) {
    wrap.innerHTML = '';
    langs.forEach((code) => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'chip lang';
      button.textContent = code.toUpperCase();
      button.dataset.lang = code;
      button.setAttribute('aria-pressed', 'false');
      button.addEventListener('click', () => {
        if (typeof onLangChange === 'function') {
          onLangChange(code);
        }
      });
      wrap.appendChild(button);
    });
  }

  const installBtn = mount.querySelector('#installBtn');
  if (!installBtn) return;

  window.addEventListener('beforeinstallprompt', (event) => {
    event.preventDefault();
    _deferredPrompt = event;
    installBtn.style.display = '';
  });

  installBtn.addEventListener('click', async () => {
    if (!_deferredPrompt) return;
    _deferredPrompt.prompt();
    try {
      await _deferredPrompt.userChoice;
    } catch (error) {
      console.warn('PWA install prompt failed', error);
    } finally {
      _deferredPrompt = null;
    }
  });

  const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent);
  const isStandalone = window.matchMedia('(display-mode: standalone)').matches || navigator.standalone;
  if (isIOS && !isStandalone) {
    installBtn.textContent = 'Add to Home Screen';
    installBtn.style.display = '';
    installBtn.onclick = () => alert('On iPhone: Share ▸ Add to Home Screen');
  }
}
