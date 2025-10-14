let _headerInit = false;
let _deferredPrompt = null;

const IOS_DEVICES = /iPad|iPhone|iPod/;
const IS_IOS = IOS_DEVICES.test(navigator.userAgent);
const IS_STANDALONE = window.matchMedia('(display-mode: standalone)').matches || navigator.standalone;

function applyIosHelper(btn) {
  if (!btn) return;
  if (IS_IOS && !IS_STANDALONE) {
    btn.textContent = 'Add to Home Screen';
    btn.style.display = '';
    btn.onclick = () => alert('On iPhone: Share ▸ Add to Home Screen');
  }
}

function showDeferredPrompt(btn) {
  if (!btn) return;
  if (_deferredPrompt) {
    btn.style.display = '';
  }
}

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

  const headerHtml = `
  <div class="ck-header-outer ${variant === 'band' ? 'ck-header--band' : ''}">
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

  mount.innerHTML = headerHtml;

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

  if (!showInstall) return;

  const installBtn = mount.querySelector('#installBtn');
  showDeferredPrompt(installBtn);
  applyIosHelper(installBtn);
}

window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault();
  _deferredPrompt = e;
  const btn = document.getElementById('installBtn');
  if (btn) btn.style.display = '';
});

document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'installBtn' && _deferredPrompt) {
    _deferredPrompt.prompt();
    _deferredPrompt = null;
  }
});

if (IS_IOS && !IS_STANDALONE) {
  const btn = document.getElementById('installBtn');
  if (btn) {
    btn.textContent = 'Add to Home Screen';
    btn.style.display = '';
    btn.onclick = () => alert('On iPhone: Share ▸ Add to Home Screen');
  }
}
