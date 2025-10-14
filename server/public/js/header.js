let _headerBooted = false;

export function renderHeader({
  mountId = 'app-header',
  langs = ['en', 'ko'],
  onLangChange,
  showInstall = true,
  rightSlot
} = {}) {
  if (_headerBooted) return;
  _headerBooted = true;

  const mount = document.getElementById(mountId);
  if (!mount) return;

  mount.innerHTML = `
    <div class="ck-header">
      <div class="ck-header-left">
        <a class="ck-brand" href="/">CK WALLET</a>
        <div id="lang-controls" class="ck-lang-wrap"></div>
      </div>
      <div class="ck-header-right">
        ${showInstall ? `<button id="installBtn" class="btn-primary">Install App</button>` : ''}
      </div>
    </div>
  `;

  const wrap = mount.querySelector('#lang-controls');
  if (!wrap) return;
  wrap.innerHTML = '';

  const storedLang = readStoredLang(langs);
  const buttons = [];

  const updateActive = (active) => {
    buttons.forEach((btn) => {
      const isActive = btn.dataset.lang === active;
      btn.classList.toggle('active', isActive);
      btn.setAttribute('aria-pressed', String(isActive));
    });
  };

  langs.forEach((code) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'chip lang';
    btn.textContent = code.toUpperCase();
    btn.dataset.lang = code;
    btn.setAttribute('aria-pressed', 'false');
    btn.addEventListener('click', () => {
      updateActive(code);
      if (typeof onLangChange === 'function') {
        onLangChange(code);
      }
    });
    buttons.push(btn);
    wrap.appendChild(btn);
  });

  updateActive(storedLang);

  if (rightSlot) {
    const right = mount.querySelector('.ck-header-right');
    if (right) right.appendChild(rightSlot);
  }
}

function readStoredLang(langs) {
  if (!Array.isArray(langs) || !langs.length) return 'en';
  try {
    const value = window.localStorage?.getItem('ck.lang');
    if (value && langs.includes(value)) return value;
  } catch (error) {
    console.warn('[CK Header] unable to read stored language', error);
  }
  return langs[0];
}
