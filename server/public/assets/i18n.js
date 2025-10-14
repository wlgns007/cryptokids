(function(){
  const translations = {
    en: {
      common: {
        languageLabel: 'Language:',
        languages: {
          en: 'English',
          es: 'Spanish',
          ko: 'Korean'
        }
      },
      index: {
        pageTitle: 'Parents Shop (Web 2.5)',
        heading: 'Parents Shop (Web 2.5)',
        kidLegend: 'Kid',
        pickKid: 'Pick kid:',
        refresh: 'Refresh balance',
        rtBalance: 'RT balance:',
        choresLegend: 'Chores → RT',
        shopLegend: 'Parents Shop',
        badgesLegend: 'Badges (VC)',
        rtAmount: '{amount} RT',
        giveRt: 'Give {amount} RT',
        buy: 'Buy',
        bonus: 'Bonus {amount} RT',
        issue: 'Issue to kid',
        redeem: 'Redeem',
        chores: {
          wakeUp: 'Wake up early',
          brushTeeth: 'Brush teeth',
          makeBed: 'Make bed',
          homework: 'Homework 30m',
          read: 'Read 20m',
          practiceMusic: 'Practice music',
          packBackpack: 'Pack backpack',
          feedPet: 'Feed pet',
          helpDishes: 'Help dishes',
          exercise: 'Exercise'
        },
        items: {
          stickerPack: 'Sticker pack',
          smallToy: 'Small toy',
          extraScreen: 'Extra screen 20m',
          comic: 'Comic',
          cardPack: 'Card pack',
          snack: 'Snack',
          puzzle: 'Puzzle',
          book: 'Book',
          craftKit: 'Craft kit',
          outingPass: 'Outing pass'
        },
        badges: {
          earlyBird: 'Early Bird (7 days)',
          homework: 'Homework streak',
          reading: 'Reading streak',
          teeth: 'Teeth streak',
          kindness: 'Kindness x5'
        }
      },
      shop: {
        pageTitle: 'CryptoKids – Shop',
        heading: 'CryptoKids – Shop',
        userIdPlaceholder: 'Your ID (e.g., leo)',
        empty: {
          title: 'No RT to spend',
          message: 'You can still browse, but items you can’t afford are disabled. Earn RT first to unlock purchases.'
        },
        watchNote: 'Watching for approval… (auto-refreshing balance)',
        messages: {
          loading: 'Loading…',
          balance: 'Balance: {amount} RT',
          noItems: 'No items yet.',
          priceRt: '{amount} RT',
          notEnoughRt: 'Not enough RT. You have {balance}, need {price}.',
          qrCreating: 'Creating approval QR…',
          approvalPrompt: 'Ask your parent to scan to approve: {item} (−{price} RT)',
          approvedBalance: 'Approved. Balance: {amount} RT'
        },
        errors: {
          userIdRequired: 'User ID required',
          loadItems: 'Failed to load items: {message}',
          genericLoadItems: 'Failed to load items',
          requestFailed: 'Request failed',
          createApproval: 'Failed to create approval',
          rewardNotFound: 'Reward not found. Reload and try again.'
        },
        buttons: {
          loadItems: 'Load Items',
          buy: 'Buy',
          notEnoughRt: 'Not enough RT',
          cancel: 'Cancel',
          openApproval: 'Open approval link'
        },
        alerts: {
          pending: 'Please complete or cancel the current approval first.',
          enterIdFirst: 'Enter your ID first'
        },
        qr: {
          rewardLabel: 'Reward: {reward}',
          expired: 'QR expired',
          expiresIn: 'Expires in {minutes}:{seconds}',
          missingLib: 'qrcode.min.js missing'
        }
      }
    },
    es: {
      common: {
        languageLabel: 'Idioma:',
        languages: {
          en: 'Inglés',
          es: 'Español',
          ko: 'Coreano'
        }
      },
      index: {
        pageTitle: 'Tienda para Padres (Web 2.5)',
        heading: 'Tienda para Padres (Web 2.5)',
        kidLegend: 'Hijo',
        pickKid: 'Elige hijo:',
        refresh: 'Actualizar saldo',
        rtBalance: 'Saldo de RT:',
        choresLegend: 'Tareas → RT',
        shopLegend: 'Tienda para Padres',
        badgesLegend: 'Insignias (VC)',
        rtAmount: '{amount} RT',
        giveRt: 'Dar {amount} RT',
        buy: 'Comprar',
        bonus: 'Bono {amount} RT',
        issue: 'Entregar al niño',
        redeem: 'Canjear',
        chores: {
          wakeUp: 'Despiértate temprano',
          brushTeeth: 'Cepíllate los dientes',
          makeBed: 'Arregla la cama',
          homework: 'Tarea 30 min',
          read: 'Leer 20 min',
          practiceMusic: 'Practicar música',
          packBackpack: 'Empacar mochila',
          feedPet: 'Alimenta a la mascota',
          helpDishes: 'Ayuda con los platos',
          exercise: 'Hacer ejercicio'
        },
        items: {
          stickerPack: 'Paquete de pegatinas',
          smallToy: 'Juguete pequeño',
          extraScreen: 'Pantalla extra 20 min',
          comic: 'Cómic',
          cardPack: 'Paquete de cartas',
          snack: 'Bocadillo',
          puzzle: 'Rompecabezas',
          book: 'Libro',
          craftKit: 'Kit de manualidades',
          outingPass: 'Pase de salida'
        },
        badges: {
          earlyBird: 'Madrugador (7 días)',
          homework: 'Racha de tarea',
          reading: 'Racha de lectura',
          teeth: 'Racha de cepillado',
          kindness: 'Bondad x5'
        }
      },
      shop: {
        pageTitle: 'CryptoKids – Tienda',
        heading: 'CryptoKids – Tienda',
        userIdPlaceholder: 'Tu ID (p. ej., leo)',
        empty: {
          title: 'Sin RT para gastar',
          message: 'Puedes explorar, pero los artículos que no puedas pagar estarán deshabilitados. Gana RT primero para desbloquear las compras.'
        },
        watchNote: 'Esperando aprobación… (el saldo se actualiza automáticamente)',
        messages: {
          loading: 'Cargando…',
          balance: 'Saldo: {amount} RT',
          noItems: 'Aún no hay artículos.',
          priceRt: '{amount} RT',
          notEnoughRt: 'RT insuficiente. Tienes {balance} y necesitas {price}.',
          qrCreating: 'Creando QR de aprobación…',
          approvalPrompt: 'Pide a tus padres que escaneen para aprobar: {item} (−{price} RT)',
          approvedBalance: 'Aprobado. Saldo: {amount} RT'
        },
        errors: {
          userIdRequired: 'Se requiere ID de usuario',
          loadItems: 'No se pudieron cargar los artículos: {message}',
          genericLoadItems: 'No se pudieron cargar los artículos',
          requestFailed: 'La solicitud falló',
          createApproval: 'No se pudo crear la aprobación',
          rewardNotFound: 'Recompensa no encontrada. Vuelve a cargar e inténtalo de nuevo.'
        },
        buttons: {
          loadItems: 'Cargar artículos',
          buy: 'Comprar',
          notEnoughRt: 'RT insuficiente',
          cancel: 'Cancelar',
          openApproval: 'Abrir enlace de aprobación'
        },
        alerts: {
          pending: 'Completa o cancela la aprobación actual primero.',
          enterIdFirst: 'Ingresa tu ID primero'
        },
        qr: {
          rewardLabel: 'Recompensa: {reward}',
          expired: 'QR vencido',
          expiresIn: 'Expira en {minutes}:{seconds}',
          missingLib: 'Falta qrcode.min.js'
        }
      }
    },
    ko: {
      common: {
        languageLabel: '언어:',
        languages: {
          en: '영어',
          es: '스페인어',
          ko: '한국어'
        }
      },
      index: {
        pageTitle: '부모님 상점 (웹 2.5)',
        heading: '부모님 상점 (웹 2.5)',
        kidLegend: '아이',
        pickKid: '아이 선택:',
        refresh: '잔액 새로고침',
        rtBalance: 'RT 잔액:',
        choresLegend: '집안일 → RT',
        shopLegend: '부모님 상점',
        badgesLegend: '배지 (VC)',
        rtAmount: '{amount} RT',
        giveRt: '{amount} RT 주기',
        buy: '구매하기',
        bonus: '보너스 {amount} RT',
        issue: '아이에게 발급',
        redeem: '교환하기',
        chores: {
          wakeUp: '일찍 일어나기',
          brushTeeth: '양치하기',
          makeBed: '침대 정리하기',
          homework: '숙제 30분',
          read: '읽기 20분',
          practiceMusic: '악기 연습',
          packBackpack: '가방 챙기기',
          feedPet: '반려동물 먹이 주기',
          helpDishes: '설거지 돕기',
          exercise: '운동하기'
        },
        items: {
          stickerPack: '스티커 세트',
          smallToy: '작은 장난감',
          extraScreen: '추가 화면 20분',
          comic: '만화책',
          cardPack: '카드 팩',
          snack: '간식',
          puzzle: '퍼즐',
          book: '책',
          craftKit: '공예 키트',
          outingPass: '외출 이용권'
        },
        badges: {
          earlyBird: '아침형 인간 (7일)',
          homework: '숙제 연속 달성',
          reading: '독서 연속 달성',
          teeth: '양치 연속 달성',
          kindness: '친절 5회'
        }
      },
      shop: {
        pageTitle: 'CryptoKids – 상점',
        heading: 'CryptoKids – 상점',
        userIdPlaceholder: '내 ID (예: leo)',
        empty: {
          title: '사용할 RT가 없어요',
          message: '둘러볼 수는 있지만 부족한 상품은 비활성화돼요. 먼저 RT를 모아 구매를 해보세요.'
        },
        watchNote: '승인을 확인하는 중… (잔액 자동 새로고침)',
        messages: {
          loading: '불러오는 중…',
          balance: '잔액: {amount} RT',
          noItems: '아직 상품이 없어요.',
          priceRt: '{amount} RT',
          notEnoughRt: 'RT가 부족해요. 현재 {balance} RT이고 {price} RT가 필요해요.',
          qrCreating: '승인 QR을 만드는 중…',
          approvalPrompt: '부모님께 스캔하여 승인받으세요: {item} (−{price} RT)',
          approvedBalance: '승인 완료. 잔액: {amount} RT'
        },
        errors: {
          userIdRequired: '사용자 ID를 입력하세요',
          loadItems: '상품을 불러오지 못했어요: {message}',
          genericLoadItems: '상품을 불러오지 못했어요',
          requestFailed: '요청에 실패했습니다',
          createApproval: '승인 생성을 실패했어요',
          rewardNotFound: '보상을 찾을 수 없어요. 다시 불러와 주세요.'
        },
        buttons: {
          loadItems: '상품 불러오기',
          buy: '구매하기',
          notEnoughRt: 'RT가 부족해요',
          cancel: '취소',
          openApproval: '승인 링크 열기'
        },
        alerts: {
          pending: '현재 승인 절차를 먼저 완료하거나 취소하세요.',
          enterIdFirst: '먼저 ID를 입력하세요.'
        },
        qr: {
          rewardLabel: '보상: {reward}',
          expired: 'QR이 만료됐어요',
          expiresIn: '만료까지 {minutes}:{seconds}',
          missingLib: 'qrcode.min.js가 필요해요'
        }
      }
    }
  };

  const FALLBACK_LANG = 'en';
  const STORAGE_KEY = 'ck-preferred-language';
  let currentLang = FALLBACK_LANG;
  const switchers = new Set();
  const readyCallbacks = [];
  let isReady = false;

  function getNested(lang, key){
    const parts = key.split('.');
    let node = translations[lang];
    for (const part of parts){
      if (node && Object.prototype.hasOwnProperty.call(node, part)){
        node = node[part];
      } else {
        return undefined;
      }
    }
    return node;
  }

  function format(str, params){
    if (!params) return str;
    return str.replace(/\{(\w+)\}/g, (_, name) => {
      return Object.prototype.hasOwnProperty.call(params, name) ? params[name] : `{${name}}`;
    });
  }

  function translate(key, params){
    for (const lang of [currentLang, FALLBACK_LANG]){
      const value = getNested(lang, key);
      if (typeof value === 'string'){
        return format(value, params);
      }
    }
    return key;
  }

  function parseParams(el){
    const value = el.getAttribute('data-i18n-params');
    if (!value) return undefined;
    try {
      return JSON.parse(value);
    } catch {
      return undefined;
    }
  }

  function applyTranslations(root = document){
    root.querySelectorAll('[data-i18n]').forEach((el) => {
      const key = el.getAttribute('data-i18n');
      const params = parseParams(el);
      el.textContent = translate(key, params);
    });
    root.querySelectorAll('[data-i18n-placeholder]').forEach((el) => {
      const key = el.getAttribute('data-i18n-placeholder');
      const params = parseParams(el);
      el.setAttribute('placeholder', translate(key, params));
    });
  }

  function renderSwitcher(el){
    if (!el) return;
    el.innerHTML = '';
    const label = document.createElement('span');
    label.className = 'lang-switcher__label';
    label.textContent = translate('common.languageLabel');
    el.appendChild(label);
    const list = document.createElement('div');
    list.className = 'lang-switcher__buttons';
    for (const lang of Object.keys(translations)){
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = translate(`common.languages.${lang}`);
      btn.className = 'lang-switcher__button';
      btn.dataset.lang = lang;
      if (lang === currentLang) btn.classList.add('is-active');
      btn.onclick = () => setLanguage(lang);
      list.appendChild(btn);
    }
    el.appendChild(list);
  }

  function renderAllSwitchers(){
    switchers.forEach(renderSwitcher);
  }

  function registerSwitcher(el){
    if (!el) return;
    switchers.add(el);
    renderSwitcher(el);
  }

  function setLanguage(lang){
    if (!Object.prototype.hasOwnProperty.call(translations, lang)){
      lang = FALLBACK_LANG;
    }
    currentLang = lang;
    try {
      window.localStorage.setItem(STORAGE_KEY, lang);
    } catch {}
    document.documentElement.lang = lang;
    applyTranslations();
    renderAllSwitchers();
    const event = new CustomEvent('i18n:change', { detail: { lang } });
    document.dispatchEvent(event);
  }

  function getLanguage(){
    return currentLang;
  }

  function ready(callback){
    if (isReady) {
      callback();
      return;
    }
    readyCallbacks.push(callback);
  }

  function init(){
    const stored = (() => {
      try { return window.localStorage.getItem(STORAGE_KEY); } catch { return null; }
    })();
    if (stored && Object.prototype.hasOwnProperty.call(translations, stored)){
      currentLang = stored;
    }
    document.documentElement.lang = currentLang;
    applyTranslations();
    renderAllSwitchers();
    isReady = true;
    for (const cb of readyCallbacks.splice(0, readyCallbacks.length)){
      try { cb(); } catch (err) { console.error(err); }
    }
  }

  window.i18n = {
    t: translate,
    setLanguage,
    getLanguage,
    applyTranslations,
    registerSwitcher,
    ready,
    available: Object.keys(translations)
  };

  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
