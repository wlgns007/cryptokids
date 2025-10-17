// ---- language packs (extend as needed) ----
const MESSAGES = {
  en: {
    'app.title': 'CleverKids Admin',
    'tabs.memberMgmt': 'Family Member Management',
    'tabs.registerMember': 'Register New Family Member',
    'tabs.existingMembers': 'Existing Family Members',
    'tabs.issuePoints': 'Issue Points',
    'tabs.holds': 'Holding Rewards To Be Redeemed',
    'tabs.rewardsMenu': 'Rewards Menu',
    'tabs.registerReward': 'Register New Reward',
    'tabs.editEarnMenu': 'Edit Earn Points Menu',
    'tabs.activity': 'Activity',
    nav_member: 'Family Member Management',
    nav_register: 'Register New Family Member',
    nav_existing: 'Existing Family Members',
    member_info: 'Member Info',
    check_balance: 'Check Balance',
    view_history: 'View History',
    user_id: 'User ID',
    name: 'Name',
    dob: 'Date of Birth',
    sex: 'Sex',
    register_member: 'Register Member',
    balance: 'Balance',
    search_placeholder: 'search id or name',
    load_rewards: 'Load Rewards',
    redeem: 'Redeem',
    invalid_youtube: 'Invalid YouTube URL'
  },
  ko: {
    'app.title': '클레버키즈 관리자',
    'tabs.memberMgmt': '가족 구성원 관리',
    'tabs.registerMember': '신규 가족 구성원 등록',
    'tabs.existingMembers': '기존 가족 구성원',
    'tabs.issuePoints': '포인트 지급',
    'tabs.holds': '보류 보상 승인',
    'tabs.rewardsMenu': '리워드 메뉴',
    'tabs.registerReward': '신규 리워드 등록',
    'tabs.editEarnMenu': '적립 메뉴 편집',
    'tabs.activity': '활동',
    nav_member: '가족 구성원 관리',
    nav_register: '신규 가족 구성원 등록',
    nav_existing: '기존 가족 구성원',
    member_info: '회원 정보',
    check_balance: '잔액 확인',
    view_history: '내역 보기',
    user_id: '아이디',
    name: '이름',
    dob: '생년월일',
    sex: '성별',
    register_member: '회원 등록',
    balance: '잔액',
    search_placeholder: '아이디 또는 이름 검색',
    load_rewards: '리워드 불러오기',
    redeem: '사용',
    invalid_youtube: '유효하지 않은 YouTube 주소'
  }
};

// ---- state ----
let CURRENT_LANG = 'en';
try {
  const stored = localStorage.getItem('ck.lang');
  if (stored && Object.prototype.hasOwnProperty.call(MESSAGES, stored)) {
    CURRENT_LANG = stored;
  }
} catch (error) {
  console.warn('Unable to read stored language', error);
}

// ---- core API ----
function t(key) {
  const pack = MESSAGES[CURRENT_LANG] || MESSAGES.en;
  return (pack && Object.prototype.hasOwnProperty.call(pack, key) ? pack[key] : null) || key;
}

function setLang(lang) {
  if (!MESSAGES[lang]) lang = 'en';
  CURRENT_LANG = lang;
  try {
    localStorage.setItem('ck.lang', CURRENT_LANG);
  } catch (error) {
    console.warn('Unable to persist language selection', error);
  }
  applyAdminTranslations();
}

function getLang() {
  return CURRENT_LANG;
}

// Apply translations to anything with [data-i18n] (text or placeholders)
function applyAdminTranslations(root = document) {
  if (!root) return;
  const nodes = root.querySelectorAll('[data-i18n]');
  for (const el of nodes) {
    const key = el.getAttribute('data-i18n');
    const mode = el.getAttribute('data-i18n-attr');
    const value = t(key);
    if (mode === 'placeholder') {
      el.setAttribute('placeholder', value);
    } else if (mode) {
      el.setAttribute(mode, value);
    } else {
      el.textContent = value;
    }
  }
  const placeholderNodes = root.querySelectorAll('[data-i18n-placeholder]');
  for (const el of placeholderNodes) {
    const key = el.getAttribute('data-i18n-placeholder');
    const value = t(key);
    el.setAttribute('placeholder', value);
  }
  if (root === document || root === document.documentElement || root === document.body) {
    document.documentElement.lang = CURRENT_LANG;
  }
}

// ---- expose globally for non-module pages ----
window.ckI18n = { t, setLang, getLang, applyAdminTranslations };
