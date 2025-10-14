(() => {
  const STRINGS = {
    en: {
      nav_member: "Member Management",
      nav_register: "Register New Member",
      nav_existing: "Existing Members",
      member_info: "Member Info",
      check_balance: "Check Balance",
      view_history: "View History",
      user_id: "User ID",
      name: "Name",
      dob: "Date of Birth",
      sex: "Sex",
      register_member: "Register Member",
      search_placeholder: "search id or name",
      load_rewards: "Load Rewards",
      redeem: "Redeem",
      balance: "Balance",
      invalid_youtube: "Invalid YouTube URL"
    },
    ko: {
      nav_member: "회원 관리",
      nav_register: "신규 회원 등록",
      nav_existing: "기존 회원",
      member_info: "회원 정보",
      check_balance: "잔액 확인",
      view_history: "내역 보기",
      user_id: "아이디",
      name: "이름",
      dob: "생년월일",
      sex: "성별",
      register_member: "회원 등록",
      search_placeholder: "아이디 또는 이름 검색",
      load_rewards: "리워드 불러오기",
      redeem: "사용",
      balance: "잔액",
      invalid_youtube: "유효하지 않은 YouTube 주소"
    }
  };

  const LS = "ck.lang";
  const SUPPORTED = ["en", "ko"];
  let lang = localStorage.getItem(LS) || "en";
  if (!SUPPORTED.includes(lang)) lang = "en";

  function t(key) {
    return (STRINGS[lang] && STRINGS[lang][key]) || key;
  }

  function applyI18n(root = document) {
    root.querySelectorAll("[data-i18n]").forEach(el => {
      const key = el.getAttribute("data-i18n");
      if (key) el.textContent = t(key);
    });
    root.querySelectorAll("[data-i18n-placeholder]").forEach(el => {
      const key = el.getAttribute("data-i18n-placeholder");
      if (key) el.placeholder = t(key);
    });
    document.documentElement.lang = lang;
  }

  function setLang(next) {
    if (!SUPPORTED.includes(next)) next = "en";
    lang = next;
    localStorage.setItem(LS, lang);
    applyI18n();
  }

  window.I18N = { t, setLang, getLang: () => lang, applyI18n, STRINGS, SUPPORTED };
  applyI18n();
})();
