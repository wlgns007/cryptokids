import { renderHeader } from './header.js';

(() => {
  'use strict';

  // ---- constants FIRST (avoid TDZ) ----
  const ADMIN_KEY_STORAGE = 'ck.adminKey';
  const ADMIN_KEY_INPUT_SELECTOR = '#adminKey';
  const ADMIN_KEY_SAVE_SELECTOR = '#saveAdminKey';
  const ADMIN_CONTEXT_STORAGE = 'CK_ADMIN_CONTEXT';
  const ADMIN_INVALID_MSG = 'Admin key invalid.';
  const ADMIN_KEY_REQUIRED_MSG = 'Please enter the adminkey first';
  const SUPPORTED_LANGS = ['en', 'ko'];

  // ---- safe helpers (defined before use) ----
  let adminKeyMemory = '';

  function getAdminKey() {
    try {
      const stored = window.localStorage?.getItem(ADMIN_KEY_STORAGE);
      if (stored != null) {
        adminKeyMemory = stored;
        return stored;
      }
    } catch {
      // ignore storage errors
    }
    return adminKeyMemory || '';
  }

  function setAdminKey(val) {
    const value = val || '';
    let persisted = true;
    try {
      if (value) {
        window.localStorage?.setItem(ADMIN_KEY_STORAGE, value);
      } else {
        window.localStorage?.removeItem(ADMIN_KEY_STORAGE);
      }
    } catch {
      persisted = false;
    }
    adminKeyMemory = value;
    return persisted;
  }

  function loadAdminKey() {
    const el = document.querySelector(ADMIN_KEY_INPUT_SELECTOR);
    if (el) el.value = getAdminKey();
  }

  let ckI18nApi = {};

  function readStoredLang(getLangFn) {
    if (typeof getLangFn === 'function') {
      const current = getLangFn();
      if (SUPPORTED_LANGS.includes(current)) return current;
    }
    try {
      const stored = window.localStorage?.getItem('ck.lang');
      if (stored && SUPPORTED_LANGS.includes(stored)) return stored;
    } catch (error) {
      console.warn('Unable to read stored language', error);
    }
    return SUPPORTED_LANGS[0];
  }

  function translate(key, fallback = key) {
    const translator = ckI18nApi.t || (window.ckI18n && window.ckI18n.t);
    return typeof translator === 'function' ? translator(key) : fallback;
  }

  function syncHeaderLangButtons(active) {
    document
      .querySelectorAll('#lang-controls button[data-lang]')
      .forEach((btn) => {
        const isActive = btn.dataset.lang === active;
        btn.classList.toggle('active', isActive);
        btn.setAttribute('aria-pressed', String(isActive));
      });
  }

  function initI18n() {
    const api = window.ckI18n;
    if (!api) return;

    const { applyAdminTranslations, setLang, getLang, t } = api;
    ckI18nApi = { applyAdminTranslations, setLang, getLang, t };

    if (typeof applyAdminTranslations === 'function') {
      applyAdminTranslations(document);
    }

    const titleEl = document.querySelector('[data-i18n="app.title"]');
    if (titleEl && typeof t === 'function') {
      titleEl.textContent = t('app.title');
    }

    function handleSetLang(lang) {
      const normalized = SUPPORTED_LANGS.includes(lang) ? lang : SUPPORTED_LANGS[0];
      if (typeof setLang === 'function') setLang(normalized);
      syncHeaderLangButtons(normalized);
      return normalized;
    }

    const bindLangButton = (lang) => {
      const btn = document.querySelector(`[data-lang="${lang}"]`);
      if (!btn || btn.dataset.i18nBound) return;
      btn.dataset.i18nBound = 'true';
      btn.addEventListener('click', () => handleSetLang(lang));
    };

    renderHeader({
      mountId: 'app-header',
      langs: SUPPORTED_LANGS,
      onLangChange: handleSetLang,
      variant: 'band',
      showInstall: true
    });

    bindLangButton('en');
    bindLangButton('ko');

    const initialLang = readStoredLang(getLang);
    handleSetLang(initialLang);

    window.setLang = handleSetLang;
  }

  function getYouTubeId(url) {
    if (!url) return '';
    try {
      const u = new URL(url);
      if (u.hostname.includes('youtu.be')) return u.pathname.slice(1);
      if (u.searchParams.has('v')) return u.searchParams.get('v');
      const parts = u.pathname.split('/').filter(Boolean);
      const idx = parts.findIndex((p) => p === 'embed' || p === 'shorts');
      if (idx !== -1 && parts[idx + 1]) return parts[idx + 1];
      return '';
    } catch {
      const m = String(url).match(/(?:v=|\/embed\/|youtu\.be\/|\/shorts\/)([A-Za-z0-9_-]{6,})/);
      return m ? m[1] : '';
    }
  }

  function getYouTubeThumbnail(url) {
    const id = getYouTubeId(url);
    return id ? `https://img.youtube.com/vi/${id}/hqdefault.jpg` : '';
  }

  function getYouTubeEmbed(url, { host = 'www.youtube.com', autoplay = true } = {}) {
    const id = getYouTubeId(url);
    if (!id) return '';
    const params = new URLSearchParams({
      modestbranding: '1',
      rel: '0',
      playsinline: '1'
    });
    if (autoplay) params.set('autoplay', '1');
    return `https://${host}/embed/${id}?${params.toString()}`;
  }

  function isLikelyVerticalYouTube(url) {
    if (!url) return false;
    const base = typeof window !== 'undefined' ? window.location.origin : 'https://youtube.com';
    try {
      const parsed = new URL(url, base);
      if (parsed.pathname?.toLowerCase().includes('/shorts/')) return true;
      for (const value of parsed.searchParams.values()) {
        if (String(value).toLowerCase().includes('shorts')) return true;
      }
      return false;
    } catch {
      return String(url).toLowerCase().includes('shorts');
    }
  }

  window.getYouTubeId = getYouTubeId;
  window.getYouTubeThumbnail = getYouTubeThumbnail;
  window.getYouTubeEmbed = getYouTubeEmbed;
  window.isLikelyVerticalYouTube = isLikelyVerticalYouTube;

  function show(selector) {
    const el = document.querySelector(selector);
    if (el) el.classList.remove('hidden');
  }

  function hide(selector) {
    const el = document.querySelector(selector);
    if (el) el.classList.add('hidden');
  }

  document.addEventListener('click', (event) => {
    const target = event.target.closest('[data-close]');
    if (!target) return;
    const selector = target.getAttribute('data-close');
    if (selector) hide(selector);
  });

function initAdmin() {
  if (window.__CK_ADMIN_READY__) return;
  window.__CK_ADMIN_READY__ = true;

  const $k = (id) => document.getElementById(id);
  const $ = $k;
  const keyInput = $k('adminKey'); // use current ID
  const memoryStore = {};
  const adminState = {
    role: null,
    familyId: null,
    families: [],
    currentFamilyId: null,
    masterView: 'templates',
    showInactiveFamilies: false
  };
  const FAMILY_STATUS_OPTIONS = ['active', 'inactive'];

  const whoamiBanner = $k('adminWhoami');
  const whoamiRoleLabel = $k('adminRoleLabel');
  const scopeBadgeEl = $k('admin-scope-badge');
  const masterToolbar = $k('masterToolbar');
  const familyScopeWrapper = $k('familyScopeWrapper');
  const familyScopeSelect = $k('familyScopeSelect');
  const familyScopeSummary = $k('familyScopeSummary');
  const familySearchForm = $k('familySearchForm');
  const familySearchInput = $k('familySearchInput');
  const familyManagementPanel = $k('familyManagementPanel');
  const familiesRefreshButton = $k('btnFamiliesRefresh');
  const familyListTableBody = $k('familyListTableBody');
  const familyListEmptyRow = $k('familyListEmpty');
  const familyIncludeInactiveToggle = $k('familyIncludeInactive');
  const familyCreateForm = $k('familyCreateForm');
  const familyCreateNameInput = $k('familyCreateName');
  const familyCreateFirstNameInput = $k('familyCreateFirstName');
  const familyCreateLastNameInput = $k('familyCreateLastName');
  const familyCreatePhoneInput = $k('familyCreatePhone');
  const familyCreateIdPreview = $k('familyCreateIdPreview');
  const familyCreateSubmitButton = $k('btnCreateFamily');
  const newFamilyButton = $k('btn-new-family');
  const forgotKeyButton = $k('btn-forgot-key');
  const newFamilyCreateButton = $k('nf-create');
  const forgotKeySendButton = $k('fk-send');
  const roleVisibilityNodes = Array.from(document.querySelectorAll('[data-admin-role]'));
  const pendingBanner = $k('pendingTemplatesBanner');
  const pendingList = $k('pendingTemplatesList');
  const pendingCount = $k('pendingTemplatesCount');
  const pendingStatus = $k('pendingTemplatesStatus');
  const pendingRefreshButton = $k('pendingTemplatesRefresh');

  const masterViewTabs = Array.from(document.querySelectorAll('[data-view-tab]'));
  const masterViewPanels = Array.from(document.querySelectorAll('[data-master-view]'));
  const scopeNotice = $k('masterScopeNotice');
  const scopeDependentSections = Array.from(document.querySelectorAll('[data-requires-scope="true"]'));
  const templateTabs = Array.from(document.querySelectorAll('[data-template-tab]'));
  const templatePanels = Array.from(document.querySelectorAll('[data-template-panel]'));

  masterViewTabs.forEach((btn) => {
    if (!btn) return;
    const view = btn.dataset.viewTab || '';
    btn.addEventListener('click', () => {
      if (btn.disabled) return;
      setMasterView(view);
    });
  });

  const masterTaskForm = $k('masterTaskForm');
  const masterTaskTitle = $k('masterTaskTitle');
  const masterTaskDescription = $k('masterTaskDescription');
  const masterTaskIcon = $k('masterTaskIcon');
  const masterTaskYoutube = $k('masterTaskYoutube');
  const masterTaskPoints = $k('masterTaskPoints');
  const masterTaskStatusSelect = $k('masterTaskStatusSelect');
  const masterTaskSubmit = $k('masterTaskSubmit');
  const masterTaskReset = $k('masterTaskReset');
  const masterTaskFormHint = $k('masterTaskFormHint');
  const masterTaskFilter = $k('masterTaskFilter');
  const masterTaskRefresh = $k('masterTaskRefresh');
  const masterTaskTableBody = $k('masterTasksTableBody');
  const masterTasksEmpty = $k('masterTasksEmpty');
  const masterTaskStatus = $k('masterTaskStatus');

  const masterRewardForm = $k('masterRewardForm');
  const masterRewardTitle = $k('masterRewardTitle');
  const masterRewardDescription = $k('masterRewardDescription');
  const masterRewardIcon = $k('masterRewardIcon');
  const masterRewardYoutube = $k('masterRewardYoutube');
  const masterRewardCost = $k('masterRewardCost');
  const masterRewardStatusSelect = $k('masterRewardStatusSelect');
  const masterRewardSubmit = $k('masterRewardSubmit');
  const masterRewardReset = $k('masterRewardReset');
  const masterRewardFormHint = $k('masterRewardFormHint');
  const masterRewardFilter = $k('masterRewardFilter');
  const masterRewardRefresh = $k('masterRewardRefresh');
  const masterRewardsTableBody = $k('masterRewardsTableBody');
  const masterRewardsEmpty = $k('masterRewardsEmpty');
  const masterRewardStatus = $k('masterRewardStatus');

  const pendingTemplatesState = {
    items: [],
    loading: false,
    error: ''
  };

  const masterTemplatesState = {
    tasks: [],
    rewards: [],
    loadingTasks: false,
    loadingRewards: false,
    activeTab: 'tasks',
    editing: { task: null, reward: null }
  };

  function canShowPendingBanner() {
    return (adminState.role === 'master' || adminState.role === 'family') && Boolean(adminState.currentFamilyId);
  }

  function normalizeMasterStatus(value, fallback = 'active') {
    const normalized = (value || '').toString().trim().toLowerCase();
    return normalized === 'inactive' ? 'inactive' : fallback;
  }

  function normalizeScopeId(value) {
    if (value === undefined || value === null) return null;
    const text = String(value).trim();
    if (!text) return null;
    return text.toLowerCase() === 'default' ? null : text;
  }

  function applyMasterViewVisibility() {
    const isMaster = adminState.role === 'master';
    masterViewTabs.forEach((btn) => {
      if (!btn) return;
      const view = btn.dataset.viewTab || '';
      const isActive = isMaster && view === adminState.masterView;
      btn.classList.toggle('is-active', isActive);
      btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
      btn.hidden = !isMaster;
      if (!isMaster) {
        btn.disabled = false;
      }
    });
    masterViewPanels.forEach((panel) => {
      if (!panel) return;
      const view = panel.dataset.masterView || '';
      const hide = isMaster ? view !== adminState.masterView : false;
      panel.classList.toggle('is-inactive-view', hide);
    });
  }

  function setMasterView(view) {
    const desired = view === 'families' ? 'families' : 'templates';
    if (adminState.masterView !== desired) {
      adminState.masterView = desired;
    }
    applyMasterViewVisibility();
    if (adminState.role === 'master') {
      saveAdminContext({
        role: adminState.role,
        family_id: adminState.familyId,
        currentFamilyId: adminState.currentFamilyId,
        masterView: adminState.masterView,
        showInactiveFamilies: adminState.showInactiveFamilies
      });
    }
  }

  function updateMasterScopeVisibility() {
    const isMaster = adminState.role === 'master';
    const hasScope = Boolean(adminState.currentFamilyId);
    if (scopeNotice) {
      scopeNotice.hidden = !(isMaster && !hasScope);
    }
    scopeDependentSections.forEach((section) => {
      if (!section) return;
      section.classList.toggle('is-scope-disabled', isMaster && !hasScope);
    });
    const familiesTab = masterViewTabs.find((btn) => btn?.dataset.viewTab === 'families');
    if (familiesTab) {
      familiesTab.disabled = isMaster && !hasScope;
    }
    const resetView = isMaster && !hasScope && adminState.masterView === 'families';
    if (resetView) {
      setMasterView('templates');
    } else {
      applyMasterViewVisibility();
    }
  }

  function setMasterTemplatesTab(tab) {
    const desired = tab === 'rewards' ? 'rewards' : 'tasks';
    masterTemplatesState.activeTab = desired;
    templateTabs.forEach((btn) => {
      if (!btn) return;
      const tabName = btn.dataset.templateTab || 'tasks';
      const isActive = tabName === desired;
      btn.classList.toggle('is-active', isActive);
      btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
    });
    templatePanels.forEach((panel) => {
      if (!panel) return;
      const tabName = panel.dataset.templatePanel || 'tasks';
      panel.hidden = tabName !== desired;
    });
  }

  function resetMasterTaskForm() {
    if (masterTaskForm) masterTaskForm.reset();
    if (masterTaskStatusSelect) masterTaskStatusSelect.value = 'active';
    if (masterTaskPoints) masterTaskPoints.value = '0';
    if (masterTaskIcon) masterTaskIcon.value = '';
    if (masterTaskDescription) masterTaskDescription.value = '';
    if (masterTaskYoutube) masterTaskYoutube.value = '';
    masterTemplatesState.editing.task = null;
    if (masterTaskSubmit) masterTaskSubmit.textContent = 'Create Task Template';
    if (masterTaskFormHint) masterTaskFormHint.textContent = 'Create a new template or select one below to edit.';
  }

  function resetMasterRewardForm() {
    if (masterRewardForm) masterRewardForm.reset();
    if (masterRewardStatusSelect) masterRewardStatusSelect.value = 'active';
    if (masterRewardCost) masterRewardCost.value = '0';
    if (masterRewardIcon) masterRewardIcon.value = '';
    if (masterRewardDescription) masterRewardDescription.value = '';
    if (masterRewardYoutube) masterRewardYoutube.value = '';
    masterTemplatesState.editing.reward = null;
    if (masterRewardSubmit) masterRewardSubmit.textContent = 'Create Reward Template';
    if (masterRewardFormHint) masterRewardFormHint.textContent = 'Create a new template or select one below to edit.';
  }

  function renderPendingTemplates() {
    if (!pendingBanner) return;
    const { items = [], loading, error } = pendingTemplatesState;
    const canShow = canShowPendingBanner() && (loading || error || (Array.isArray(items) && items.length > 0));
    pendingBanner.hidden = !canShow;
    if (pendingCount) pendingCount.textContent = String(Array.isArray(items) ? items.length : 0);
    if (!canShow) {
      if (pendingStatus) pendingStatus.textContent = loading ? 'Loading templates…' : '';
      if (pendingList) pendingList.innerHTML = '';
      return;
    }
    if (pendingStatus) {
      if (loading) {
        pendingStatus.textContent = 'Loading templates…';
      } else if (error) {
        pendingStatus.textContent = error;
      } else if (!items.length) {
        pendingStatus.textContent = 'You’re all caught up.';
      } else {
        pendingStatus.textContent = '';
      }
    }
    if (!pendingList) return;
    pendingList.innerHTML = '';
    if (!Array.isArray(items) || !items.length) return;

    for (const item of items) {
      if (!item) continue;
      const li = document.createElement('li');
      li.className = 'pending-template-item';

      const info = document.createElement('div');
      info.className = 'pending-template-info';

      const title = document.createElement('span');
      title.className = 'pending-template-title';
      title.textContent = item.title || 'Untitled template';
      info.appendChild(title);

      const meta = document.createElement('span');
      meta.className = 'pending-template-meta';
      const kindLabel = item.kind === 'reward' ? 'Reward' : 'Task';
      const valueLabel = item.kind === 'reward'
        ? `${Number(item.base_cost ?? 0) || 0} points`
        : `${Number(item.base_points ?? 0) || 0} points`;
      meta.textContent = `${kindLabel} • ${valueLabel}`;
      info.appendChild(meta);

      if (item.description) {
        const desc = document.createElement('div');
        desc.className = 'pending-template-description';
        desc.textContent = item.description;
        info.appendChild(desc);
      }

      li.appendChild(info);

      const actions = document.createElement('div');
      actions.className = 'pending-template-actions';

      const adoptBtn = document.createElement('button');
      adoptBtn.type = 'button';
      adoptBtn.className = 'btn-primary';
      adoptBtn.textContent = 'Adopt';
      adoptBtn.addEventListener('click', () => handleAdoptPending(item, adoptBtn));
      actions.appendChild(adoptBtn);

      const dismissBtn = document.createElement('button');
      dismissBtn.type = 'button';
      dismissBtn.className = 'btn';
      dismissBtn.textContent = 'Dismiss';
      dismissBtn.addEventListener('click', () => handleDismissPending(item, dismissBtn));
      actions.appendChild(dismissBtn);

      li.appendChild(actions);

      pendingList.appendChild(li);
    }
  }

  async function loadPendingTemplates({ silent = false } = {}) {
    if (!pendingBanner || !pendingList) return;
    if (!canShowPendingBanner()) {
      pendingTemplatesState.items = [];
      pendingTemplatesState.error = '';
      pendingTemplatesState.loading = false;
      renderPendingTemplates();
      return;
    }
    pendingTemplatesState.loading = true;
    pendingTemplatesState.error = '';
    renderPendingTemplates();
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      pendingTemplatesState.loading = false;
      pendingTemplatesState.items = [];
      pendingTemplatesState.error = '';
      renderPendingTemplates();
      return;
    }
    try {
      const url = appendFamilyQuery('/api/family/pending/templates', familyId);
      const { res, body } = await adminFetch(url);
      if (res.status === 401) {
        pendingTemplatesState.items = [];
        pendingTemplatesState.error = ADMIN_INVALID_MSG;
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Failed to load pending templates');
        throw new Error(msg);
      }
      const list = Array.isArray(body?.items) ? body.items : [];
      pendingTemplatesState.items = list.map((entry) => ({
        kind: entry.kind === 'reward' ? 'reward' : 'task',
        master_id: entry.master_id,
        title: entry.title || '',
        description: entry.description || null,
        icon: entry.icon || null,
        base_points: Number(entry.base_points ?? 0) || 0,
        base_cost: Number(entry.base_cost ?? 0) || 0
      }));
    } catch (error) {
      pendingTemplatesState.items = [];
      pendingTemplatesState.error = error.message || 'Unable to load pending templates.';
      if (!silent) toast(pendingTemplatesState.error, 'error');
    } finally {
      pendingTemplatesState.loading = false;
      renderPendingTemplates();
    }
  }

  async function handleAdoptPending(item, trigger) {
    if (!item || !item.kind || !item.master_id) return;
    if (trigger) trigger.disabled = true;
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      if (trigger) trigger.disabled = false;
      toast('Select a family scope to adopt templates.', 'error');
      return;
    }
    try {
      const url = appendFamilyQuery('/api/family/adopt', familyId);
      const payload = withFamilyInBody({ kind: item.kind, master_id: item.master_id }, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Adoption failed');
        throw new Error(msg);
      }
      toast(item.kind === 'reward' ? 'Reward template adopted' : 'Task template adopted');
      await loadPendingTemplates({ silent: true });
      if (item.kind === 'reward') {
        if (typeof loadRewards === 'function') {
          await loadRewards();
        }
      } else if (typeof loadTemplates === 'function') {
        await loadTemplates();
      }
    } catch (error) {
      toast(error.message || 'Adoption failed', 'error');
    } finally {
      if (trigger) trigger.disabled = false;
    }
  }

  async function handleDismissPending(item, trigger) {
    if (!item || !item.kind || !item.master_id) return;
    if (trigger) trigger.disabled = true;
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      if (trigger) trigger.disabled = false;
      toast('Select a family scope to dismiss templates.', 'error');
      return;
    }
    try {
      const url = appendFamilyQuery('/api/family/dismiss', familyId);
      const payload = withFamilyInBody({ kind: item.kind, master_id: item.master_id }, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (!res.ok && res.status !== 204) {
        const msg = presentError(body?.error, 'Dismiss failed');
        throw new Error(msg);
      }
      toast(item.kind === 'reward' ? 'Reward dismissed' : 'Task dismissed', 'info');
      await loadPendingTemplates({ silent: true });
    } catch (error) {
      toast(error.message || 'Dismiss failed', 'error');
    } finally {
      if (trigger) trigger.disabled = false;
    }
  }

  function renderMasterTaskList() {
    if (!masterTaskTableBody) return;
    const all = Array.isArray(masterTemplatesState.tasks) ? masterTemplatesState.tasks : [];
    const filter = (masterTaskFilter?.value || 'all').toLowerCase();
    const rows = all.filter((item) => {
      const status = (item.status || 'active').toLowerCase();
      if (filter === 'active') return status === 'active';
      if (filter === 'inactive') return status === 'inactive';
      return true;
    });

    masterTaskTableBody.innerHTML = '';
    if (rows.length === 0) {
      if (masterTasksEmpty) masterTasksEmpty.style.display = '';
    } else if (masterTasksEmpty) {
      masterTasksEmpty.style.display = 'none';
    }

    for (const item of rows) {
      if (!item) continue;
      const tr = document.createElement('tr');
      tr.className = 'master-template-row';
      if (masterTemplatesState.editing.task && masterTemplatesState.editing.task === item.id) {
        tr.classList.add('is-editing');
      }

      const idCell = document.createElement('td');
      idCell.textContent = item.id || '';
      tr.appendChild(idCell);

      const titleCell = document.createElement('td');
      titleCell.textContent = item.title || '';
      tr.appendChild(titleCell);

      const pointsCell = document.createElement('td');
      pointsCell.textContent = `${Number(item.base_points ?? 0) || 0}`;
      tr.appendChild(pointsCell);

      const statusCell = document.createElement('td');
      statusCell.textContent = (item.status || 'active').toLowerCase() === 'inactive' ? 'Inactive' : 'Active';
      tr.appendChild(statusCell);

      const actionsCell = document.createElement('td');
      const editBtn = document.createElement('button');
      editBtn.type = 'button';
      editBtn.className = 'btn';
      editBtn.textContent = 'Edit';
      editBtn.addEventListener('click', () => startEditMasterTask(item));
      actionsCell.appendChild(editBtn);
      tr.appendChild(actionsCell);

      masterTaskTableBody.appendChild(tr);
    }

    if (masterTaskStatus) {
      if (masterTemplatesState.loadingTasks) {
        masterTaskStatus.textContent = 'Loading templates…';
      } else if (rows.length === 0) {
        masterTaskStatus.textContent = all.length ? 'No templates match this filter.' : 'No templates yet.';
      } else {
        masterTaskStatus.textContent = `Showing ${rows.length} template${rows.length === 1 ? '' : 's'}.`;
      }
    }
  }

  function renderMasterRewardList() {
    if (!masterRewardsTableBody) return;
    const all = Array.isArray(masterTemplatesState.rewards) ? masterTemplatesState.rewards : [];
    const filter = (masterRewardFilter?.value || 'all').toLowerCase();
    const rows = all.filter((item) => {
      const status = (item.status || 'active').toLowerCase();
      if (filter === 'active') return status === 'active';
      if (filter === 'inactive') return status === 'inactive';
      return true;
    });

    masterRewardsTableBody.innerHTML = '';
    if (rows.length === 0) {
      if (masterRewardsEmpty) masterRewardsEmpty.style.display = '';
    } else if (masterRewardsEmpty) {
      masterRewardsEmpty.style.display = 'none';
    }

    for (const item of rows) {
      if (!item) continue;
      const tr = document.createElement('tr');
      tr.className = 'master-template-row';
      if (masterTemplatesState.editing.reward && masterTemplatesState.editing.reward === item.id) {
        tr.classList.add('is-editing');
      }

      const idCell = document.createElement('td');
      idCell.textContent = item.id || '';
      tr.appendChild(idCell);

      const titleCell = document.createElement('td');
      titleCell.textContent = item.title || '';
      tr.appendChild(titleCell);

      const costCell = document.createElement('td');
      costCell.textContent = `${Number(item.base_cost ?? 0) || 0}`;
      tr.appendChild(costCell);

      const statusCell = document.createElement('td');
      statusCell.textContent = (item.status || 'active').toLowerCase() === 'inactive' ? 'Inactive' : 'Active';
      tr.appendChild(statusCell);

      const actionsCell = document.createElement('td');
      const editBtn = document.createElement('button');
      editBtn.type = 'button';
      editBtn.className = 'btn';
      editBtn.textContent = 'Edit';
      editBtn.addEventListener('click', () => startEditMasterReward(item));
      actionsCell.appendChild(editBtn);
      tr.appendChild(actionsCell);

      masterRewardsTableBody.appendChild(tr);
    }

    if (masterRewardStatus) {
      if (masterTemplatesState.loadingRewards) {
        masterRewardStatus.textContent = 'Loading templates…';
      } else if (rows.length === 0) {
        masterRewardStatus.textContent = all.length ? 'No templates match this filter.' : 'No templates yet.';
      } else {
        masterRewardStatus.textContent = `Showing ${rows.length} template${rows.length === 1 ? '' : 's'}.`;
      }
    }
  }

  function startEditMasterTask(item) {
    if (!item) return;
    if (masterTaskTitle) masterTaskTitle.value = item.title || '';
    if (masterTaskDescription) masterTaskDescription.value = item.description || '';
    if (masterTaskIcon) masterTaskIcon.value = item.icon || '';
    if (masterTaskYoutube) masterTaskYoutube.value = item.youtube_url || '';
    if (masterTaskPoints) masterTaskPoints.value = String(Number(item.base_points ?? 0) || 0);
    if (masterTaskStatusSelect) masterTaskStatusSelect.value = normalizeMasterStatus(item.status);
    masterTemplatesState.editing.task = item.id || null;
    if (masterTaskSubmit) masterTaskSubmit.textContent = 'Update Task Template';
    if (masterTaskFormHint) masterTaskFormHint.textContent = `Editing template ${item.title || item.id || ''}`.trim();
    setMasterTemplatesTab('tasks');
  }

  function startEditMasterReward(item) {
    if (!item) return;
    if (masterRewardTitle) masterRewardTitle.value = item.title || '';
    if (masterRewardDescription) masterRewardDescription.value = item.description || '';
    if (masterRewardIcon) masterRewardIcon.value = item.icon || '';
    if (masterRewardYoutube) masterRewardYoutube.value = item.youtube_url || '';
    if (masterRewardCost) masterRewardCost.value = String(Number(item.base_cost ?? 0) || 0);
    if (masterRewardStatusSelect) masterRewardStatusSelect.value = normalizeMasterStatus(item.status);
    masterTemplatesState.editing.reward = item.id || null;
    if (masterRewardSubmit) masterRewardSubmit.textContent = 'Update Reward Template';
    if (masterRewardFormHint) masterRewardFormHint.textContent = `Editing template ${item.title || item.id || ''}`.trim();
    setMasterTemplatesTab('rewards');
  }

  async function loadMasterTasks({ silent = false } = {}) {
    if (adminState.role !== 'master') {
      masterTemplatesState.tasks = [];
      masterTemplatesState.loadingTasks = false;
      renderMasterTaskList();
      return;
    }
    masterTemplatesState.loadingTasks = true;
    renderMasterTaskList();
    try {
      const { res, body } = await adminFetch('/api/master/tasks', { skipScope: true });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        masterTemplatesState.tasks = [];
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Failed to load templates');
        throw new Error(msg);
      }
      const items = Array.isArray(body?.items) ? body.items : [];
      masterTemplatesState.tasks = items;
    } catch (error) {
      masterTemplatesState.tasks = [];
      if (!silent) toast(error.message || 'Failed to load templates', 'error');
      if (masterTaskStatus) masterTaskStatus.textContent = error.message || 'Failed to load templates.';
    } finally {
      masterTemplatesState.loadingTasks = false;
      renderMasterTaskList();
    }
  }

  async function loadMasterRewards({ silent = false } = {}) {
    if (adminState.role !== 'master') {
      masterTemplatesState.rewards = [];
      masterTemplatesState.loadingRewards = false;
      renderMasterRewardList();
      return;
    }
    masterTemplatesState.loadingRewards = true;
    renderMasterRewardList();
    try {
      const { res, body } = await adminFetch('/api/master/rewards', { skipScope: true });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        masterTemplatesState.rewards = [];
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Failed to load templates');
        throw new Error(msg);
      }
      const items = Array.isArray(body?.items) ? body.items : [];
      masterTemplatesState.rewards = items;
    } catch (error) {
      masterTemplatesState.rewards = [];
      if (!silent) toast(error.message || 'Failed to load templates', 'error');
      if (masterRewardStatus) masterRewardStatus.textContent = error.message || 'Failed to load templates.';
    } finally {
      masterTemplatesState.loadingRewards = false;
      renderMasterRewardList();
    }
  }

  window.CKPWA?.initAppShell({
    swVersion: '1.0.0',
    installButtonSelector: '#installBtn'
  });

  if (pendingRefreshButton) {
    pendingRefreshButton.addEventListener('click', () => {
      loadPendingTemplates();
    });
  }

  setMasterTemplatesTab(masterTemplatesState.activeTab);

  templateTabs.forEach((btn) => {
    if (!btn) return;
    btn.addEventListener('click', () => setMasterTemplatesTab(btn.dataset.templateTab || 'tasks'));
  });

  masterTaskFilter?.addEventListener('change', () => renderMasterTaskList());
  masterRewardFilter?.addEventListener('change', () => renderMasterRewardList());
  masterTaskRefresh?.addEventListener('click', () => loadMasterTasks());
  masterRewardRefresh?.addEventListener('click', () => loadMasterRewards());
  masterTaskReset?.addEventListener('click', () => resetMasterTaskForm());
  masterRewardReset?.addEventListener('click', () => resetMasterRewardForm());

  masterTaskForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const title = (masterTaskTitle?.value || '').trim();
    if (!title) {
      toast('Title is required', 'error');
      masterTaskTitle?.focus();
      return;
    }
    const basePointsValue = Number(masterTaskPoints?.value ?? 0);
    if (!Number.isFinite(basePointsValue) || basePointsValue < 0) {
      toast('Base points must be zero or greater', 'error');
      masterTaskPoints?.focus();
      return;
    }
    const payload = {
      title,
      base_points: Math.trunc(basePointsValue),
      description: (masterTaskDescription?.value || '').trim() || null,
      icon: (masterTaskIcon?.value || '').trim() || null,
      youtube_url: (masterTaskYoutube?.value || '').trim() || null,
      status: normalizeMasterStatus(masterTaskStatusSelect?.value || 'active')
    };
    if (!payload.description) payload.description = null;
    if (!payload.icon) payload.icon = null;
    if (!payload.youtube_url) payload.youtube_url = null;

    const editingId = masterTemplatesState.editing.task;
    try {
      if (editingId) {
        const { res, body } = await adminFetch(`/api/master/tasks/${editingId}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          skipScope: true
        });
        if (res.status === 401) {
          toast(ADMIN_INVALID_MSG, 'error');
          return;
        }
        if (!res.ok) {
          const msg = presentError(body?.error, 'Update failed');
          throw new Error(msg);
        }
        toast('Task template updated');
      } else {
        const { res, body } = await adminFetch('/api/master/tasks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          skipScope: true
        });
        if (res.status === 401) {
          toast(ADMIN_INVALID_MSG, 'error');
          return;
        }
        if (!res.ok) {
          const msg = presentError(body?.error, 'Create failed');
          throw new Error(msg);
        }
        toast('Task template created');
      }
      resetMasterTaskForm();
      await loadMasterTasks({ silent: true });
      await loadPendingTemplates({ silent: true });
    } catch (error) {
      toast(error.message || 'Save failed', 'error');
    }
  });

  masterRewardForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const title = (masterRewardTitle?.value || '').trim();
    if (!title) {
      toast('Title is required', 'error');
      masterRewardTitle?.focus();
      return;
    }
    const baseCostValue = Number(masterRewardCost?.value ?? 0);
    if (!Number.isFinite(baseCostValue) || baseCostValue < 0) {
      toast('Base cost must be zero or greater', 'error');
      masterRewardCost?.focus();
      return;
    }
    const payload = {
      title,
      base_cost: Math.trunc(baseCostValue),
      description: (masterRewardDescription?.value || '').trim() || null,
      icon: (masterRewardIcon?.value || '').trim() || null,
      youtube_url: (masterRewardYoutube?.value || '').trim() || null,
      status: normalizeMasterStatus(masterRewardStatusSelect?.value || 'active')
    };
    if (!payload.description) payload.description = null;
    if (!payload.icon) payload.icon = null;
    if (!payload.youtube_url) payload.youtube_url = null;

    const editingId = masterTemplatesState.editing.reward;
    try {
      if (editingId) {
        const { res, body } = await adminFetch(`/api/master/rewards/${editingId}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          skipScope: true
        });
        if (res.status === 401) {
          toast(ADMIN_INVALID_MSG, 'error');
          return;
        }
        if (!res.ok) {
          const msg = presentError(body?.error, 'Update failed');
          throw new Error(msg);
        }
        toast('Reward template updated');
      } else {
        const { res, body } = await adminFetch('/api/master/rewards', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          skipScope: true
        });
        if (res.status === 401) {
          toast(ADMIN_INVALID_MSG, 'error');
          return;
        }
        if (!res.ok) {
          const msg = presentError(body?.error, 'Create failed');
          throw new Error(msg);
        }
        toast('Reward template created');
      }
      resetMasterRewardForm();
      await loadMasterRewards({ silent: true });
      await loadPendingTemplates({ silent: true });
    } catch (error) {
      toast(error.message || 'Save failed', 'error');
    }
  });

  renderMasterTaskList();
  renderMasterRewardList();
  renderPendingTemplates();

  function storageGet(key) {
    try {
      const value = window.localStorage.getItem(key);
      if (value != null) memoryStore[key] = value;
      return value;
    } catch (error) {
      console.warn('localStorage getItem failed', error);
      return Object.prototype.hasOwnProperty.call(memoryStore, key) ? memoryStore[key] : null;
    }
  }

  function storageSet(key, value) {
    let ok = true;
    try {
      window.localStorage.setItem(key, value);
    } catch (error) {
      console.warn('localStorage setItem failed', error);
      ok = false;
    }
    memoryStore[key] = value;
    return ok;
  }

  function storageRemove(key) {
    let ok = true;
    try {
      window.localStorage.removeItem(key);
    } catch (error) {
      console.warn('localStorage removeItem failed', error);
      ok = false;
    }
    delete memoryStore[key];
    return ok;
  }

  function loadAdminContext() {
    try {
      const raw = storageGet(ADMIN_CONTEXT_STORAGE);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') return null;
      return {
        role: parsed.role ?? null,
        family_id: normalizeScopeId(parsed.family_id),
        currentFamilyId: normalizeScopeId(parsed.currentFamilyId),
        masterView: parsed.masterView === 'families' ? 'families' : 'templates'
      };
    } catch (error) {
      console.warn('Unable to parse stored admin context', error);
      return null;
    }
  }

  function saveAdminContext(context) {
    if (!context || (context.role == null && context.family_id == null && context.currentFamilyId == null)) {
      storageRemove(ADMIN_CONTEXT_STORAGE);
      return;
    }
    try {
      storageSet(ADMIN_CONTEXT_STORAGE, JSON.stringify(context));
    } catch (error) {
      console.warn('Unable to persist admin context', error);
    }
  }

  function applyRoleVisibility() {
    const activeRole = adminState.role || '';
    const hasScope = Boolean(adminState.currentFamilyId);
    if (familySearchInput) familySearchInput.disabled = activeRole !== 'master';
    const searchSubmit = familySearchForm?.querySelector('button[type="submit"]');
    if (searchSubmit) searchSubmit.disabled = activeRole !== 'master';
    if (familiesRefreshButton) familiesRefreshButton.disabled = activeRole !== 'master';
    for (const node of roleVisibilityNodes) {
      if (!node) continue;
      const roles = (node.dataset.adminRole || '')
        .split(',')
        .map((role) => role.trim())
        .filter(Boolean);
      const show = roles.length === 0 || roles.includes(activeRole);
      node.hidden = !show;
    }

    if (!activeRole) {
      applyMasterViewVisibility();
      return;
    }

    if (activeRole !== 'master') {
      applyMasterViewVisibility();
      return;
    }

    updateFamilyCreateButtonState();
    applyMasterViewVisibility();
  }

  applyRoleVisibility();
  setMasterView('templates');

  function clearAdminContext() {
    adminState.role = null;
    adminState.familyId = null;
    adminState.families = [];
    adminState.currentFamilyId = null;
    adminState.masterView = 'templates';
    adminState.showInactiveFamilies = false;
    window.currentFamilyId = null;
    saveAdminContext(null);
    updateWhoamiBanner();
    applyRoleVisibility();
    updateMasterScopeVisibility();
    updateScopeBadgeDisplay();
    updateScopeBadgeDisplay();
    if (familySearchInput) familySearchInput.value = '';
    if (familyCreateForm) familyCreateForm.reset();
    if (familyCreateIdPreview) {
      familyCreateIdPreview.textContent = '—';
      familyCreateIdPreview.dataset.value = '';
    }
    updateFamilyCreateButtonState();
    renderFamilyManagement();
  }

  function setAdminState(partial = {}, { persist = true } = {}) {
    const previousRole = adminState.role;
    const previousFamilyScope = adminState.currentFamilyId;
    if (Object.prototype.hasOwnProperty.call(partial, 'role')) {
      const nextRole = partial.role ?? null;
      if (nextRole === 'master' && previousRole !== 'master') {
        adminState.masterView = 'templates';
      } else if (nextRole !== 'master' && previousRole === 'master') {
        adminState.masterView = 'families';
      }
      adminState.role = nextRole;
      if (adminState.role !== 'master') {
        adminState.showInactiveFamilies = false;
      }
    }
    if (Object.prototype.hasOwnProperty.call(partial, 'masterView')) {
      adminState.masterView = partial.masterView === 'families' ? 'families' : 'templates';
    }
    if (Object.prototype.hasOwnProperty.call(partial, 'family_id')) {
      adminState.familyId = normalizeScopeId(partial.family_id);
    }
    if (Object.prototype.hasOwnProperty.call(partial, 'showInactiveFamilies')) {
      adminState.showInactiveFamilies = !!partial.showInactiveFamilies;
    }
    const hasCurrentFamilyId = Object.prototype.hasOwnProperty.call(partial, 'currentFamilyId');
    if (Object.prototype.hasOwnProperty.call(partial, 'families')) {
      const incoming = Array.isArray(partial.families) ? partial.families : [];
      adminState.families = incoming
        .map((family) => {
          if (!family) return null;
          const id = normalizeScopeId(family.id);
          if (!id) return null;
          return { ...family, id };
        })
        .filter(Boolean);
      if (!hasCurrentFamilyId && adminState.role === 'master') {
        if (!adminState.families.some((family) => family && family.id === adminState.currentFamilyId)) {
          adminState.currentFamilyId = null;
        }
      }
    }
    if (hasCurrentFamilyId) {
      adminState.currentFamilyId = normalizeScopeId(partial.currentFamilyId);
    } else if (adminState.role === 'family') {
      adminState.currentFamilyId = normalizeScopeId(adminState.familyId);
    } else {
      adminState.currentFamilyId = normalizeScopeId(adminState.currentFamilyId);
    }
    window.currentFamilyId = adminState.currentFamilyId || null;
    if (persist) {
      saveAdminContext({
        role: adminState.role,
        family_id: adminState.familyId,
        currentFamilyId: adminState.currentFamilyId,
        masterView: adminState.masterView,
        showInactiveFamilies: adminState.showInactiveFamilies
      });
    }
    updateWhoamiBanner();
    applyRoleVisibility();
    updateMasterScopeVisibility();
    renderFamilyManagement();
    if (!adminState.currentFamilyId) {
      pendingTemplatesState.items = [];
      pendingTemplatesState.error = '';
      pendingTemplatesState.loading = false;
    }
    renderPendingTemplates();
    if (previousRole === 'master' && adminState.role !== 'master') {
      masterTemplatesState.tasks = [];
      masterTemplatesState.rewards = [];
      renderMasterTaskList();
      renderMasterRewardList();
    }
    if (!adminState.currentFamilyId && previousFamilyScope) {
      renderPendingTemplates();
    }
  }

  function findFamilyLabel(familyId) {
    if (!familyId) return '';
    const entry = adminState.families.find((family) => family && family.id === familyId);
    if (!entry) return familyId;
    return entry.name ? `${entry.name} (${entry.id})` : entry.id;
  }

  function lettersOnly(value) {
    return (value ?? '').toString().replace(/[^A-Za-z]/g, '');
  }

  function digitsOnly(value) {
    return (value ?? '').toString().replace(/\D/g, '');
  }

  function capitalizeWord(value) {
    const lower = (value ?? '').toString().toLowerCase();
    if (!lower) return '';
    return lower.charAt(0).toUpperCase() + lower.slice(1);
  }

  function generateFamilyIdBase({ firstName, lastName, phone }) {
    const last = lettersOnly(lastName);
    const first = lettersOnly(firstName);
    const digits = digitsOnly(phone).slice(-4);
    if (!last || !first || digits.length < 4) return '';
    return `${capitalizeWord(last)}${first.charAt(0).toUpperCase()}${digits}`;
  }

  function ensureUniqueFamilyId(base) {
    if (!base) return '';
    const existingIds = new Set(
      (adminState.families || [])
        .map((family) => (family && family.id ? String(family.id) : null))
        .filter(Boolean)
    );
    if (!existingIds.has(base)) return base;
    for (let attempt = 1; attempt <= 20; attempt += 1) {
      const candidate = `${base}-a${attempt}`;
      if (!existingIds.has(candidate)) return candidate;
    }
    let fallback = 1;
    while (fallback <= 100) {
      const candidate = `${base}-${fallback}`;
      if (!existingIds.has(candidate)) return candidate;
      fallback += 1;
    }
    return `${base}-${Date.now().toString().slice(-4)}`;
  }

  function computeFamilyIdSuggestion({ firstName, lastName, phone } = {}) {
    const base = generateFamilyIdBase({ firstName, lastName, phone });
    if (!base) return '';
    return ensureUniqueFamilyId(base);
  }

  function readFamilyFormValues() {
    return {
      firstName: familyCreateFirstNameInput?.value || '',
      lastName: familyCreateLastNameInput?.value || '',
      phone: familyCreatePhoneInput?.value || ''
    };
  }

  function updateFamilyCreateButtonState() {
    if (!familyCreateSubmitButton) return;
    if (adminState.role !== 'master') {
      familyCreateSubmitButton.disabled = true;
      return;
    }
    const nameReady = (familyCreateNameInput?.value || '').trim().length > 0;
    const idReady = Boolean(familyCreateIdPreview?.dataset?.value);
    familyCreateSubmitButton.disabled = !(nameReady && idReady);
  }

  function refreshFamilyIdPreview() {
    if (!familyCreateIdPreview) return '';
    const suggestion = computeFamilyIdSuggestion(readFamilyFormValues());
    familyCreateIdPreview.textContent = suggestion || '—';
    familyCreateIdPreview.dataset.value = suggestion || '';
    updateFamilyCreateButtonState();
    return suggestion;
  }

  function updateScopeBadgeDisplay() {
    if (!scopeBadgeEl) return;
    scopeBadgeEl.classList.remove('bg-green-200', 'bg-sky-200');
    scopeBadgeEl.style.backgroundColor = '';
    scopeBadgeEl.style.color = '';

    if (!adminState.role) {
      scopeBadgeEl.textContent = 'No admin';
      return;
    }

    if (adminState.role === 'master') {
      scopeBadgeEl.classList.add('bg-green-200');
      scopeBadgeEl.style.backgroundColor = '#bbf7d0';
      scopeBadgeEl.style.color = '#14532d';
      if (adminState.currentFamilyId) {
        scopeBadgeEl.textContent = `Master · ${findFamilyLabel(adminState.currentFamilyId)}`;
      } else {
        scopeBadgeEl.textContent = 'Master · Select a family';
      }
      return;
    }

    if (adminState.role === 'family') {
      scopeBadgeEl.classList.add('bg-sky-200');
      scopeBadgeEl.style.backgroundColor = '#bae6fd';
      scopeBadgeEl.style.color = '#0c4a6e';
      const familyLabel = adminState.currentFamilyId || adminState.familyId || '';
      scopeBadgeEl.textContent = familyLabel
        ? `Family Admin (${findFamilyLabel(familyLabel)})`
        : 'Family Admin';
      return;
    }

    scopeBadgeEl.textContent = adminState.role;
  }

  function updateWhoamiBanner() {
    if (!whoamiBanner) return;
    const { role, familyId, families, currentFamilyId } = adminState;
    if (!role) {
      whoamiBanner.hidden = true;
      if (familyScopeWrapper) familyScopeWrapper.hidden = true;
      if (familyScopeSummary) familyScopeSummary.hidden = true;
      return;
    }

    whoamiBanner.hidden = false;
    if (whoamiRoleLabel) {
      whoamiRoleLabel.innerHTML = '';
      if (role) {
        const chip = document.createElement('span');
        chip.className = 'role-chip';
        if (role === 'master') {
          chip.classList.add('role-chip--master');
          chip.textContent = 'Master';
        } else if (role === 'family') {
          chip.classList.add('role-chip--family');
          const familyLabel = adminState.familyId || familyId || '';
          chip.textContent = familyLabel
            ? `Family Admin (${familyLabel})`
            : 'Family Admin';
        } else {
          chip.textContent = role;
        }
        whoamiRoleLabel.appendChild(chip);
      }
    }

    if (role === 'master') {
      if (familyScopeWrapper) {
        familyScopeWrapper.hidden = false;
      }
      if (familyScopeSelect) {
        const existingValue = familyScopeSelect.value;
        familyScopeSelect.innerHTML = '';
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = families && families.length ? 'Select family…' : 'No families available';
        familyScopeSelect.appendChild(placeholder);
        if (Array.isArray(families)) {
          for (const family of families) {
            if (!family || !family.id) continue;
            const option = document.createElement('option');
            option.value = family.id;
            option.textContent = family.name ? `${family.name} (${family.id})` : family.id;
            familyScopeSelect.appendChild(option);
          }
        }
        const desired = currentFamilyId || '';
        familyScopeSelect.value = desired;
        if (familyScopeSelect.value !== desired) {
          familyScopeSelect.value = '';
        }
      }
      if (familyScopeSummary) {
        familyScopeSummary.hidden = false;
        familyScopeSummary.textContent = currentFamilyId
          ? `Family scope: ${findFamilyLabel(currentFamilyId)}`
          : 'Select a family to manage data.';
      }
    } else {
      if (familyScopeWrapper) {
        familyScopeWrapper.hidden = true;
      }
      if (familyScopeSummary) {
        const scopeLabel = adminState.currentFamilyId || familyId;
        if (scopeLabel) {
          familyScopeSummary.hidden = false;
          familyScopeSummary.textContent = `Family scope: ${findFamilyLabel(scopeLabel)}`;
        } else {
          familyScopeSummary.hidden = true;
        }
      }
      if (familyScopeSelect) {
        familyScopeSelect.innerHTML = '';
      }
    }
  }

  async function fetchFamiliesStrict() {
    const params = new URLSearchParams();
    if (adminState.showInactiveFamilies) {
      params.set('include_inactive', '1');
    }
    const url = params.toString() ? `/api/families?${params.toString()}` : '/api/families';
    const { res, body } = await adminFetch(url, { skipScope: true });
    if (res.status === 401) {
      const error = new Error(ADMIN_INVALID_MSG);
      error.code = 'UNAUTHORIZED';
      throw error;
    }
    if (!res.ok) {
      const message = presentError(body?.error, 'Failed to load families');
      const error = new Error(message);
      error.code = res.status;
      throw error;
    }
    if (!Array.isArray(body)) return [];
    return body
      .map((family) => {
        if (!family) return null;
        const id = normalizeScopeId(family.id);
        if (!id) return null;
        return { ...family, id };
      })
      .filter(Boolean);
  }

  async function refreshFamiliesFromServer({ silent = false } = {}) {
    if (adminState.role !== 'master') return false;
    try {
      const families = await fetchFamiliesStrict();
      const nextState = { families };
      if (!families.some((family) => family && family.id === adminState.currentFamilyId)) {
        nextState.currentFamilyId = null;
      }
      setAdminState(nextState);
      return true;
    } catch (error) {
      if (!silent) {
        toast(error.message || 'Failed to load families', 'error');
      } else {
        console.warn('refreshFamiliesFromServer failed', error);
      }
      return false;
    }
  }

  async function updateFamily(id, patch) {
    if (!id) throw new Error('Family ID required');
    const payload = patch && typeof patch === 'object' ? patch : {};
    return apiFetch(`/api/families/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      skipScope: true
    });
  }

  async function deleteFamily(id) {
    if (!id) throw new Error('Family ID required');
    return apiFetch(`/api/families/${encodeURIComponent(id)}`, {
      method: 'DELETE',
      skipScope: true
    });
  }

  async function handleFamilyRowSave({ id, nameInput, emailInput, statusSelect, button }) {
    if (!id || !nameInput || !statusSelect || !button) return;
    const trimmedName = nameInput.value.trim();
    const originalName = (nameInput.dataset.originalValue || '').trim();
    const currentStatus = statusSelect.value;
    const originalStatus = statusSelect.dataset.originalValue || '';
    const payload = {};
    if (trimmedName !== originalName) {
      if (!trimmedName) {
        toast('Family name is required.', 'error');
        return;
      }
      payload.name = trimmedName;
    }
    if (currentStatus !== originalStatus) {
      payload.status = currentStatus;
    }
    if (emailInput) {
      const trimmedEmail = (emailInput.value || '').trim().toLowerCase();
      const originalEmail = (emailInput.dataset.originalValue || '').trim().toLowerCase();
      if (trimmedEmail !== originalEmail) {
        if (!trimmedEmail) {
          toast('Email is required.', 'error');
          return;
        }
        if (!trimmedEmail.includes('@')) {
          toast('Enter a valid email address.', 'error');
          return;
        }
        payload.email = trimmedEmail;
      }
    }
    if (!Object.keys(payload).length) {
      button.disabled = true;
      return;
    }
    const originalLabel = button.textContent;
    button.disabled = true;
    button.textContent = 'Saving...';
    try {
      await updateFamily(id, payload);
      toast('Family updated.');
      await refreshFamiliesFromServer({ silent: true });
    } catch (error) {
      if (error?.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
      } else {
        const message = presentError(error?.body?.error || error?.message, 'Failed to update family');
        toast(message, 'error');
      }
    } finally {
      button.textContent = originalLabel;
      button.disabled = true;
    }
  }

  function createFamilyRow(family, statusOptions) {
    if (!familyListTableBody || !family) return;
    const row = document.createElement('tr');
    if (family.id) row.dataset.familyId = String(family.id);
    if (family.id && family.id === adminState.currentFamilyId) {
      row.classList.add('is-active');
    }
    if (normalizedStatus === 'inactive') {
      row.classList.add('is-inactive');
    }

    const idCell = document.createElement('td');
    idCell.textContent = family.id || '—';
    row.appendChild(idCell);

    const nameCell = document.createElement('td');
    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    const originalName = family.name ? String(family.name) : '';
    nameInput.value = originalName;
    nameInput.dataset.originalValue = originalName;
    nameInput.placeholder = 'Family name';
    nameCell.appendChild(nameInput);
    row.appendChild(nameCell);

    const emailCell = document.createElement('td');
    const emailInput = document.createElement('input');
    emailInput.type = 'email';
    const originalEmail = family.email ? String(family.email) : '';
    emailInput.value = originalEmail;
    emailInput.dataset.originalValue = originalEmail;
    emailInput.placeholder = 'Email';
    emailCell.appendChild(emailInput);
    row.appendChild(emailCell);

    const statusCell = document.createElement('td');
    const statusSelect = document.createElement('select');
    statusSelect.className = 'family-status-select';
    const normalizedStatus = (family.status ?? 'active').toString().trim().toLowerCase() || 'active';
    statusSelect.dataset.originalValue = normalizedStatus;
    const deleteOptionValue = '__delete__';
    for (const option of statusOptions) {
      const opt = document.createElement('option');
      opt.value = option;
      opt.textContent = option.charAt(0).toUpperCase() + option.slice(1);
      statusSelect.appendChild(opt);
    }
    const deleteOpt = document.createElement('option');
    deleteOpt.value = deleteOptionValue;
    deleteOpt.textContent = 'Delete permanently';
    statusSelect.appendChild(deleteOpt);
    if (statusSelect.value !== normalizedStatus) {
      statusSelect.value = normalizedStatus;
    }
    statusCell.appendChild(statusSelect);
    row.appendChild(statusCell);

    const actionsCell = document.createElement('td');
    actionsCell.className = 'actions';
    const saveButton = document.createElement('button');
    saveButton.type = 'button';
    saveButton.textContent = 'Save';
    saveButton.disabled = true;
    actionsCell.appendChild(saveButton);
    row.appendChild(actionsCell);

    const evaluateDirtyState = () => {
      if (statusSelect.value === deleteOptionValue) {
        saveButton.disabled = true;
        return;
      }
      const currentName = nameInput.value.trim();
      const originalNameNormalized = (nameInput.dataset.originalValue || '').trim();
      const currentStatus = statusSelect.value;
      const originalStatus = statusSelect.dataset.originalValue || '';
      const currentEmail = emailInput.value.trim().toLowerCase();
      const originalEmailNormalized = (emailInput.dataset.originalValue || '').trim().toLowerCase();
      const changed =
        currentName !== originalNameNormalized ||
        currentStatus !== originalStatus ||
        currentEmail !== originalEmailNormalized;
      saveButton.disabled = !changed;
    };

    nameInput.addEventListener('input', evaluateDirtyState);
    emailInput.addEventListener('input', evaluateDirtyState);
    statusSelect.addEventListener('change', async () => {
      if (statusSelect.value === deleteOptionValue) {
        statusSelect.value = statusSelect.dataset.originalValue || normalizedStatus;
        if (!confirm('Delete this family permanently?')) {
          evaluateDirtyState();
          return;
        }
        try {
          await deleteFamily(family.id);
          toast('Family deleted.');
          await refreshFamiliesFromServer({ silent: false });
        } catch (error) {
          const message = presentError(error?.body?.error || error?.message, 'Delete failed');
          toast(message, 'error');
        }
        return;
      }
      evaluateDirtyState();
    });
    saveButton.addEventListener('click', () =>
      handleFamilyRowSave({ id: family.id, nameInput, emailInput, statusSelect, button: saveButton })
    );

    familyListTableBody.appendChild(row);
  }

  function renderFamilyManagement() {
    if (!familyManagementPanel) return;
    const isMaster = adminState.role === 'master';
    if (familyIncludeInactiveToggle) {
      familyIncludeInactiveToggle.checked = !!adminState.showInactiveFamilies;
      familyIncludeInactiveToggle.disabled = !isMaster;
    }
    if (!isMaster) {
      if (familyListTableBody) {
        familyListTableBody.innerHTML = '';
        if (familyListEmptyRow) {
          const cell = familyListEmptyRow.querySelector('td');
          if (cell) {
            cell.colSpan = 5;
            cell.textContent = 'Master access required.';
          }
          familyListTableBody.appendChild(familyListEmptyRow);
        }
      }
      if (familyCreateIdPreview) {
        familyCreateIdPreview.textContent = '—';
        familyCreateIdPreview.dataset.value = '';
      }
      updateFamilyCreateButtonState();
      return;
    }

    const families = Array.isArray(adminState.families) ? adminState.families : [];
    if (familyListTableBody) {
      familyListTableBody.innerHTML = '';
      if (!families.length) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 5;
        cell.className = 'muted';
        cell.textContent = 'No families yet.';
        row.appendChild(cell);
        familyListTableBody.appendChild(row);
      } else {
        const statusSet = new Set(FAMILY_STATUS_OPTIONS);
        for (const family of families) {
          const normalizedStatus = (family?.status ?? 'active').toString().trim().toLowerCase() || 'active';
          statusSet.add(normalizedStatus);
        }
        const statusOptions = Array.from(statusSet);
        for (const family of families) {
          createFamilyRow(family, statusOptions);
        }
      }
    }

    refreshFamilyIdPreview();
  }

  function ensureMemberPanelStyles() {
    if (document.getElementById('memberPanelsStyles')) return;
    const style = document.createElement('style');
    style.id = 'memberPanelsStyles';
    style.textContent = `
.member-balance-container {
  display: grid;
  gap: 12px;
}

details.member-fold {
  border: 1px solid var(--line, #e5e7eb);
  border-radius: 10px;
  background: #fff;
  overflow: hidden;
}

details.member-fold[open] {
  box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08);
}

details.member-fold summary {
  list-style: none;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  cursor: pointer;
  font-weight: 600;
  padding: 12px 14px;
  position: relative;
  padding-right: 36px;
}

details.member-fold summary::-webkit-details-marker {
  display: none;
}

details.member-fold summary::after {
  content: '▾';
  position: absolute;
  right: 14px;
  color: var(--muted, #6b7280);
  font-size: 12px;
  transition: transform 0.2s ease;
}

details.member-fold[open] summary::after {
  transform: rotate(-180deg);
}

details.member-fold .summary-value {
  font-size: 13px;
  color: var(--muted, #6b7280);
  font-weight: 500;
}

.member-fold-body {
  padding: 0 14px 14px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.ledger-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 8px;
}

.ledger-summary .chip {
  display: inline-flex;
  flex-direction: column;
  padding: 6px 10px;
  background: rgba(37, 99, 235, 0.1);
  color: var(--accent, #2563eb);
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  min-width: 120px;
}

.ledger-summary .chip span {
  color: var(--muted, #6b7280);
  font-weight: 500;
}

.member-ledger {
  display: grid;
  gap: 8px;
}
`;
    if (document.head) {
      document.head.appendChild(style);
    } else if (document.body) {
      document.body.appendChild(style);
    }
  }

  function ensureMemberPanels() {
    ensureMemberPanelStyles();
    if (!memberInfoPanel) return;
    let container = document.getElementById('memberBalanceContainer');
    if (!container) {
      container = document.createElement('div');
      container.id = 'memberBalanceContainer';
      container.className = 'stack member-balance-container';
      if (memberInfoDetails && memberInfoDetails.nextSibling) {
        memberInfoPanel.insertBefore(container, memberInfoDetails.nextSibling);
      } else {
        memberInfoPanel.appendChild(container);
      }
    } else if (container.parentElement !== memberInfoPanel) {
      memberInfoPanel.appendChild(container);
    }

    if (!container.querySelector('#memberBalanceDetails')) {
      container.innerHTML = `
<details id="memberBalanceDetails" class="member-fold">
  <summary>
    <span>Balance</span>
    <span class="summary-value" id="memberBalanceSummaryValue">—</span>
  </summary>
  <div class="member-fold-body" id="memberBalanceBody">
    <div class="muted">Balance info will appear here.</div>
  </div>
</details>
<details id="memberEarnDetails" class="member-fold">
  <summary>
    <span>Earn</span>
    <span class="summary-value" id="memberEarnSummaryValue">—</span>
  </summary>
  <div class="member-fold-body" id="memberEarnBody">
    <div id="memberEarnSummary" class="ledger-summary">
      <div class="muted">Earn activity will appear here.</div>
    </div>
  </div>
</details>
<details id="memberRedeemDetails" class="member-fold">
  <summary>
    <span>Redeem</span>
    <span class="summary-value" id="memberRedeemSummaryValue">—</span>
  </summary>
  <div class="member-fold-body">
    <div id="memberRedeemSummary" class="ledger-summary">
      <div class="muted">Redeem activity will appear here.</div>
    </div>
    <div id="memberLedger" class="member-ledger muted">Redeemed rewards will appear here.</div>
  </div>
</details>
<details id="memberRefundDetails" class="member-fold">
  <summary>
    <span>Refund</span>
    <span class="summary-value" id="memberRefundSummaryValue">—</span>
  </summary>
  <div class="member-fold-body" id="memberRefundBody">
    <div id="memberRefundSummary" class="ledger-summary">
      <div class="muted">Refund activity will appear here.</div>
    </div>
  </div>
</details>
`;
    }
  }

  if (keyInput) {
    keyInput.placeholder = 'enter admin key';
    const saved = getAdminKey();
    if (saved && !keyInput.value) keyInput.value = saved;
  }

  const toastHost = $('toastHost');

  function toast(msg, type = 'success', ms = 2400) {
    if (!toastHost) return alert(msg);
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = msg;
    toastHost.appendChild(el);
    requestAnimationFrame(() => el.classList.add('show'));
    setTimeout(() => {
      el.classList.remove('show');
      setTimeout(() => el.remove(), 200);
    }, ms);
  }

  function openImageModal(src){
    if (!src) return;
    const m=document.createElement('div');
    Object.assign(m.style,{position:'fixed',inset:0,background:'rgba(0,0,0,.7)',display:'grid',placeItems:'center',zIndex:9999});
    m.addEventListener('click',()=>m.remove());
    const big=new Image();
    big.src=src;
    big.style.maxWidth='90vw';
    big.style.maxHeight='90vh';
    big.style.boxShadow='0 8px 24px rgba(0,0,0,.5)';
    m.appendChild(big);
    document.body.appendChild(m);
  }

  function getYouTubeId(u) {
    if (!u) return '';
    try {
      // Allow raw IDs
      if (/^[\w-]{11}$/.test(u)) return u;

      const x = new URL(u);
      // youtu.be/<id>
      if (x.hostname.includes('youtu.be')) {
        return (x.pathname.split('/')[1] || '').split('?')[0].split('&')[0];
      }
      // youtube.com/watch?v=<id>
      const v = x.searchParams.get('v');
      if (v) return v.split('&')[0];

      // youtube.com/shorts/<id>
      const mShorts = x.pathname.match(/\/shorts\/([\w-]{11})/);
      if (mShorts) return mShorts[1];

      // youtube.com/embed/<id>
      const mEmbed = x.pathname.match(/\/embed\/([\w-]{11})/);
      if (mEmbed) return mEmbed[1];

      // Last resort: first 11-char token
      const m = u.match(/([\w-]{11})/);
      return m ? m[1] : '';
    } catch {
      const m = String(u).match(/([\w-]{11})/);
      return m ? m[1] : '';
    }
  }

  (function setupVideoModal() {
    const modal = document.getElementById("videoModal");
    if (!modal) return;

    window.openVideoModal = function openVideoModal(url) {
      const id = window.getYouTubeId ? getYouTubeId(url) : "";
      if (!id) {
        if (typeof toast === "function") toast(translate('invalid_youtube'), 'error');
        console.warn("openVideoModal: no video id for url:", url);
        return;
      }
      const iframe = modal?.querySelector("iframe");
      const link = modal?.querySelector("#openOnYouTube");
      const dialog = modal?.querySelector(".modal-dialog");
      if (!iframe) {
        console.error("openVideoModal: modal/iframe not found");
        return;
      }
      const embedUrl =
        window.getYouTubeEmbed?.(`https://www.youtube.com/watch?v=${id}`, {
          autoplay: true,
        }) ||
        `https://www.youtube.com/embed/${id}?autoplay=1&rel=0&modestbranding=1&playsinline=1`;
      iframe.src = embedUrl;
      if (dialog) {
        const isVertical = window.isLikelyVerticalYouTube?.(url) || isLikelyVerticalYouTube(url);
        dialog.classList.toggle("vertical", Boolean(isVertical));
      }
      if (link) {
        link.href = `https://www.youtube.com/watch?v=${id}`;
      }
      modal.classList.remove("hidden");
      modal.classList.add("open");
    };

    window.closeVideoModal = function closeVideoModal() {
      const iframe = modal?.querySelector("iframe");
      const link = modal?.querySelector("#openOnYouTube");
      if (iframe) iframe.src = "";
      if (link) link.removeAttribute("href");
      modal.classList.remove("open");
      modal.classList.add("hidden");
    };

    // Close on backdrop or [data-close]
    modal.addEventListener("click", (e) => {
      if (e.target.matches("[data-close]") || e.target === modal.querySelector(".modal-backdrop")) {
        e.preventDefault();
        closeVideoModal();
      }
    });

    // Close on Esc
    window.addEventListener("keydown", (e) => {
      if (!modal.classList.contains("hidden") && e.key === "Escape") closeVideoModal();
    });
  })();

  const saveAdminButton = document.querySelector(ADMIN_KEY_SAVE_SELECTOR);
  if (saveAdminButton) {
    saveAdminButton.addEventListener('click', async () => {
      const value = (keyInput?.value || '').trim();
      const persisted = setAdminKey(value);
      toast(persisted ? 'Admin key saved' : 'Admin key saved for this session only (storage blocked).');
      if (value) {
        const ok = await refreshAdminContext({ showToastOnError: true });
        if (ok) {
          await loadFeatureFlagsFromServer();
          await reloadScopedData();
        }
      } else {
        clearAdminContext();
      }
    });
  }

  async function hydrateAdminContext() {
    const saved = getAdminKey();
    if (saved && keyInput && !keyInput.value) {
      keyInput.value = saved;
    }
    const storedContext = loadAdminContext();
    if (storedContext) {
      setAdminState(storedContext, { persist: false });
    } else {
      updateWhoamiBanner();
    }
    if (saved) {
      const ok = await refreshAdminContext({ silent: true });
      if (ok) {
        await loadFeatureFlagsFromServer();
        await reloadScopedData();
      }
    }
  }

  hydrateAdminContext().catch((error) => console.warn('hydrateAdminContext failed', error));

  familyScopeSelect?.addEventListener('change', async () => {
    const selected = normalizeScopeId(familyScopeSelect.value);
    setAdminState({ currentFamilyId: selected });
    await reloadScopedData();
  });

  familyIncludeInactiveToggle?.addEventListener('change', async () => {
    const include = !!familyIncludeInactiveToggle.checked;
    setAdminState({ showInactiveFamilies: include });
    await refreshFamiliesFromServer({ silent: true });
  });

  const quickFamilyButton = document.querySelector('#btn-list-families');
  const quickFamilyTable = document.querySelector('#families-table');

  async function renderQuickFamilyTable() {
    if (!quickFamilyTable) return;
    quickFamilyTable.innerHTML = '<div class="muted">Loading families…</div>';
    try {
      const rows = await apiFetch('/api/families', { skipScope: true });
      if (!Array.isArray(rows) || rows.length === 0) {
        quickFamilyTable.innerHTML = '<div class="muted">No families yet.</div>';
        return;
      }
      const table = document.createElement('table');
      table.className = 'quick-family-table';
      const thead = document.createElement('thead');
      thead.innerHTML = '<tr><th>ID</th><th>Name</th><th>Email</th><th>Status</th></tr>';
      table.appendChild(thead);
      const tbody = document.createElement('tbody');
      for (const family of rows) {
        if (!family) continue;
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${family.id || ''}</td>
          <td>${family.name || ''}</td>
          <td>${family.email || ''}</td>
          <td>${family.status || 'active'}</td>
        `;
        tbody.appendChild(tr);
      }
      table.appendChild(tbody);
      quickFamilyTable.innerHTML = '';
      quickFamilyTable.appendChild(table);
    } catch (error) {
      quickFamilyTable.innerHTML = `<div class="muted">${error?.message || 'Unable to load families.'}</div>`;
    }
  }

  if (quickFamilyButton) {
    quickFamilyButton.addEventListener('click', async () => {
      await renderQuickFamilyTable();
    });
  }

  familySearchForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (adminState.role !== 'master') return;
    const query = (familySearchInput?.value || '').trim();
    if (!query) {
      toast('Enter a family ID to search.', 'error');
      return;
    }
    const submitButton = familySearchForm?.querySelector('button[type="submit"]');
    if (submitButton) submitButton.disabled = true;
    try {
      const families = await fetchFamiliesStrict();
      const match = families.find((family) => family && String(family.id) === query);
      if (!match) {
        toast('Family not found.', 'error');
        return;
      }
      const scopeId = normalizeScopeId(match.id);
      if (!scopeId) {
        toast('Family not found.', 'error');
        return;
      }
      if (adminState.currentFamilyId === scopeId) {
        toast(`Already viewing ${findFamilyLabel(scopeId)}.`);
        return;
      }
      setAdminState({ families, currentFamilyId: scopeId });
      await reloadScopedData();
    } catch (error) {
      toast(error.message || 'Unable to search families.', 'error');
    } finally {
      if (submitButton) submitButton.disabled = adminState.role !== 'master';
    }
  });

  familiesRefreshButton?.addEventListener('click', async () => {
    if (familiesRefreshButton.disabled) return;
    const originalLabel = familiesRefreshButton.textContent;
    familiesRefreshButton.disabled = true;
    familiesRefreshButton.textContent = 'Refreshing...';
    try {
      await refreshFamiliesFromServer({ silent: false });
    } finally {
      familiesRefreshButton.textContent = originalLabel || 'Refresh';
      familiesRefreshButton.disabled = adminState.role !== 'master';
    }
  });

  familyCreateNameInput?.addEventListener('input', () => updateFamilyCreateButtonState());
  [familyCreateFirstNameInput, familyCreateLastNameInput, familyCreatePhoneInput]
    .filter(Boolean)
    .forEach((input) => input.addEventListener('input', () => refreshFamilyIdPreview()));

  familyCreateForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (adminState.role !== 'master') return;
    const familyName = (familyCreateNameInput?.value || '').trim();
    if (!familyName) {
      toast('Family name is required.', 'error');
      return;
    }
    const generatedId = refreshFamilyIdPreview();
    if (!generatedId) {
      toast('Provide guardian details to generate an ID.', 'error');
      return;
    }
    const payload = { id: generatedId, name: familyName, status: 'active' };
    if (familyCreateSubmitButton) {
      familyCreateSubmitButton.disabled = true;
      familyCreateSubmitButton.textContent = 'Creating...';
    }
    try {
      const { res, body } = await adminFetch('/api/families', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        skipScope: true
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (res.status === 409) {
        toast('Family ID already exists. Refresh and try again.', 'error');
        await refreshFamiliesFromServer({ silent: true });
        refreshFamilyIdPreview();
        return;
      }
      if (!res.ok) {
        const message = presentError(body?.error, 'Failed to create family');
        throw new Error(message);
      }
      toast('Family created.');
      familyCreateForm.reset();
      refreshFamilyIdPreview();
      await refreshFamiliesFromServer({ silent: true });
    } catch (error) {
      toast(error.message || 'Failed to create family', 'error');
    } finally {
      if (familyCreateSubmitButton) {
        familyCreateSubmitButton.textContent = 'Create family';
      }
      updateFamilyCreateButtonState();
    }
  });

  refreshFamilyIdPreview();

  async function loadFeatureFlagsFromServer() {
    try {
      const { res, body } = await adminFetch('/api/features');
      if (!res.ok) return;
      const data = body && typeof body === 'object' ? body : {};
      if (data && typeof data === 'object') {
        applyFeatureFlags(data);
      }
    } catch (err) {
      console.warn('feature flag fetch failed', err);
    }
  }

  function getAdminKeyInput() {
    const el = document.getElementById('adminKey');
    return (el?.value || '').trim();
  }
  function ensureAdminKey() {
    const value = getAdminKeyInput();
    if (value) return value;
    const stored = getAdminKey();
    if (stored && keyInput) {
      keyInput.value = stored;
      return stored;
    }
    return stored;
  }

  function readCurrentFamilyId() {
    try {
      if (typeof window.currentFamilyId === 'function') {
        const value = window.currentFamilyId();
        if (value) return String(value).trim();
      }
    } catch (error) {
      console.warn('currentFamilyId getter failed', error);
    }
    const direct = window.currentFamilyId;
    if (typeof direct === 'string' && direct.trim()) {
      return direct.trim();
    }
    if (adminState.currentFamilyId) return adminState.currentFamilyId;
    if (adminState.familyId) return adminState.familyId;
    return '';
  }

  function requireFamilyId({ silent = false } = {}) {
    const id = readCurrentFamilyId();
    if (!id) {
      if (!silent) toast('Select a family first', 'error');
      throw new Error('family_id required');
    }
    return id;
  }

  function appendFamilyQuery(url, familyId) {
    const [base, query = ''] = String(url).split('?');
    const params = new URLSearchParams(query);
    params.set('family_id', familyId);
    const qs = params.toString();
    return `${base}?${qs}`;
  }

  function withFamilyInBody(payload, familyId) {
    const base = payload && typeof payload === 'object' ? { ...payload } : {};
    base.family_id = familyId;
    return base;
  }
  async function adminFetch(url, opts = {}) {
    const { idempotencyKey, headers: extraHeaders, skipScope = false, ...fetchOpts } = opts;
    const key = ensureAdminKey();
    if (!key) {
      return {
        res: {
          ok: false,
          status: 428,
          statusText: 'Admin key required',
          headers: { get: () => null }
        },
        body: { error: ADMIN_KEY_REQUIRED_MSG, code: 'ADMIN_KEY_REQUIRED' }
      };
    }
    const headers = {
      'x-admin-key': key,
      'x-actor-role': 'admin',
      ...(extraHeaders || {})
    };
    if (!skipScope) {
      const scopeId = adminState.currentFamilyId || null;
      if (scopeId) {
        headers['x-act-as-family'] = scopeId;
      }
    }
    if (idempotencyKey) headers['idempotency-key'] = idempotencyKey;
    const res = await fetch(url, { ...fetchOpts, headers });
    const ct = res.headers.get('content-type') || '';
    const body = ct.includes('application/json') ? await res.json().catch(()=>({})) : await res.text().catch(()=> '');
    return { res, body };
  }

  async function apiFetch(url, opts = {}) {
    const { res, body } = await adminFetch(url, opts);
    if (!res.ok) {
      const message = body && typeof body === 'object' ? body.error || res.statusText : res.statusText;
      const error = new Error(message || 'Request failed');
      error.status = res.status;
      error.body = body;
      throw error;
    }
    return body;
  }

  async function loadAvailableTaskTemplates() {
    const familyId = requireFamilyId();
    const list = await apiFetch(
      `/api/families/${encodeURIComponent(familyId)}/master-tasks/available`
    );
    return Array.isArray(list) ? list : [];
  }

  async function adoptTaskTemplate(masterTaskId) {
    if (!masterTaskId) {
      throw new Error('masterTaskId required');
    }
    const familyId = requireFamilyId();
    await apiFetch(`/api/families/${encodeURIComponent(familyId)}/tasks/from-master`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ master_task_id: masterTaskId })
    });
    await reloadEarnPointsTable('active');
  }

  async function fetchFamilyDetail(familyId) {
    if (!familyId) return null;
    try {
      const params = new URLSearchParams({ id: familyId }).toString();
      const { res, body } = await adminFetch(`/api/families?${params}`, { skipScope: true });
      if (!res.ok) return null;
      if (Array.isArray(body)) {
        return body.find((entry) => entry && entry.id === familyId) || null;
      }
      if (body && typeof body === 'object' && body.id === familyId) {
        return body;
      }
    } catch (error) {
      console.warn('fetchFamilyDetail failed', error);
    }
    return null;
  }

  async function reloadScopedData() {
    const loaders = [
      loadMembersList,
      loadRewards,
      loadTemplates,
      loadPendingTemplates,
      loadHolds,
      loadActivity,
      loadMasterTasks,
      loadMasterRewards
    ];
    for (const loader of loaders) {
      if (typeof loader !== 'function') continue;
      try {
        await loader();
      } catch (error) {
        console.warn('Scoped data reload failed', error);
      }
    }
  }

  async function refreshAdminContext({ showToastOnError = false, silent = false } = {}) {
    const key = ensureAdminKey();
    const masterCard = document.querySelector('#card-family-scope');
    const quickFamilyTable = document.querySelector('#families-table');
    if (!key) {
      clearAdminContext();
      if (masterCard) masterCard.hidden = true;
      if (quickFamilyTable) quickFamilyTable.innerHTML = '';
      return false;
    }
    try {
      const payload = await apiFetch('/api/whoami', { skipScope: true });
      const nextState = {
        role: payload.role ?? null,
        family_id: payload.family_id ?? payload.familyId ?? null
      };
      if (masterCard) {
        masterCard.hidden = nextState.role !== 'master';
        if (nextState.role !== 'master' && quickFamilyTable) {
          quickFamilyTable.innerHTML = '';
        }
      }
      if (nextState.role === 'master') {
        try {
          const families = (await fetchFamiliesStrict()).filter((family) => family && normalizeScopeId(family.id));
          nextState.families = families;
          const previous = normalizeScopeId(adminState.currentFamilyId);
          if (previous && families.some((family) => family?.id === previous)) {
            nextState.currentFamilyId = previous;
          } else {
            nextState.currentFamilyId = null;
          }
        } catch (error) {
          if (!silent) {
            console.warn('Unable to fetch family list', error);
          }
          nextState.families = Array.isArray(adminState.families)
            ? adminState.families.filter((family) => family && normalizeScopeId(family.id))
            : [];
          nextState.currentFamilyId = normalizeScopeId(adminState.currentFamilyId);
        }
      } else if (nextState.role === 'family') {
        nextState.currentFamilyId = normalizeScopeId(nextState.family_id);
        if (nextState.family_id) {
          const detail = await fetchFamilyDetail(nextState.family_id);
          if (detail) {
            nextState.families = [detail];
          } else if (Array.isArray(adminState.families)) {
            const existing = adminState.families.find((entry) => entry && entry.id === nextState.family_id);
            if (existing) {
              nextState.families = [existing];
            }
          }
        }
      } else {
        nextState.currentFamilyId = null;
      }
      setAdminState(nextState);
      return true;
    } catch (error) {
      if (!silent) {
        console.warn('Admin context refresh failed', error);
      }
      if (showToastOnError) {
        toast(error?.message || 'Admin key validation failed', 'error');
      }
      clearBadge();
      if (masterCard) masterCard.style.display = 'none';
      if (quickFamilyTable) quickFamilyTable.innerHTML = '';
      clearAdminContext();
      return false;
    }
  }

  newFamilyButton?.addEventListener('click', () => show('#modal-new-family'));
  forgotKeyButton?.addEventListener('click', () => show('#modal-forgot-key'));

  newFamilyCreateButton?.addEventListener('click', async () => {
    const familyName = document.querySelector('#nf-family')?.value?.trim();
    const adminName = document.querySelector('#nf-admin-name')?.value?.trim();
    const email = document.querySelector('#nf-email')?.value?.trim();
    const adminKey = document.querySelector('#nf-admin-key')?.value?.trim();
    const msg = document.querySelector('#nf-msg');
    if (msg) msg.textContent = 'Creating...';
    try {
      const res = await fetch('/api/families/self-register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ familyName, adminName, email, adminKey })
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const error = data && typeof data === 'object' ? data.error : null;
        throw new Error(error || 'Failed');
      }
      if (msg) msg.textContent = 'Family created. Check your email for confirmation.';
      await refreshAdminContext();
      window.setTimeout(() => hide('#modal-new-family'), 1200);
    } catch (error) {
      if (msg) msg.textContent = error.message || 'Failed to create family';
    }
  });

  forgotKeySendButton?.addEventListener('click', async () => {
    const email = document.querySelector('#fk-email')?.value?.trim();
    const msg = document.querySelector('#fk-msg');
    if (msg) msg.textContent = 'Sending...';
    try {
      const res = await fetch('/api/families/forgot-admin-key', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email })
      });
      if (!res.ok) throw new Error('Failed');
      if (msg) msg.textContent = 'If that email exists, we sent the key.';
      window.setTimeout(() => hide('#modal-forgot-key'), 1200);
    } catch (error) {
      if (msg) msg.textContent = 'Unable to send right now.';
    }
  });

  const ERROR_MESSAGES = {
    INVALID_AMOUNT: 'Enter a positive amount to continue.',
    INVALID_DELTA: 'Adjustment amount must be non-zero.',
    INVALID_USER: 'Choose a member before performing this action.',
    INVALID_PARENT_TX: 'Select a redeemed reward to refund.',
    INVALID_REASON: 'Pick a refund reason.',
    REFUND_NOT_ALLOWED: 'This reward has already been fully refunded.',
    OVER_REFUND: 'Amount exceeds the refundable balance.',
    FEATURE_DISABLED: 'This feature is currently turned off.',
    ROLE_REQUIRED: 'You do not have permission to do that.',
    INSUFFICIENT_FUNDS: 'Not enough balance to complete this action.',
    TOKEN_USED: 'This QR code has already been used.',
    hold_not_pending: 'This reward request is no longer pending.',
    invalid_payload: 'The request was missing required information.',
    scan_failed: 'Unable to redeem that QR code. Try again.',
    ADMIN_KEY_REQUIRED: ADMIN_KEY_REQUIRED_MSG,
    'PLEASE ENTER THE ADMINKEY FIRST': ADMIN_KEY_REQUIRED_MSG
  };

  function presentError(code, fallback) {
    if (!code && fallback) return fallback;
    const normalized = String(code || '').trim();
    if (!normalized) return fallback || 'Something went wrong.';
    const direct = ERROR_MESSAGES[normalized];
    if (direct) return direct;
    const upper = ERROR_MESSAGES[normalized.toUpperCase()];
    if (upper) return upper;
    return fallback || normalized.replace(/_/g, ' ');
  }

  function debounce(fn, wait = 300) {
    let timer = null;
    return function debounced(...args) {
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), wait);
    };
  }

  function renderQr(elId, text) {
    const el = $(elId);
    if (!el) return;
    el.innerHTML = '';
    if (!text) return;
    new QRCode(el, { text, width: 200, height: 200 });
  }

  function generateIdempotencyKey() {
    if (window.crypto?.randomUUID) {
      return window.crypto.randomUUID();
    }
    const rand = Math.random().toString(16).slice(2);
    return `refund-${Date.now()}-${rand}`;
  }

  const MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];

  function buildDateParts(year, month, day) {
    const y = Number(year);
    const m = Number(month);
    const d = Number(day);
    if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
    const date = new Date(Date.UTC(y, m - 1, d));
    if (date.getUTCFullYear() !== y || date.getUTCMonth() + 1 !== m || date.getUTCDate() !== d) return null;
    return { year: y, month: m, day: d, date };
  }

  function parseDobParts(value) {
    if (value === undefined || value === null) return null;
    const trimmed = String(value).trim();
    if (!trimmed) return null;

    const digits = trimmed.replace(/[^0-9]/g, '');
    if (digits.length === 8) {
      const parts = buildDateParts(digits.slice(0, 4), digits.slice(4, 6), digits.slice(6, 8));
      if (parts) return parts;
    }

    const isoMatch = trimmed.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (isoMatch) {
      const parts = buildDateParts(isoMatch[1], isoMatch[2], isoMatch[3]);
      if (parts) return parts;
    }

    const sanitized = trimmed
      .replace(/(\d+)(st|nd|rd|th)/gi, '$1')
      .replace(/[.,]/g, '')
      .replace(/-/g, ' ');
    const parsed = Date.parse(sanitized);
    if (!Number.isNaN(parsed)) {
      const date = new Date(parsed);
      return buildDateParts(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
    }

    return null;
  }

  function normalizeDobInput(value) {
    const trimmed = (value ?? '').toString().trim();
    if (!trimmed) return '';
    const parts = parseDobParts(trimmed);
    if (!parts) return trimmed;
    const month = String(parts.month).padStart(2, '0');
    const day = String(parts.day).padStart(2, '0');
    return `${parts.year.toString().padStart(4, '0')}-${month}-${day}`;
  }

  function formatDobFriendly(value) {
    const trimmed = (value ?? '').toString().trim();
    if (!trimmed) return '';
    const parts = parseDobParts(trimmed);
    if (!parts) return trimmed;
    const monthName = MONTH_NAMES[parts.month - 1] || parts.month;
    const day = parts.day;
    const suffix = (() => {
      const mod100 = day % 100;
      if (mod100 >= 11 && mod100 <= 13) return 'th';
      switch (day % 10) {
        case 1: return 'st';
        case 2: return 'nd';
        case 3: return 'rd';
        default: return 'th';
      }
    })();
    return `${parts.year}-${monthName} ${day}${suffix}`;
  }

  function normalizeSexValue(value) {
    const normalized = (value ?? '').toString().trim().toLowerCase();
    if (!normalized) return '';
    if (['boy', 'male', 'm'].includes(normalized)) return 'Boy';
    if (['girl', 'female', 'f'].includes(normalized)) return 'Girl';
    return '';
  }


  const memberActionInput = document.querySelector('#member-search');
  const memberIdInput = $('memberUserId') || memberActionInput;
  const memberStatusEl = $('memberStatus');
  const memberInfoDetails = $('memberInfoDetails');
  const memberBalanceContainer = $('memberBalanceContainer');
  const memberBalanceDetails = $('memberBalanceDetails');
  const memberBalanceSummaryValue = $('memberBalanceSummaryValue');
  const memberBalanceBody = $('memberBalanceBody');
  const memberEarnDetails = $('memberEarnDetails');
  const memberEarnSummaryValue = $('memberEarnSummaryValue');
  const memberEarnSummary = $('memberEarnSummary');
  const memberRedeemDetails = $('memberRedeemDetails');
  const memberRedeemSummaryValue = $('memberRedeemSummaryValue');
  const memberRedeemSummary = $('memberRedeemSummary');
  const memberRefundDetails = $('memberRefundDetails');
  const memberRefundSummaryValue = $('memberRefundSummaryValue');
  const memberRefundSummary = $('memberRefundSummary');
  const memberLedgerHost = $('memberLedger');
  const memberTableBody = $('memberTable')?.querySelector('tbody');
  const memberListStatus = $('memberListStatus');
  const memberSearchInput = $('memberSearch');
  const memberListSection = $('memberListSection');
  const memberEditModal = $('memberEditModal');
  const memberEditForm = $('memberEditForm');
  const memberEditIdInput = $('memberEditId');
  const memberEditNameInput = $('memberEditName');
  const memberEditDobInput = $('memberEditDob');
  const memberEditSexSelect = $('memberEditSex');
  const memberEditSaveBtn = $('btnMemberEditSave');

  const refundModal = $('refundModal');
  const refundForm = $('refundForm');
  const refundAmountInput = $('refundAmount');
  const refundReasonSelect = $('refundReason');
  const refundNotesInput = $('refundNotes');
  const refundRemainingText = $('refundRemaining');
  const refundConfirmBtn = $('btnRefundConfirm');
  const activityTableBody = $('activityTable')?.querySelector('tbody');
  const activityStatus = $('activityStatus');
  const activityVerb = $('activityVerb');
  const activityActor = $('activityActor');
  const activityFrom = $('activityFrom');
  const activityTo = $('activityTo');
  const btnActivityRefresh = $('btnActivityRefresh');
  const activityRowIndex = new Map();

  const MEMBER_EDIT_SAVE_DEFAULT_TEXT = memberEditSaveBtn?.textContent || 'Save Changes';
  let activeMemberEdit = null;
  let activeRefundContext = null;
  let latestHints = null;
  let latestHintsErrorMessage = null;
  let featureFlags = { refunds: true };

  function applyFeatureFlags(flags = {}) {
    featureFlags = { ...featureFlags, ...flags };
    const refundsEnabled = !!featureFlags.refunds;
    document.documentElement.setAttribute('data-feature-refunds', refundsEnabled ? 'on' : 'off');
    const nodes = document.querySelectorAll('[data-feature="refunds"]');
    nodes.forEach((el) => {
      if (el instanceof HTMLButtonElement) {
        el.disabled = !refundsEnabled;
        el.title = refundsEnabled ? '' : 'Refunds are disabled by policy.';
      } else if (!refundsEnabled) {
        el.setAttribute('aria-disabled', 'true');
      } else {
        el.removeAttribute('aria-disabled');
      }
    });
    if (!refundsEnabled && typeof closeRefundModal === 'function') {
      closeRefundModal();
    }
  }

  function renderBalanceFromHints(hints) {
    if (!memberBalanceSummaryValue || !memberBalanceBody) return;
    if (!hints) {
      memberBalanceSummaryValue.textContent = '—';
      const message = latestHintsErrorMessage || 'Balance info will appear here.';
      setPlaceholder(memberBalanceBody, message);
      return;
    }
    const balanceValue = Number(hints.balance ?? 0);
    const formattedBalance = formatTokenValue(balanceValue);
    memberBalanceSummaryValue.textContent = `${formattedBalance} tokens`;
    memberBalanceBody.innerHTML = '';
    const line = document.createElement('div');
    line.textContent = `Current balance: ${formattedBalance} tokens.`;
    memberBalanceBody.appendChild(line);
    const details = document.createElement('small');
    details.className = 'muted';
    const redeemInfo = hints.can_redeem
      ? `Can redeem up to ${formatTokenValue(hints.max_redeem ?? balanceValue)} tokens.`
      : 'Not enough balance to redeem right now.';
    details.textContent = redeemInfo;
    memberBalanceBody.appendChild(details);
  }

  function applyStateHints(hints, { errorMessage = null } = {}) {
    latestHints = hints ? { ...hints } : null;
    latestHintsErrorMessage = hints ? null : errorMessage || null;
    if (!hints) {
      renderBalanceFromHints(null);
      return;
    }
    applyFeatureFlags(hints.features || {});
    renderBalanceFromHints(hints);
    const refundsEnabled = !!(hints.features?.refunds);
    const refundAllowed = refundsEnabled && !!hints.can_refund;
    document.querySelectorAll('[data-feature="refunds"]').forEach((node) => {
      if (!(node instanceof HTMLButtonElement)) return;
      node.disabled = !refundAllowed;
      node.title = refundAllowed ? '' : refundsEnabled ? 'Nothing refundable right now.' : 'Refunds are disabled by policy.';
    });
  }

  function syncMemberInputs(value) {
    const next = typeof value === 'string' ? value : '';
    if (memberIdInput && memberIdInput.value !== next) memberIdInput.value = next;
    if (memberActionInput && memberActionInput.value !== next) memberActionInput.value = next;
  }

  function getMemberIdInfo() {
    const rawPrimary = (memberIdInput?.value || '').trim();
    const rawSearch = memberActionInput && memberActionInput !== memberIdInput
      ? (memberActionInput.value || '').trim()
      : '';
    const raw = rawPrimary || rawSearch;
    return { raw, normalized: raw.toLowerCase() };
  }

  function normalizeMemberInput() {
    const info = getMemberIdInfo();
    if (info.raw && info.raw !== info.normalized) {
      syncMemberInputs(info.normalized);
      return { raw: info.normalized, normalized: info.normalized };
    }
    return info;
  }

  function setPlaceholder(container, text) {
    if (!container) return;
    container.innerHTML = '';
    if (!text) return;
    const div = document.createElement('div');
    div.className = 'muted';
    div.textContent = text;
    container.appendChild(div);
  }

  function resetSummaryValue(el) {
    if (el) el.textContent = '—';
  }

  function collapseDetails(detailsEl) {
    detailsEl?.removeAttribute('open');
  }

  function clearMemberLedger(message = 'Redeemed rewards will appear here.') {
    setPlaceholder(memberLedgerHost, message);
    setPlaceholder(memberEarnSummary, 'Earn activity will appear here.');
    setPlaceholder(memberRedeemSummary, 'Redeem activity will appear here.');
    setPlaceholder(memberRefundSummary, 'Refund activity will appear here.');
    if (memberBalanceBody) setPlaceholder(memberBalanceBody, 'Balance info will appear here.');
    resetSummaryValue(memberBalanceSummaryValue);
    resetSummaryValue(memberEarnSummaryValue);
    resetSummaryValue(memberRedeemSummaryValue);
    resetSummaryValue(memberRefundSummaryValue);
    collapseDetails(memberBalanceDetails);
    collapseDetails(memberEarnDetails);
    collapseDetails(memberRedeemDetails);
    collapseDetails(memberRefundDetails);
    if (memberBalanceContainer) memberBalanceContainer.hidden = true;
    activeRefundContext = null;
  }

  function formatTokenValue(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) {
      return `${value ?? 0}`;
    }
    return num.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 0 });
  }

  function renderSummarySection({ summary, valueEl, container, emptyMessage }) {
    if (valueEl) {
      if (!summary) {
        valueEl.textContent = 'No activity';
      } else {
        const d7 = formatTokenValue(summary.d7 ?? 0);
        const d30 = formatTokenValue(summary.d30 ?? 0);
        valueEl.textContent = `7d ${d7} · 30d ${d30}`;
      }
    }
    if (!container) return;
    container.innerHTML = '';
    if (!summary) {
      setPlaceholder(container, emptyMessage);
      return;
    }
    const rows = [
      { label: 'Past 7 days', value: summary.d7 ?? 0 },
      { label: 'Past 30 days', value: summary.d30 ?? 0 }
    ];
    for (const row of rows) {
      const chip = document.createElement('div');
      chip.className = 'chip';
      chip.textContent = row.label;
      const span = document.createElement('span');
      span.textContent = `${formatTokenValue(row.value)} tokens`;
      chip.appendChild(span);
      container.appendChild(chip);
    }
  }

  function renderLedgerSummary(summary) {
    renderSummarySection({
      summary: summary?.earn,
      valueEl: memberEarnSummaryValue,
      container: memberEarnSummary,
      emptyMessage: 'No earn activity recorded.'
    });
    renderSummarySection({
      summary: summary?.redeem,
      valueEl: memberRedeemSummaryValue,
      container: memberRedeemSummary,
      emptyMessage: 'No redeemed activity recorded.'
    });
    renderSummarySection({
      summary: summary?.refund,
      valueEl: memberRefundSummaryValue,
      container: memberRefundSummary,
      emptyMessage: 'No refunds recorded.'
    });
  }

  function renderRedeemLedger(redeems = []) {
    if (!memberLedgerHost) return;
    activeRefundContext = null;
    memberLedgerHost.innerHTML = '';
    if (!Array.isArray(redeems) || !redeems.length) {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = 'No redeemed rewards yet.';
      memberLedgerHost.appendChild(empty);
      return;
    }
    for (const entry of redeems) {
      const card = document.createElement('div');
      card.className = 'ledger-entry';

      const header = document.createElement('div');
      header.className = 'ledger-entry-header';
      const title = document.createElement('div');
      title.className = 'ledger-entry-title';
      const amount = Math.abs(Number(entry.redeem_amount || entry.delta || 0));
      const label = entry.note || entry.action || 'Redeem';
      title.textContent = `${label} · ${amount} tokens`;
      header.appendChild(title);

      const badge = document.createElement('span');
      badge.className = 'badge';
      if (entry.refund_status === 'refunded') {
        badge.classList.add('success');
        badge.textContent = 'Refunded';
      } else if (entry.refund_status === 'partial') {
        badge.classList.add('warning');
        badge.textContent = `Partial refund`;
      } else {
        badge.textContent = 'Redeemed';
      }
      header.appendChild(badge);

      const actions = document.createElement('div');
      actions.className = 'ledger-actions';
      if (entry.remaining_refundable > 0) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'primary';
        btn.dataset.feature = 'refunds';
        btn.textContent = entry.remaining_refundable === amount
          ? 'Refund'
          : `Refund (remaining ${entry.remaining_refundable})`;
        btn.addEventListener('click', () => openRefundModal(entry));
        actions.appendChild(btn);
      } else {
        const note = document.createElement('span');
        note.className = 'refund-remaining';
        note.textContent = 'No refundable amount remaining';
        actions.appendChild(note);
      }
      header.appendChild(actions);

      card.appendChild(header);

      const meta = document.createElement('div');
      meta.className = 'meta';
      const when = document.createElement('span');
      when.textContent = `Redeemed ${formatTime(entry.at)}`;
      meta.appendChild(when);
      if (entry.actor) {
        const actor = document.createElement('span');
        actor.textContent = `By ${entry.actor}`;
        meta.appendChild(actor);
      }
      const totals = document.createElement('span');
      totals.textContent = `Refunded ${entry.refunded_amount || 0} · Remaining ${entry.remaining_refundable || 0}`;
      meta.appendChild(totals);
      card.appendChild(meta);

      if (Array.isArray(entry.refunds) && entry.refunds.length) {
        const list = document.createElement('div');
        list.className = 'ledger-refund-list';
        for (const refund of entry.refunds) {
          const line = document.createElement('div');
          line.className = 'ledger-refund';
          const headline = document.createElement('strong');
          headline.textContent = `+${Number(refund.delta || 0)} tokens on ${formatTime(refund.at)}`;
          line.appendChild(headline);
          const details = document.createElement('span');
          const parts = [];
          if (refund.refund_reason) parts.push(refund.refund_reason.replace(/_/g, ' '));
          if (refund.actor) parts.push(refund.actor);
          if (refund.id) parts.push(`#${refund.id}`);
          details.textContent = parts.join(' · ');
          line.appendChild(details);
          if (refund.refund_notes) {
            const notes = document.createElement('span');
            notes.textContent = refund.refund_notes;
            line.appendChild(notes);
          }
          list.appendChild(line);
        }
        card.appendChild(list);
      }

      memberLedgerHost.appendChild(card);
    }
  }

  function openRefundModal(entry) {
    if (!refundModal || !refundAmountInput || !refundReasonSelect || !refundConfirmBtn) return;
    activeRefundContext = entry;
    const remaining = Math.max(0, Number(entry.remaining_refundable || 0));
    refundAmountInput.value = remaining || Math.abs(Number(entry.redeem_amount || entry.delta || 0)) || 1;
    refundAmountInput.max = remaining || '';
    refundAmountInput.focus();
    refundReasonSelect.value = refundReasonSelect.options[0]?.value || 'duplicate';
    if (refundNotesInput) refundNotesInput.value = '';
    if (refundRemainingText) {
      const label = entry.note || entry.action || 'reward';
      refundRemainingText.textContent = `Up to ${remaining} tokens can be refunded for “${label}”.`;
    }
    refundConfirmBtn.disabled = false;
    refundConfirmBtn.textContent = 'Confirm refund';
    refundModal.classList.remove('hidden');
  }

  function closeRefundModal() {
    if (!refundModal) return;
    refundModal.classList.add('hidden');
    activeRefundContext = null;
  }

  async function refreshMemberLedger(userId, { showPanels = true, familyId: familyIdOverride = '' } = {}) {
    if (!userId) {
      clearMemberLedger();
      return null;
    }
    const familyId = familyIdOverride || readCurrentFamilyId();
    if (!familyId) {
      toast('Select a family first', 'error');
      return null;
    }
    if (showPanels && memberBalanceContainer) memberBalanceContainer.hidden = false;
    if (memberLedgerHost) setPlaceholder(memberLedgerHost, 'Loading redeemed items…');
    if (memberEarnSummaryValue) memberEarnSummaryValue.textContent = 'Loading…';
    if (memberRedeemSummaryValue) memberRedeemSummaryValue.textContent = 'Loading…';
    if (memberRefundSummaryValue) memberRefundSummaryValue.textContent = 'Loading…';
    if (memberEarnSummary) setPlaceholder(memberEarnSummary, 'Loading earn activity…');
    if (memberRedeemSummary) setPlaceholder(memberRedeemSummary, 'Loading redeem activity…');
    if (memberRefundSummary) setPlaceholder(memberRefundSummary, 'Loading refund activity…');
    try {
      const url = appendFamilyQuery(`/ck/ledger/${encodeURIComponent(userId)}`, familyId);
      const { res, body } = await adminFetch(url);
      if (res.status === 401) {
        clearMemberLedger('Admin key invalid.');
        toast(ADMIN_INVALID_MSG, 'error');
        return null;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load ledger');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      renderLedgerSummary(data.summary || {});
      renderRedeemLedger(Array.isArray(data.redeems) ? data.redeems : []);
      if (data.hints) {
        applyStateHints(data.hints);
      } else {
        applyStateHints(null);
      }
      if (showPanels && memberBalanceContainer) memberBalanceContainer.hidden = false;
      return data;
    } catch (err) {
      console.error(err);
      const friendly = presentError(err?.message, 'Failed to load ledger history.');
      if (memberLedgerHost) setPlaceholder(memberLedgerHost, friendly);
      renderLedgerSummary(null);
      applyStateHints(null, { errorMessage: presentError(err?.message, 'Balance temporarily unavailable. Please try again.') });
      return null;
    }
  }

  function requireMemberId({ silent = false } = {}) {
    const info = normalizeMemberInput();
    if (!info.normalized) {
      if (!silent) toast('Enter user id', 'error');
      memberIdInput?.focus();
      return null;
    }
    return info.normalized;
  }

  async function resolveMemberAdmin(userInput, familyIdOverride = '') {
    const raw = (userInput ?? '').toString().trim();
    if (!raw) {
      throw new Error('Enter a user name or ID.');
    }
    const familyId = familyIdOverride || requireFamilyId();
    const params = new URLSearchParams({ user: raw, family_id: familyId }).toString();
    const { res, body } = await adminFetch(`/api/admin/resolve-member?${params}`);
    if (res.status === 401) {
      toast(ADMIN_INVALID_MSG, 'error');
      throw new Error(ADMIN_INVALID_MSG);
    }
    if (!res.ok) {
      const message = presentError(body?.error, 'Member not found');
      const error = new Error(message);
      error.status = res.status;
      error.body = body;
      throw error;
    }
    if (!body || typeof body !== 'object' || !body.id) {
      throw new Error('Member not found');
    }
    return { id: body.id, name: body.name || '', family_id: body.family_id || null };
  }

  async function resolveMemberForActions() {
    const info = getMemberIdInfo();
    const raw = (info.raw || '').trim();
    if (!raw) throw new Error('Enter an ID or full name');
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      toast('Select a family first', 'error');
      throw new Error('Select a family first');
    }
    const resolved = await resolveMemberAdmin(raw, familyId);
    if (!resolved || !resolved.id) {
      throw new Error('Member not found');
    }
    syncMemberInputs(resolved.id);
    return resolved.id;
  }

  function setMemberStatus(message) {
    if (memberStatusEl) memberStatusEl.textContent = message || '';
  }

  function setMemberInfoMessage(message) {
    if (!memberInfoDetails) return;
    memberInfoDetails.innerHTML = '';
    const div = document.createElement('div');
    div.className = 'muted';
    div.textContent = message;
    memberInfoDetails.appendChild(div);
  }

  // Collapsible panels: pairs <button.card-toggle aria-controls="..."> + <div id="...">
  function initCollapsibles() {
    const toggles = document.querySelectorAll('.card .card-toggle[aria-controls]');
    toggles.forEach(btn => {
      const targetId = btn.getAttribute('aria-controls');
      const target = document.getElementById(targetId);
      if (!target) return;

      const arrow = btn.querySelector('[data-arrow]');

      const setState = (expanded) => {
        btn.setAttribute('aria-expanded', String(expanded));
        target.hidden = !expanded;
        if (arrow) arrow.textContent = expanded ? '▲' : '▼';
      };

      // start collapsed unless markup says otherwise
      setState(btn.getAttribute('aria-expanded') === 'true');

      btn.addEventListener('click', () => {
        const next = btn.getAttribute('aria-expanded') !== 'true';
        setState(next);
      });
    });
  }

  initCollapsibles();

  function renderMemberInfo(member) {
    if (!memberInfoDetails) return;
    memberInfoDetails.innerHTML = '';
    if (!member) {
      setMemberInfoMessage('No member found.');
      return;
    }
    const nameEl = document.createElement('div');
    nameEl.style.fontWeight = '600';
    nameEl.textContent = member.name || member.userId;
    memberInfoDetails.appendChild(nameEl);

    const idEl = document.createElement('div');
    idEl.className = 'muted mono';
    idEl.textContent = `ID: ${member.userId}`;
    memberInfoDetails.appendChild(idEl);

    const dobEl = document.createElement('div');
    dobEl.className = 'muted';
    const dobDisplay = formatDobFriendly(member.dob || '') || '—';
    dobEl.textContent = `DOB: ${dobDisplay}`;
    memberInfoDetails.appendChild(dobEl);

    const sexEl = document.createElement('div');
    sexEl.className = 'muted';
    const sexDisplay = normalizeSexValue(member.sex) || member.sex || '—';
    sexEl.textContent = `Sex: ${sexDisplay}`;
    memberInfoDetails.appendChild(sexEl);
  }

  function memberIdChanged({ loadActivityNow = true } = {}) {
    normalizeMemberInput();
    setMemberStatus('');
    setMemberInfoMessage('Enter a user ID and click Member Info to view details.');
    clearMemberLedger('Redeemed rewards will appear here.');
    loadHolds();
    loadActivity();
  }

  memberIdInput?.addEventListener('change', (event) => memberIdChanged({ loadActivityNow: event?.isTrusted !== false }));
  memberIdInput?.addEventListener('input', () => {
    if (!memberIdInput.value.trim()) resetActivityView();
  });
  memberIdInput?.addEventListener('blur', normalizeMemberInput);
  memberIdInput?.addEventListener('keyup', (event) => {
    if (event.key === 'Enter') memberIdChanged({ loadActivityNow: event.isTrusted !== false });
  });

  if (memberActionInput && memberActionInput !== memberIdInput) {
    memberActionInput.addEventListener('blur', () => {
      normalizeMemberInput();
    });
    memberActionInput.addEventListener('input', () => {
      if (!memberActionInput.value.trim()) {
        syncMemberInputs('');
        resetActivityView();
      }
    });
  }

  async function handleMemberInfoAction() {
    let userId;
    try {
      userId = await resolveMemberForActions();
    } catch (error) {
      const message = error?.message || 'Member not found';
      if (message !== 'Select a family first') {
        toast(message, 'error');
        memberIdInput?.focus();
      }
      return;
    }
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      return;
    }
    setMemberStatus('');
    setMemberInfoMessage('Loading member info...');
    clearMemberLedger();
    try {
      loadActivity();
      const url = appendFamilyQuery(`/api/admin/members/${encodeURIComponent(userId)}`, familyId);
      const { res, body } = await adminFetch(url);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        setMemberInfoMessage('Admin key invalid.');
        return;
      }
      if (res.status === 404) {
        setMemberInfoMessage(`No profile found for "${userId}".`);
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load member');
        throw new Error(msg);
      }
      const member = body && typeof body === 'object' ? body : null;
      renderMemberInfo(member);
      if (member) {
        setMemberStatus(`Loaded member ${member.userId}.`);
        clearMemberLedger();
      } else {
        clearMemberLedger('Redeemed rewards will appear here.');
      }
    } catch (err) {
      console.error(err);
      const message = err?.message || 'Failed to load member.';
      setMemberInfoMessage(message);
      toast(message, 'error');
      clearMemberLedger(message || 'Redeem history unavailable.');
    }
  }

  const memberInfoButtons = Array.from(
    new Set([
      $('btnMemberInfo'),
      document.querySelector('#btn-member-info')
    ].filter(Boolean))
  );
  memberInfoButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      void handleMemberInfoAction();
    });
  });

  memberActionInput?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      const targetBtn = document.querySelector('#btn-member-info') || $('btnMemberInfo');
      targetBtn?.click();
    }
  });

  async function handleMemberBalanceAction() {
    let userId;
    try {
      userId = await resolveMemberForActions();
    } catch (error) {
      const message = error?.message || 'Member not found';
      setMemberStatus(message);
      if (message !== 'Select a family first') {
        toast(message, 'error');
      }
      return;
    }
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      return;
    }
    loadActivity();
    setMemberStatus('Fetching balance...');
    if (memberBalanceContainer) memberBalanceContainer.hidden = false;
    collapseDetails(memberBalanceDetails);
    collapseDetails(memberEarnDetails);
    collapseDetails(memberRedeemDetails);
    collapseDetails(memberRefundDetails);
    if (memberBalanceSummaryValue) memberBalanceSummaryValue.textContent = 'Loading…';
    if (memberBalanceBody) setPlaceholder(memberBalanceBody, 'Loading balance…');
    const data = await refreshMemberLedger(userId, { showPanels: true, familyId });
    if (data?.hints) {
      const formattedBalance = formatTokenValue(Number(data.hints.balance ?? 0));
      setMemberStatus(`Balance: ${formattedBalance} tokens.`);
    } else {
      setMemberStatus('Balance unavailable.');
    }
  }

  const memberBalanceButtons = Array.from(
    new Set([
      $('btnMemberBalance'),
      document.querySelector('#btn-check-balance')
    ].filter(Boolean))
  );
  memberBalanceButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      void handleMemberBalanceAction();
    });
  });

  async function submitMemberRegistration() {
    const idEl = $('memberRegisterId');
    const nameEl = $('memberRegisterName');
    const dobEl = $('memberRegisterDob');
    const sexEl = $('memberRegisterSex');
    const userId = (idEl?.value || '').trim().toLowerCase();
    const name = (nameEl?.value || '').trim();
    const dob = (dobEl?.value || '').trim();
    const sex = (sexEl?.value || '').trim();
    if (!userId || !name) {
      toast('User ID and name required', 'error');
      return;
    }
    setMemberStatus('Registering member...');
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery('/api/admin/members', familyId);
      const payload = withFamilyInBody({
        userId,
        name,
        dob: dob || undefined,
        sex: sex || undefined
      }, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        setMemberStatus('Admin key invalid.');
        return;
      }
      if (res.status === 409) {
        throw new Error('User ID already exists');
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to register');
        throw new Error(msg);
      }
      toast('Member registered');
      setMemberStatus(`Registered member ${userId}.`);
      if (idEl) idEl.value = '';
      if (nameEl) nameEl.value = '';
      if (dobEl) dobEl.value = '';
      if (sexEl) sexEl.value = '';
      if (memberIdInput) {
        syncMemberInputs(userId);
        memberIdChanged();
      }
      await loadMembersList();
    } catch (err) {
      console.error(err);
      setMemberStatus(err.message || 'Failed to register member.');
      toast(err.message || 'Failed to register member', 'error');
    }
  }

  $('btnMemberRegister')?.addEventListener('click', submitMemberRegistration);

  function autoFormatDobField(input) {
    if (!input) return;
    const formatted = formatDobFriendly(input.value);
    input.value = formatted;
  }

  function openMemberEditModal(member) {
    if (!memberEditModal || !memberEditForm) return;
    activeMemberEdit = { ...member };
    if (memberEditIdInput) memberEditIdInput.value = member.userId || '';
    if (memberEditNameInput) memberEditNameInput.value = member.name || '';
    if (memberEditDobInput) {
      memberEditDobInput.value = member.dob ? formatDobFriendly(member.dob) : '';
      autoFormatDobField(memberEditDobInput);
    }
    if (memberEditSexSelect) {
      const normalizedSex = normalizeSexValue(member.sex) || '';
      memberEditSexSelect.value = normalizedSex;
    }
    memberEditModal.classList.remove('hidden');
    if (memberEditNameInput) memberEditNameInput.focus();
  }

  function closeMemberEditModal() {
    if (!memberEditModal) return;
    memberEditModal.classList.add('hidden');
    memberEditForm?.reset?.();
    if (memberEditSaveBtn) {
      memberEditSaveBtn.disabled = false;
      memberEditSaveBtn.textContent = MEMBER_EDIT_SAVE_DEFAULT_TEXT;
    }
    activeMemberEdit = null;
  }

  memberEditDobInput?.addEventListener('blur', () => autoFormatDobField(memberEditDobInput));
  memberEditDobInput?.addEventListener('change', () => autoFormatDobField(memberEditDobInput));

  memberEditForm?.addEventListener('submit', async (event) => {
    event?.preventDefault();
    if (!activeMemberEdit) {
      closeMemberEditModal();
      return;
    }
    const userId = activeMemberEdit.userId;
    const name = memberEditNameInput?.value?.trim() || '';
    if (!name) {
      toast('Name required', 'error');
      memberEditNameInput?.focus?.();
      return;
    }
    const dob = normalizeDobInput(memberEditDobInput?.value || '');
    const sexRaw = memberEditSexSelect?.value || '';
    const sex = normalizeSexValue(sexRaw) || '';
    if (memberEditSaveBtn) {
      memberEditSaveBtn.disabled = true;
      memberEditSaveBtn.textContent = 'Saving...';
    }
    try {
      const familyId = requireFamilyId();
      const payload = withFamilyInBody({ name, dob: dob || undefined, sex: sex || undefined }, familyId);
      const url = appendFamilyQuery(`/api/admin/members/${encodeURIComponent(userId)}`, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to update member');
        throw new Error(msg);
      }
      toast('Member updated');
      const updated = body && typeof body === 'object' ? body.member || body : null;
      closeMemberEditModal();
      await loadMembersList();
      if (updated && memberIdInput?.value === updated.userId) {
        renderMemberInfo(updated);
      }
    } catch (err) {
      console.error(err);
      toast(err.message || 'Failed to update member', 'error');
    } finally {
      if (memberEditSaveBtn && memberEditModal && !memberEditModal.classList.contains('hidden')) {
        memberEditSaveBtn.disabled = false;
        memberEditSaveBtn.textContent = MEMBER_EDIT_SAVE_DEFAULT_TEXT;
      }
    }
  });

  function editMember(member) {
    if (!member) return;
    openMemberEditModal(member);
  }

  async function deleteMember(member) {
    if (!member) return;
    if (!confirm(`Delete member "${member.userId}"? This cannot be undone.`)) return;
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery(`/api/admin/members/${encodeURIComponent(member.userId)}`, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'DELETE'
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        return;
      }
      if (res.status === 404) {
        toast('Member already removed', 'error');
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to delete member');
        throw new Error(msg);
      }
      toast('Member deleted');
      if (memberIdInput?.value === member.userId) {
        syncMemberInputs('');
        memberIdChanged();
      }
      await loadMembersList();
    } catch (err) {
      console.error(err);
      toast(err.message || 'Failed to delete member', 'error');
    }
  }

  async function loadMembersList() {
    if (!memberTableBody) return;
    const search = (memberSearchInput?.value || '').trim().toLowerCase();
    if (!search) {
      memberTableBody.innerHTML = '';
      if (memberListStatus) memberListStatus.textContent = 'Type in the search box to list members.';
      if (memberListSection) memberListSection.hidden = false;
      return;
    }
    if (memberListSection) memberListSection.hidden = false;
    memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">Loading...</td></tr>';
    if (memberListStatus) memberListStatus.textContent = '';
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">Select a family to search members.</td></tr>';
      if (memberListStatus) memberListStatus.textContent = 'Select a family to search members.';
      return;
    }
    try {
      const params = new URLSearchParams();
      if (search) params.set('search', search);
      params.set('family_id', familyId);
      const qs = params.toString() ? `?${params.toString()}` : '';
      const { res, body } = await adminFetch(`/api/admin/members${qs}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">Admin key invalid.</td></tr>';
        if (memberListStatus) memberListStatus.textContent = 'Admin key invalid.';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load members');
        throw new Error(msg);
      }
      const rows = Array.isArray(body) ? body : [];
      memberTableBody.innerHTML = '';
      if (!rows.length) {
        memberTableBody.innerHTML = '<tr><td colspan="5" class="muted">No members found.</td></tr>';
        if (memberListStatus) memberListStatus.textContent = 'No members found.';
        return;
      }
      for (const row of rows) {
        const tr = document.createElement('tr');
        const idCell = document.createElement('td');
        idCell.textContent = row.userId;
        tr.appendChild(idCell);
        const nameCell = document.createElement('td');
        nameCell.textContent = row.name || '';
        tr.appendChild(nameCell);
        const dobCell = document.createElement('td');
        dobCell.textContent = formatDobFriendly(row.dob || '') || '';
        tr.appendChild(dobCell);
        const sexCell = document.createElement('td');
        sexCell.textContent = normalizeSexValue(row.sex) || row.sex || '';
        tr.appendChild(sexCell);
        const actions = document.createElement('td');
        actions.style.display = 'flex';
        actions.style.flexWrap = 'wrap';
        actions.style.gap = '6px';
        actions.style.alignItems = 'center';

        const editBtn = document.createElement('button');
        editBtn.textContent = 'Edit';
        editBtn.addEventListener('click', () => editMember(row));
        actions.appendChild(editBtn);

        const deleteBtn = document.createElement('button');
        deleteBtn.textContent = 'Delete';
        deleteBtn.addEventListener('click', () => deleteMember(row));
        actions.appendChild(deleteBtn);

        tr.appendChild(actions);
        memberTableBody.appendChild(tr);
      }
      if (memberListStatus) {
        const count = rows.length;
        memberListStatus.textContent = `Showing ${count} member${count === 1 ? '' : 's'}.`;
      }
    } catch (err) {
      console.error(err);
      memberTableBody.innerHTML = `<tr><td colspan="5" class="muted">${err.message || 'Failed to load members.'}</td></tr>`;
      if (memberListStatus) memberListStatus.textContent = err.message || 'Failed to load members.';
    }
  }

  $('btnMemberReload')?.addEventListener('click', loadMembersList);

  let memberSearchTimer = null;
  memberSearchInput?.addEventListener('input', () => {
    clearTimeout(memberSearchTimer);
    memberSearchTimer = setTimeout(loadMembersList, 250);
  });

  function parseTokenFromScan(data) {
    try {
      if (!data) return null;
      let token = data.trim();
      let url = '';
      if (token.startsWith('http')) {
        const urlObj = new URL(token);
        if (urlObj.searchParams.get('t')) token = urlObj.searchParams.get('t');
        url = urlObj.toString();
      }
      const part = token.split('.')[0];
      const padded = part.replace(/-/g, '+').replace(/_/g, '/');
      const mod = padded.length % 4;
      const base = mod ? padded + '='.repeat(4 - mod) : padded;
      const payload = JSON.parse(atob(base));
      if (!url && typeof window !== 'undefined' && window.location) {
        url = `${window.location.origin}/scan?t=${encodeURIComponent(token)}`;
      }
      return { token, payload, url };
    } catch (e) {
      console.error('parse token failed', e);
      return null;
    }
  }

  async function generateIssueQr() {
    const userId = requireMemberId();
    if (!userId) return;
    const amount = Number($('issueAmount').value);
    const note = $('issueNote').value.trim();
    if (!Number.isFinite(amount) || amount <= 0) {
      toast('Enter a positive amount', 'error');
      return;
    }
    $('issueStatus').textContent = 'Generating QR...';
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery('/api/tokens/give', familyId);
      const payload = withFamilyInBody({ userId, amount, note: note || undefined }, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = body && typeof body === 'object' ? body : {};
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        $('issueStatus').textContent = 'Admin key invalid.';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'request failed');
        throw new Error(msg);
      }
      renderQr('qrIssue', data.qrText);
      $('issueLink').value = data.qrText || '';
      $('issueStatus').textContent = `QR expires in 2 minutes. Amount ${data.amount ?? '?'} points.`;
      toast('QR ready');
    } catch (err) {
      console.error(err);
      if (err?.message === 'family_id required') {
        $('issueStatus').textContent = 'Select a family first.';
        return;
      }
      $('issueStatus').textContent = 'Failed to generate QR.';
      toast(err.message || 'Failed', 'error');
    }
  }
  $('btnIssueGenerate')?.addEventListener('click', generateIssueQr);

  $('btnIssueCopy')?.addEventListener('click', () => {
    const text = $('issueLink').value;
    if (!text) return toast('Nothing to copy', 'error');
    navigator.clipboard?.writeText(text).then(() => toast('Link copied')).catch(() => toast('Copy failed', 'error'));
  });

  // ===== Holds =====
  const holdsTable = $('holdsTable')?.querySelector('tbody');
  async function loadHolds() {
    if (!holdsTable) return;
    const status = $('holdFilter')?.value || 'pending';
    const memberInfo = normalizeMemberInput() || {};
    const rawUserId = (memberInfo.raw || '').trim();
    const normalizedUser = (memberInfo.normalized || '').trim();
    holdsTable.innerHTML = '';
    if (!normalizedUser) {
      const msg = 'Enter a user ID in Member Management to view holds.';
      $('holdsStatus').textContent = msg;
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 6;
      cell.className = 'muted';
      cell.textContent = msg;
      row.appendChild(cell);
      holdsTable.appendChild(row);
      return;
    }
    $('holdsStatus').textContent = 'Loading...';
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      const msg = 'Select a family to view holds.';
      $('holdsStatus').textContent = msg;
      holdsTable.innerHTML = `<tr><td colspan="6" class="muted">${msg}</td></tr>`;
      return;
    }
    try {
      const params = new URLSearchParams({ status });
      if (normalizedUser) params.set('userId', normalizedUser);
      params.set('family_id', familyId);
      const { res, body } = await adminFetch(`/api/holds?${params.toString()}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        $('holdsStatus').textContent = 'Admin key invalid.';
        holdsTable.innerHTML = '<tr><td colspan="6" class="muted">Admin key invalid.</td></tr>';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'failed');
        throw new Error(msg);
      }
      const rows = Array.isArray(body) ? body : [];
      if (!rows.length) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = 6;
        cell.className = 'muted';
        cell.textContent = `No holds for "${rawUserId}".`;
        row.appendChild(cell);
        holdsTable.appendChild(row);
        $('holdsStatus').textContent = `No holds for "${rawUserId}".`;
        return;
      }
      const frag = document.createDocumentFragment();
      for (const row of rows) {
        const tr = document.createElement('tr');
        tr.dataset.holdId = row.id;
        tr.innerHTML = `
          <td>${formatTime(row.createdAt)}</td>
          <td>${row.userId}</td>
          <td>${row.itemName || ''}</td>
          <td>${row.quotedCost ?? ''}</td>
          <td>${row.status}</td>
          <td class="actions"></td>
        `;
        const actions = tr.querySelector('.actions');
        if (row.status === 'pending') {
          const cancelBtn = document.createElement('button');
          cancelBtn.textContent = 'Cancel';
          cancelBtn.addEventListener('click', () => cancelHold(row.id));
          actions.appendChild(cancelBtn);
        } else {
          actions.textContent = '-';
        }
        frag.appendChild(tr);
      }
      holdsTable.appendChild(frag);
      const count = rows.length;
      const suffix = count === 1 ? '' : 's';
      $('holdsStatus').textContent = `Showing ${count} hold${suffix} for "${rawUserId}".`;
    } catch (err) {
      console.error(err);
      $('holdsStatus').textContent = err.message || 'Failed to load holds';
    }
  }
  $('btnReloadHolds')?.addEventListener('click', loadHolds);
  $('holdFilter')?.addEventListener('change', loadHolds);

  memberEditModal?.addEventListener('click', (event) => {
    const target = event.target;
    if (target?.dataset?.close !== undefined || target === memberEditModal || target?.classList?.contains('modal-backdrop')) {
      event.preventDefault();
      closeMemberEditModal();
    }
  });

  refundModal?.addEventListener('click', (event) => {
    const target = event.target;
    if (target?.dataset?.close !== undefined || target === refundModal || target?.classList?.contains('modal-backdrop')) {
      event.preventDefault();
      closeRefundModal();
    }
  });

  window.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && memberEditModal && !memberEditModal.classList.contains('hidden')) {
      closeMemberEditModal();
    }
    if (event.key === 'Escape' && refundModal && !refundModal.classList.contains('hidden')) {
      closeRefundModal();
    }
  });

  refundForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    if (!activeRefundContext) {
      toast('Select a redeem entry to refund.', 'error');
      return;
    }
    const userId = activeRefundContext.userId || activeRefundContext.user_id || '';
    const remaining = Math.max(0, Number(activeRefundContext.remaining_refundable || 0));
    const amount = Number(refundAmountInput?.value || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      toast('Enter a positive refund amount.', 'error');
      return;
    }
    if (remaining && amount > remaining + 0.0001) {
      toast(`Amount exceeds remaining refundable tokens (${remaining}).`, 'error');
      return;
    }
    const familyId = readCurrentFamilyId();
    if (!familyId) {
      toast('Select a family first', 'error');
      return;
    }
    const payload = {
      user_id: userId,
      redeem_tx_id: activeRefundContext.id,
      amount,
      reason: refundReasonSelect?.value || 'duplicate',
      notes: (refundNotesInput?.value || '').trim() || undefined,
      idempotency_key: generateIdempotencyKey(),
      family_id: familyId
    };
    refundConfirmBtn.disabled = true;
    refundConfirmBtn.textContent = 'Processing…';
    try {
      const url = appendFamilyQuery('/ck/refund', familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        refundConfirmBtn.disabled = false;
        refundConfirmBtn.textContent = 'Confirm refund';
        return;
      }
      if (res.status === 429) {
        const retry = Number(body?.retry_after_ms || 0);
        const seconds = retry ? Math.ceil(retry / 1000) : null;
        const msg = seconds ? `Too many refunds. Try again in ${seconds} seconds.` : 'Too many refunds right now. Try again soon.';
        throw new Error(msg);
      }
      if (res.status === 409 && body && typeof body === 'object') {
        toast(presentError(body.error || 'REFUND_EXISTS', 'This refund was already recorded.'), 'warning');
        closeRefundModal();
        await refreshMemberLedger(userId, { familyId });
        applyStateHints(body.hints || null);
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Refund failed');
        throw new Error(msg);
      }
      toast(`${amount} tokens returned.`, 'success');
      closeRefundModal();
      await refreshMemberLedger(userId, { familyId });
      applyStateHints(body?.hints || null);
      loadHolds();
    } catch (err) {
      console.error(err);
      toast(presentError(err.message, 'Refund failed'), 'error');
      refundConfirmBtn.disabled = false;
      refundConfirmBtn.textContent = 'Confirm refund';
    }
  });

  async function cancelHold(id) {
    if (!confirm('Cancel this hold?')) return;
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery(`/api/holds/${encodeURIComponent(id)}/cancel`, familyId);
      const { res, body } = await adminFetch(url, { method: 'POST' });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Cancel failed');
        throw new Error(msg);
      }
      toast('Hold released');
      applyStateHints(body?.hints || null);
      loadHolds();
    } catch (err) {
      toast(presentError(err.message, 'Cancel failed'), 'error');
    }
  }

  function renderActivity(rows = [], { emptyMessage = 'No activity yet.' } = {}) {
    if (!activityTableBody) return;
    activityRowIndex.clear();
    activityTableBody.innerHTML = '';
    if (!rows.length) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 9;
      td.className = 'muted';
      td.textContent = emptyMessage;
      tr.appendChild(td);
      activityTableBody.appendChild(tr);
      return;
    }
    const frag = document.createDocumentFragment();
    for (const row of rows) {
      const tr = document.createElement('tr');
      const txId = row.id ? String(row.id) : '';
      if (txId) {
        tr.dataset.txId = txId;
        activityRowIndex.set(txId, tr);
      }
      if (row.parent_tx_id) {
        tr.dataset.parentId = String(row.parent_tx_id);
      }
      const deltaNum = Number(row.delta || 0);
      const deltaText = `${deltaNum > 0 ? '+' : ''}${formatTokenValue(deltaNum)}`;
      const balanceText = formatTokenValue(row.balance_after ?? row.balanceAfter ?? 0);
      const cells = [
        formatTime(row.at),
        row.userId || '',
        row.verb || '',
        row.action || '',
        deltaText,
        balanceText,
        row.actor || '',
        row.parent_tx_id || '',
        txId
      ];
      cells.forEach((value, idx) => {
        const td = document.createElement('td');
        if ((idx === 7 || idx === 8) && value) {
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'link-button';
          btn.dataset.linkTarget = String(value);
          btn.textContent = value;
          td.appendChild(btn);
        } else {
          td.textContent = value ?? '';
        }
        tr.appendChild(td);
      });
      frag.appendChild(tr);
    }
    activityTableBody.appendChild(frag);
    activityTableBody.querySelectorAll('.link-button').forEach(btn => {
      btn.addEventListener('click', () => {
        const targetId = btn.getAttribute('data-link-target');
        if (!targetId) return;
        const target = activityRowIndex.get(targetId);
        if (target) {
          target.classList.add('activity-highlight');
          target.scrollIntoView({ behavior: 'smooth', block: 'center' });
          setTimeout(() => target.classList.remove('activity-highlight'), 1500);
        } else {
          toast('Transaction not in the current view.', 'info');
        }
      });
    });
  }

  const triggerActivityLoad = debounce(() => loadActivity(), 300);

  function resetActivityView(message = 'Enter a user ID to view activity.') {
    renderActivity([], { emptyMessage: message });
    if (activityStatus) activityStatus.textContent = message;
  }

  async function loadActivity() {
    if (!activityTableBody) return;
    const memberInfo = getMemberIdInfo();
    const user = (memberInfo?.normalized || '').trim();
    if (!user) {
      if (activityStatus) activityStatus.textContent = 'Enter a user ID to view activity.';
      renderActivity([], { emptyMessage: 'Enter a user ID to view activity.' });
      return;
    }
    const params = new URLSearchParams();
    const verb = (activityVerb?.value || 'all').toLowerCase();
    if (verb && verb !== 'all') params.set('verb', verb);
    params.set('userId', user);
    const actor = (activityActor?.value || '').trim();
    if (actor) params.set('actor', actor);
    if (activityFrom?.value) params.set('from', activityFrom.value);
    if (activityTo?.value) params.set('to', activityTo.value);
    params.set('limit', '100');
    params.set('offset', '0');
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      const message = 'Select a family to view activity.';
      if (activityStatus) activityStatus.textContent = message;
      renderActivity([], { emptyMessage: message });
      return;
    }
    params.set('family_id', familyId);
    const qs = params.toString() ? `?${params.toString()}` : '';
    if (activityStatus) activityStatus.textContent = 'Loading activity…';
    activityTableBody.innerHTML = '';
    try {
      const { res, body } = await adminFetch(`/api/history${qs}`);
      if (res.status === 401) {
        toast(ADMIN_INVALID_MSG, 'error');
        if (activityStatus) activityStatus.textContent = 'Admin key invalid.';
        renderActivity([], { emptyMessage: 'Admin key invalid.' });
        return;
      }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Activity load failed');
        throw new Error(msg);
      }
      const rows = Array.isArray(body?.rows) ? body.rows : [];
      renderActivity(rows);
      if (activityStatus) {
        activityStatus.textContent = rows.length
          ? `Showing ${rows.length} item${rows.length === 1 ? '' : 's'}.`
          : 'No activity matches your filters.';
      }
    } catch (err) {
      console.error(err);
      if (activityStatus) activityStatus.textContent = presentError(err.message, 'Activity unavailable.');
      renderActivity([], { emptyMessage: 'Activity unavailable.' });
    }
  }

  function formatTime(ms) {
    if (!ms) return '';
    try {
      return new Date(ms).toLocaleString();
    } catch {
      return ms;
    }
  }

  // ===== Scanner helpers =====
  function setupScanner({ buttonId, videoId, canvasId, statusId, onToken }) {
    const btn = $(buttonId);
    const video = $(videoId);
    const canvas = $(canvasId);
    const status = $(statusId);
    if (!btn || !video || !canvas) return;

    const ctx = canvas.getContext('2d');
    let stream = null;
    let raf = 0;
    let busy = false;

    function say(msg) { if (status) status.textContent = msg || ''; }

    async function start() {
      if (stream) { stop(); return; }
      if (!navigator.mediaDevices?.getUserMedia) {
        say('Camera not available');
        return;
      }
      try {
        stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' } } });
        video.srcObject = stream;
        video.style.display = 'block';
        await video.play();
        say('Point camera at QR');
        tick();
      } catch (err) {
        console.error(err);
        say('Camera blocked or unavailable');
      }
    }

    function stop() {
      cancelAnimationFrame(raf);
      if (stream) stream.getTracks().forEach(t => t.stop());
      stream = null;
      video.pause();
      video.srcObject = null;
      video.style.display = 'none';
      say('Camera stopped');
    }

    function tick() {
      if (!stream) return;
      if (video.readyState === video.HAVE_ENOUGH_DATA) {
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
        const img = ctx.getImageData(0, 0, canvas.width, canvas.height);
        const code = jsQR(img.data, img.width, img.height, { inversionAttempts: 'dontInvert' });
        if (code?.data && !busy) {
          busy = true;
          say('QR detected...');
          onToken(code.data).finally(() => {
            busy = false;
            say('Ready for next scan');
          });
        }
      }
      raf = requestAnimationFrame(tick);
    }

    btn.addEventListener('click', () => {
      if (stream) stop(); else start();
    });
  }

// Hold scanner — APPROVE spend token
setupScanner({
  buttonId: 'btnHoldCamera',
  videoId: 'holdVideo',
  canvasId: 'holdCanvas',
  statusId: 'holdScanStatus',
  onToken: async (raw) => {
    const parsed = parseTokenFromScan(raw);
    if (!parsed || parsed.payload.typ !== 'spend') {
      toast('Not a spend token', 'error');
      return;
    }

    const holdId = parsed.payload.data?.holdId;
    if (!holdId) {
      toast('Hold id missing', 'error');
      return;
    }

    const targetUrl = parsed.url || `${window.location.origin}/scan?t=${encodeURIComponent(parsed.token)}`;
    say('Opening approval page...');
    const opened = window.open(targetUrl, '_blank', 'noopener');
    if (!opened) {
      window.location.href = targetUrl;
    }
  },  // ← keep this comma
});   // ← and this closer

// Earn scanner — GENERATE earn/give token
setupScanner({
  buttonId: 'btnEarnCamera',
  videoId: 'earnVideo',
  canvasId: 'earnCanvas',
  statusId: 'earnScanStatus',
  onToken: async (raw) => {
    const parsed = parseTokenFromScan(raw);
    if (!parsed || !['earn', 'give'].includes(parsed.payload.typ)) {
      toast('Unsupported token', 'error');
      return;
    }
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery('/api/earn/scan', familyId);
      const payload = withFamilyInBody({ token: parsed.token }, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Scan failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      toast(`Credited ${data.amount} to ${data.userId}`);
    } catch (err) {
      toast(err.message || 'Scan failed', 'error');
    }
  },
}); // must close the call

  // ===== Rewards =====
  function applyUrlToggle(show) {
    document.body.classList.toggle('hide-urls', !show);
  }

  function appendMediaUrl(container, label, url) {
    if (!container || !url) return;
    const row = document.createElement('div');
    row.className = 'muted mono media-url';
    row.style.flex = '1 1 100%';
    row.style.minWidth = '0';
    row.style.display = 'flex';
    row.style.alignItems = 'baseline';
    row.style.gap = '4px';
    const strong = document.createElement('strong');
    strong.textContent = `${label}:`;
    row.appendChild(strong);
    const value = document.createElement('span');
    value.textContent = url;
    value.style.flex = '1';
    value.style.minWidth = '0';
    value.style.wordBreak = 'break-all';
    row.appendChild(value);
    container.appendChild(row);
  }
  const SHOW_URLS_KEY = 'ck_show_urls';
  (function initToggle() {
    const toggle = $('adminShowUrls');
    if (!toggle) return;
    const saved = storageGet(SHOW_URLS_KEY);
    const show = saved === '1';
    toggle.checked = show;
    applyUrlToggle(show);
    toggle.addEventListener('change', () => {
      storageSet(SHOW_URLS_KEY, toggle.checked ? '1' : '0');
      applyUrlToggle(toggle.checked);
      loadRewards();
    });
  })();

  function editReward(item) {
    const isMasterLinked = item && (item.source === 'master' || (item.master_reward_id && String(item.master_reward_id).trim()));
    if (isMasterLinked) {
      const costPrompt = prompt('Cost (points)', Number.isFinite(item.cost) ? String(item.cost) : '');
      if (costPrompt === null) return;
      const cost = Number(costPrompt.trim());
      if (!Number.isFinite(cost) || cost < 0) {
        toast('Cost must be a non-negative number', 'error');
        return;
      }
      updateReward(item.id, { cost });
      return;
    }

    const nameInput = prompt('Reward name', item.name || '');
    if (nameInput === null) return;
    const name = nameInput.trim();
    if (!name) {
      toast('Reward name required', 'error');
      return;
    }

    const costPrompt = prompt('Cost (points)', Number.isFinite(item.cost) ? String(item.cost) : '');
    if (costPrompt === null) return;
    const cost = Number(costPrompt.trim());
    if (!Number.isFinite(cost) || cost < 0) {
      toast('Cost must be a non-negative number', 'error');
      return;
    }

    const imagePrompt = prompt('Image URL (optional)', item.imageUrl || '');
    if (imagePrompt === null) return;
    const imageUrl = imagePrompt.trim();

    const youtubePrompt = prompt('YouTube URL (optional)', item.youtubeUrl || '');
    if (youtubePrompt === null) return;
    const youtubeUrl = youtubePrompt.trim();

    const descPrompt = prompt('Description (optional)', item.description || '');
    if (descPrompt === null) return;
    const description = descPrompt.trim();

    const payload = { name, cost, description };
    payload.imageUrl = imageUrl || null;
    payload.youtubeUrl = youtubeUrl || null;
    updateReward(item.id, payload);
  }

  const rewardsInactiveBtn = $('btnShowInactiveRewards');
  let rewardsStatusFilter = 'active';
  let rewardsToggleInitialized = false;

  function updateRewardsToggleButton() {
    if (!rewardsInactiveBtn) return;
    const showingDisabled = rewardsStatusFilter === 'disabled';
    if (!rewardsToggleInitialized) {
      rewardsInactiveBtn.hidden = true;
      return;
    }
    rewardsInactiveBtn.hidden = false;
    rewardsInactiveBtn.disabled = false;
    rewardsInactiveBtn.setAttribute('aria-pressed', showingDisabled ? 'true' : 'false');
    rewardsInactiveBtn.classList.toggle('is-selected', showingDisabled);
    rewardsInactiveBtn.textContent = showingDisabled ? 'Show Active Rewards' : 'Show Deactivated Rewards';
  }

  async function loadRewards() {
    const list = $('rewardsList');
    if (!list) return;
    const statusEl = $('rewardsStatus');
    const filterValue = $('filterRewards')?.value?.toLowerCase?.() || '';
    list.innerHTML = '<div class="muted">Loading...</div>';
    if (statusEl) statusEl.textContent = '';
    updateRewardsToggleButton();
    let familyId;
    try {
      familyId = requireFamilyId();
    } catch (error) {
      const message = 'Select a family to view rewards.';
      list.innerHTML = `<div class="muted">${message}</div>`;
      if (statusEl) statusEl.textContent = message;
      rewardsToggleInitialized = false;
      updateRewardsToggleButton();
      return;
    }
    try {
      const params = new URLSearchParams();
      if (rewardsStatusFilter === 'active') {
        params.set('active', '1');
      } else if (rewardsStatusFilter === 'disabled') {
        params.set('status', 'disabled');
      }
      params.set('family_id', familyId);
      const qs = params.toString() ? `?${params.toString()}` : '';
      const { res, body } = await adminFetch(`/api/admin/rewards${qs}`);
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        list.innerHTML = '<div class="muted">Admin key invalid.</div>';
        rewardsToggleInitialized = false;
        updateRewardsToggleButton();
        return;
      }
      if (!res.ok){
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'Failed to load rewards');
        throw new Error(msg);
      }
      const items = Array.isArray(body) ? body : [];
      list.innerHTML = '';
      const normalized = items.map(item => ({
        id: item.id,
        name: (item.name || item.title || '').trim(),
        cost: Number.isFinite(Number(item.cost)) ? Number(item.cost) : Number(item.price || 0),
        description: item.description || '',
        imageUrl: item.imageUrl || item.image_url || '',
        image_url: item.image_url || item.imageUrl || '',
        youtubeUrl: item.youtubeUrl || item.youtube_url || '',
        youtube_url: item.youtube_url || item.youtubeUrl || '',
        status: (item.status || (item.active ? 'active' : 'disabled') || 'active').toString().toLowerCase(),
        active: Number(item.active ?? (item.status === 'disabled' ? 0 : 1)) ? 1 : 0,
        source: item.source || null,
        master_reward_id: item.master_reward_id || null
      }));
      rewardsToggleInitialized = true;
      updateRewardsToggleButton();
      const filtered = normalized.filter(it => {
        const matchesFilter = !filterValue || it.name.toLowerCase().includes(filterValue);
        if (!matchesFilter) return false;
        if (rewardsStatusFilter === 'active') return it.status !== 'disabled';
        if (rewardsStatusFilter === 'disabled') return it.status === 'disabled';
        return true;
      });
      const showUrls = document.getElementById('adminShowUrls')?.checked;
      for (const item of filtered) {
        const card = document.createElement('div');
        card.className = 'reward-card';
        card.style.display = 'flex';
        card.style.alignItems = 'center';
        card.style.gap = '12px';
        card.style.background = '#fff';
        card.style.border = '1px solid var(--line)';
        card.style.borderRadius = '10px';
        card.style.padding = '12px';

        const thumb = document.createElement('img');
        thumb.className = 'reward-thumb';
        thumb.src = item.imageUrl || '';
        thumb.alt = '';
        thumb.loading = 'lazy';
        thumb.width = 96;
        thumb.height = 96;
        thumb.style.objectFit = 'cover';
        thumb.style.aspectRatio = '1/1';
        thumb.addEventListener('click', () => { if (thumb.src) openImageModal(thumb.src); });
        if (thumb.src){
          card.appendChild(thumb);
        } else {
          const spacer = document.createElement('div');
          spacer.style.width = '96px';
          spacer.style.height = '96px';
          spacer.style.flex = '0 0 auto';
          card.appendChild(spacer);
        }

        const youtubeThumbUrl = getYouTubeThumbnail(item.youtubeUrl);
        if (youtubeThumbUrl) {
          const ytThumb = document.createElement('img');
          ytThumb.src = youtubeThumbUrl;
          ytThumb.alt = 'YouTube preview';
          ytThumb.loading = 'lazy';
          ytThumb.width = 72;
          ytThumb.height = 54;
          ytThumb.style.objectFit = 'cover';
          ytThumb.style.aspectRatio = '4 / 3';
          ytThumb.style.borderRadius = '8px';
          ytThumb.style.flex = '0 0 auto';
          ytThumb.style.cursor = 'pointer';
          ytThumb.style.boxShadow = '0 2px 6px rgba(0,0,0,0.15)';
          ytThumb.title = 'Open YouTube video';
          ytThumb.dataset.youtube = item.youtubeUrl;
          ytThumb.addEventListener('click', () => {
            const url = ytThumb.dataset.youtube;
            if (url) {
              openVideoModal(url);
            }
          });
          ytThumb.addEventListener('error', () => ytThumb.remove());
          card.appendChild(ytThumb);
        }

        const info = document.createElement('div');
        info.style.flex = '1 1 auto';
        const title = document.createElement('div');
        title.style.fontWeight = '600';
        title.textContent = item.name || 'Reward';
        info.appendChild(title);

        const cost = document.createElement('div');
        cost.className = 'muted';
        cost.textContent = `${item.cost || 0} points`;
        info.appendChild(cost);

        if (item.description) {
          const desc = document.createElement('div');
          desc.className = 'muted';
          desc.textContent = item.description;
          info.appendChild(desc);
        }

        const isMasterLinkedReward = item.source === 'master' || (item.master_reward_id && String(item.master_reward_id).trim());

        if (!item.active) {
          const badge = document.createElement('div');
          badge.className = 'muted';
          badge.textContent = 'Inactive';
          info.appendChild(badge);
          card.style.opacity = '0.6';
        }

        if (isMasterLinkedReward) {
          const origin = document.createElement('div');
          origin.className = 'reward-origin-badge';
          origin.textContent = 'Master Template';
          info.appendChild(origin);
        }

        card.appendChild(info);

        if (showUrls){
          if (item.imageUrl) appendMediaUrl(card, 'Image', item.imageUrl);
          if (item.youtubeUrl) appendMediaUrl(card, 'YouTube', item.youtubeUrl);
        }

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.flexDirection = 'column';
        actions.style.gap = '6px';
        actions.style.flex = '0 0 auto';
        actions.style.marginLeft = 'auto';

        if (item.youtubeUrl) {
          const watchBtn = document.createElement('button');
          watchBtn.type = 'button';
          watchBtn.className = 'btn btn-sm';
          watchBtn.textContent = 'Watch clip';
          watchBtn.dataset.youtube = item.youtubeUrl;
          watchBtn.addEventListener('click', () => {
            const url = watchBtn.dataset.youtube;
            if (url) openVideoModal(url);
          });
          actions.appendChild(watchBtn);
        }

        const editBtn = document.createElement('button');
        editBtn.textContent = isMasterLinkedReward ? 'Adjust cost' : 'Edit';
        if (isMasterLinkedReward) {
          editBtn.title = 'Title and description managed by master template';
        }
        editBtn.addEventListener('click', () => editReward(item));
        actions.appendChild(editBtn);

        const isDisabled = item.status === 'disabled' || !item.active;
        if (rewardsStatusFilter === 'disabled') {
          const reactivateBtn = document.createElement('button');
          reactivateBtn.textContent = 'Reactivate';
          reactivateBtn.addEventListener('click', () => updateReward(item.id, { active: 1 }));
          actions.appendChild(reactivateBtn);

          const deleteBtn = document.createElement('button');
          deleteBtn.type = 'button';
          deleteBtn.textContent = 'Delete permanently';
          deleteBtn.dataset.deleteReward = item.id;
          actions.appendChild(deleteBtn);
        } else {
          const toggleBtn = document.createElement('button');
          toggleBtn.textContent = isDisabled ? 'Activate' : 'Deactivate';
          toggleBtn.addEventListener('click', () => updateReward(item.id, { active: isDisabled ? 1 : 0 }));
          actions.appendChild(toggleBtn);
        }

        card.appendChild(actions);

        list.appendChild(card);
      }
      if (!filtered.length) {
        const emptyLabel = rewardsStatusFilter === 'disabled' ? 'No deactivated rewards.' : 'No rewards match.';
        list.innerHTML = `<div class="muted">${emptyLabel}</div>`;
      }
      if (statusEl) {
        const label = rewardsStatusFilter === 'disabled' ? 'deactivated' : 'active';
        statusEl.textContent = `Showing ${filtered.length} ${label} reward${filtered.length === 1 ? '' : 's'}.`;
      }
    } catch (err) {
      const msg = err.message || 'Failed to load rewards';
      if (statusEl) statusEl.textContent = msg;
      if (list) list.innerHTML = `<div class="muted">${msg}</div>`;
      rewardsToggleInitialized = false;
      updateRewardsToggleButton();
    }
  }
  $('btnLoadRewards')?.addEventListener('click', () => {
    rewardsStatusFilter = 'active';
    updateRewardsToggleButton();
    loadRewards();
  });
  $('filterRewards')?.addEventListener('input', loadRewards);

  rewardsInactiveBtn?.addEventListener('click', () => {
    rewardsStatusFilter = rewardsStatusFilter === 'disabled' ? 'active' : 'disabled';
    updateRewardsToggleButton();
    loadRewards();
  });

  async function updateReward(id, body) {
    let familyId;
    try {
      familyId = requireFamilyId();
    } catch {
      toast('Select a family to update rewards.', 'error');
      return;
    }
    try {
      const url = appendFamilyQuery(`/api/admin/rewards/${id}`, familyId);
      const payload = withFamilyInBody(body, familyId);
      const { res, body: respBody } = await adminFetch(url, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = (respBody && respBody.error) || (typeof respBody === 'string' ? respBody : 'update failed');
        throw new Error(msg);
      }
      toast('Reward updated');
      loadRewards();
    } catch (err) {
      if (err?.message === 'family_id required') return;
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function deleteReward(id) {
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      throw new Error('Select a family to delete rewards.');
    }
    const url = appendFamilyQuery(`/api/admin/rewards/${encodeURIComponent(id)}`, familyId);
    const { res, body } = await adminFetch(url, { method: 'DELETE' });
    if (res.status === 401) {
      throw new Error(ADMIN_INVALID_MSG);
    }
    if (res.status === 409) {
      throw new Error('Reward is referenced by existing records.');
    }
    if (!res.ok) {
      const msg = (body && body.error) || (typeof body === 'string' ? body : 'Delete failed');
      throw new Error(msg);
    }
    return true;
  }

  async function hardDeleteReward(id) {
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch {
      throw new Error('Select a family to delete rewards.');
    }
    const url = appendFamilyQuery(`/api/admin/rewards/${encodeURIComponent(id)}`, familyId);
    await apiFetch(url, { method: 'DELETE' });
  }

  async function reloadRewardsMenu() {
    await loadRewards();
  }

  document.addEventListener('click', async (event) => {
    const btn = event.target.closest('[data-delete-reward]');
    if (!btn) return;
    event.preventDefault();
    const rewardId = btn.getAttribute('data-delete-reward');
    if (!rewardId) return;
    if (!confirm('Delete this reward and its dependent records?')) return;
    try {
      if (adminState.role === 'master') {
        await hardDeleteReward(rewardId);
      } else {
        await deleteReward(rewardId);
      }
      toast('Reward deleted');
      await reloadRewardsMenu();
    } catch (err) {
      console.error(err);
      const message = err?.message === 'family_id required' ? 'Select a family to delete rewards.' : err.message || 'Delete failed';
      toast(message, 'error');
    }
  });

  document.getElementById('btnCreateReward')?.addEventListener('click', async (e)=>{
    e.preventDefault();
    const nameEl = document.getElementById('rewardName');
    const costEl = document.getElementById('rewardCost');
    const imageEl = document.getElementById('rewardImage');
    const youtubeEl = document.getElementById('rewardYoutube');
    const descEl = document.getElementById('rewardDesc');

    const name = nameEl?.value?.trim() || '';
    const cost = Number(costEl?.value || NaN);
    const imageUrl = imageEl?.value?.trim() || null;
    const youtubeUrl = youtubeEl?.value?.trim() || null;
    const description = descEl?.value?.trim() || '';
    if (!name || Number.isNaN(cost)) { toast('Name and numeric cost required', 'error'); return; }

    const familyId = requireFamilyId();
    const url = appendFamilyQuery('/api/admin/rewards', familyId);
    const payload = withFamilyInBody({ name, cost, imageUrl, youtubeUrl, description }, familyId);
    const { res, body } = await adminFetch(url, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });

    if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
    if (!res.ok){ toast((typeof body === 'string' ? body : body?.error) || 'Create failed', 'error'); return; }

    toast('Reward created');
    if (nameEl) nameEl.value = '';
    if (costEl) costEl.value = '1';
    if (imageEl) imageEl.value = '';
    if (youtubeEl) youtubeEl.value = '';
    if (descEl) descEl.value = '';
    loadRewards?.(); // refresh the list if available
  });

  updateRewardsToggleButton();

  // image upload
  const drop = $('drop');
  const fileInput = $('file');
  const uploadStatus = $('uploadStatus');
  if (drop && fileInput) {
    drop.addEventListener('click', () => fileInput.click());
    drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('drag'); });
    drop.addEventListener('dragleave', () => drop.classList.remove('drag'));
    drop.addEventListener('drop', (e) => {
      e.preventDefault(); drop.classList.remove('drag');
      if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
    });
    fileInput.addEventListener('change', () => {
      if (fileInput.files[0]) handleFile(fileInput.files[0]);
    });
  }

  async function handleFile(file) {
    try {
      uploadStatus.textContent = 'Uploading...';
      const base64 = await fileToDataUrl(file);
      const { res, body } = await adminFetch('/admin/upload-image64', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image64: base64 })
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); uploadStatus.textContent = 'Admin key invalid.'; return; }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'upload failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      $('rewardImage').value = data.url || '';
      uploadStatus.textContent = data.url ? `Uploaded: ${data.url}` : 'Uploaded';
    } catch (err) {
      uploadStatus.textContent = 'Upload failed';
      toast(err.message || 'Upload failed', 'error');
    }
  }

  function fileToDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(reader.error || new Error('read failed'));
      reader.readAsDataURL(file);
    });
  }

  // ===== Earn templates =====
  const earnTableBody = $('earnTable')?.querySelector('tbody');
  const btnViewDeactivated = $('btn-view-deactivated');
  const btnReloadActive = $('btn-reload-active');
  const earnTasksState = { mode: 'active', cachedActive: [] };
  let earnTemplates = [];

  function updateEarnTasksControls() {
    if (btnViewDeactivated) {
      btnViewDeactivated.disabled = earnTasksState.mode === 'inactive';
    }
    if (btnReloadActive) {
      btnReloadActive.disabled = earnTasksState.mode === 'active';
    }
  }

  async function reloadEarnPointsTable(modeOverride) {
    let familyId;
    try {
      familyId = requireFamilyId({ silent: true });
    } catch (error) {
      earnTemplates = [];
      if (earnTableBody) {
        earnTableBody.innerHTML = '<tr><td colspan="9" class="muted">Select a family to manage tasks.</td></tr>';
      }
      if (modeOverride !== undefined) {
        toast('Select a family to manage tasks.', 'error');
      }
      updateEarnTasksControls();
      return;
    }

    const mode = modeOverride || earnTasksState.mode || 'active';
    earnTasksState.mode = mode;
    updateEarnTasksControls();

    try {
      const params = new URLSearchParams({ family_id: familyId, mode });
      const data = await apiFetch(`/api/admin/earn-templates?${params.toString()}`);
      earnTemplates = Array.isArray(data) ? data : [];
      if (mode === 'active') {
        earnTasksState.cachedActive = earnTemplates.slice();
      } else {
        try {
          const activeParams = new URLSearchParams({ family_id: familyId, mode: 'active' });
          const activeRows = await apiFetch(`/api/admin/earn-templates?${activeParams.toString()}`);
          earnTasksState.cachedActive = Array.isArray(activeRows) ? activeRows : [];
        } catch (err) {
          console.warn('Failed to refresh active task cache', err);
        }
      }
      renderTemplates();
      populateQuickTemplates();
    } catch (err) {
      const message = err?.message || 'Load tasks failed';
      if (earnTableBody) {
        earnTableBody.innerHTML = `<tr><td colspan="9" class="muted">${message}</td></tr>`;
      }
      toast(message, 'error');
    }
  }

  async function loadTemplates(options = {}) {
    const mode = options.mode || earnTasksState.mode || 'active';
    await reloadEarnPointsTable(mode);
  }

  function renderTemplates() {
    if (!earnTableBody) return;
    const query = ($('templateSearch')?.value || '').trim().toLowerCase();
    earnTableBody.innerHTML = '';
    const rows = (Array.isArray(earnTemplates) ? earnTemplates : []).filter((task) => {
      if (!query) return true;
      const title = (task.title || '').toLowerCase();
      const description = (task.description || '').toLowerCase();
      return title.includes(query) || description.includes(query);
    });

    if (!rows.length) {
      const emptyRow = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 9;
      cell.className = 'muted';
      cell.textContent = earnTasksState.mode === 'inactive' ? 'No deactivated tasks.' : 'No tasks found.';
      emptyRow.appendChild(cell);
      earnTableBody.appendChild(emptyRow);
      return;
    }

    for (const tpl of rows) {
      const tr = document.createElement('tr');
      if ((tpl.status || '').toLowerCase() !== 'active') {
        tr.classList.add('inactive');
      }
      const youtubeLink = tpl.youtube_url || tpl.master_youtube || '';
      const updatedValue = Number(tpl.updated_at ?? 0);
      const updatedMs = updatedValue > 0 && updatedValue < 10_000_000_000 ? updatedValue * 1000 : updatedValue;
      const updatedDisplay = updatedMs ? formatTime(updatedMs) : '—';
      const statusLabel = (tpl.status || 'active').toString();
      const friendlyStatus = statusLabel.charAt(0).toUpperCase() + statusLabel.slice(1);

      tr.innerHTML = `
        <td>${tpl.id}</td>
        <td>${tpl.title || ''}</td>
        <td>${tpl.points}</td>
        <td>${tpl.description || ''}</td>
        <td>${youtubeLink ? `<a class="video-link" href="${youtubeLink}" target="_blank" rel="noopener" title="Open video"><span aria-hidden="true">🎬</span><span class="sr-only">Video</span></a>` : ''}</td>
        <td>${friendlyStatus}</td>
        <td>${Number.isFinite(Number(tpl.sort_order)) ? Number(tpl.sort_order) : 0}</td>
        <td>${updatedDisplay}</td>
        <td class="actions"></td>
      `;

      const videoLinkEl = tr.querySelector('a[href]');
      if (videoLinkEl) {
        videoLinkEl.dataset.youtube = youtubeLink;
        videoLinkEl.addEventListener('click', (event) => {
          event.preventDefault();
          const url = videoLinkEl.dataset.youtube;
          if (url) openVideoModal(url);
        });
      }

      const actions = tr.querySelector('.actions');
      if (!actions) {
        earnTableBody.appendChild(tr);
        continue;
      }

      const isMasterLinkedTask = tpl && (tpl.source === 'master' || (tpl.master_task_id && String(tpl.master_task_id).trim()));

      if (earnTasksState.mode === 'active') {
        const editBtn = document.createElement('button');
        editBtn.textContent = isMasterLinkedTask ? 'Adjust points' : 'Edit';
        if (isMasterLinkedTask) {
          editBtn.title = 'Title and description managed by master template';
        }
        editBtn.addEventListener('click', () => editTemplate(tpl));
        actions.appendChild(editBtn);

        const deactivateBtn = document.createElement('button');
        deactivateBtn.textContent = 'Deactivate';
        deactivateBtn.addEventListener('click', () => updateTaskStatus(tpl.id, 'inactive'));
        actions.appendChild(deactivateBtn);
      } else {
        const reactivateBtn = document.createElement('button');
        reactivateBtn.textContent = 'Reactivate';
        reactivateBtn.addEventListener('click', () => updateTaskStatus(tpl.id, 'active'));
        actions.appendChild(reactivateBtn);

        const deleteBtn = document.createElement('button');
        deleteBtn.textContent = 'Delete';
        deleteBtn.addEventListener('click', () => deleteTask(tpl.id));
        actions.appendChild(deleteBtn);
      }

      if (isMasterLinkedTask) {
        const titleCell = tr.querySelector('td:nth-child(2)');
        if (titleCell) {
          titleCell.appendChild(document.createElement('br'));
          const origin = document.createElement('span');
          origin.className = 'template-origin-badge';
          origin.textContent = 'Master Template';
          titleCell.appendChild(origin);
        }
      }

      earnTableBody.appendChild(tr);
    }
  }
  $('templateSearch')?.addEventListener('input', renderTemplates);
  btnReloadActive?.addEventListener('click', () => reloadEarnPointsTable('active'));
  btnViewDeactivated?.addEventListener('click', () => reloadEarnPointsTable('inactive'));

  document.querySelector('#btn-add-task-template')?.addEventListener('click', async () => {
    const box = document.querySelector('#tpl-list');
    if (!box) return;
    box.innerHTML = '<div class="opacity-70">Loading...</div>';

    try {
      const list = await loadAvailableTaskTemplates();
      box.innerHTML = Array.isArray(list) && list.length
        ? list
            .map((t) => `
        <div class="flex items-center justify-between p-2 border rounded">
          <div>
            <div class="font-semibold">${t.title}</div>
            <div class="text-sm opacity-70">${t.points} pts${t.youtube_url ? ` · <a href="${t.youtube_url}" target="_blank">YouTube</a>` : ''}</div>
          </div>
          <button class="btn btn-sm" data-adopt="${t.id}">Add</button>
        </div>`)
            .join('')
        : '<div class="opacity-70">No more templates to add.</div>';
      show('#modal-task-templates');
    } catch (error) {
      console.warn('loadAvailableTaskTemplates failed', error);
      const message = error?.message || 'Failed to load master templates';
      if (message === 'family_id required') {
        box.innerHTML = '<div class="opacity-70">Select a family scope to adopt templates.</div>';
        toast('Select a family scope to adopt templates.', 'error');
      } else {
        box.innerHTML = '<div class="opacity-70">Failed to load templates.</div>';
        toast(message, 'error');
      }
    }
  });

  document.addEventListener('click', async (e) => {
    const adopt = e.target.closest('[data-adopt]');
    if (!adopt) return;
    const masterId = adopt.getAttribute('data-adopt');
    if (!masterId) return;
    try {
      adopt.disabled = true;
      await adoptTaskTemplate(masterId);
      toast('Template adopted.');
      hide('#modal-task-templates');
    } catch (err) {
      adopt.disabled = false;
      const message = err?.message || 'Adoption failed';
      if (message === 'family_id required') {
        toast('Select a family scope to adopt templates.', 'error');
      } else {
        toast(message, 'error');
      }
    }
  });

  async function editTemplate(tpl) {
    if (!tpl) return;
    const isMasterLinked = tpl && (tpl.source === 'master' || (tpl.master_task_id && String(tpl.master_task_id).trim()));
    if (isMasterLinked) {
      const points = Number(prompt('Points', tpl.points));
      if (!Number.isFinite(points) || points <= 0) return toast('Invalid points', 'error');
      await updateTask(tpl.id, { points });
      return;
    }

    const title = prompt('Title', tpl.title);
    if (!title) return;
    const points = Number(prompt('Points', tpl.points));
    if (!Number.isFinite(points) || points <= 0) return toast('Invalid points', 'error');
    const description = prompt('Description', tpl.description || '') || '';
    const youtubeInput = prompt('YouTube URL', tpl.youtube_url || '') || '';
    const sortPrompt = prompt('Sort order', Number.isFinite(Number(tpl.sort_order)) ? tpl.sort_order : 0);
    const sortValue = Number(sortPrompt);

    const payload = {
      title,
      points,
      description,
      youtube_url: youtubeInput.trim() ? youtubeInput.trim() : null
    };
    if (Number.isFinite(sortValue)) {
      payload.sort_order = sortValue;
    }
    await updateTask(tpl.id, payload);
  }

  async function updateTaskStatus(id, status) {
    const normalized = (status || '').toString().trim().toLowerCase();
    const message = normalized === 'inactive' ? 'Task deactivated' : 'Task reactivated';
    await updateTask(id, { status: normalized }, { successMessage: message });
  }

  async function updateTask(id, body = {}, { successMessage } = {}) {
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery(`/api/tasks/${encodeURIComponent(id)}`, familyId);
      const { res, body: respBody } = await adminFetch(url, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (res.status === 401) { toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = presentError(respBody?.error, 'Update failed');
        throw new Error(msg);
      }
      toast(successMessage || 'Task updated');
      await reloadEarnPointsTable();
    } catch (err) {
      toast(err.message || 'Update failed', 'error');
    }
  }

  async function deleteTask(id) {
    if (!confirm('Delete this task permanently?')) return;
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery(`/api/tasks/${encodeURIComponent(id)}`, familyId);
      const { res, body } = await adminFetch(url, { method: 'DELETE' });
      if (res.status === 401) { toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Delete failed');
        throw new Error(msg);
      }
      toast('Task deleted');
      await reloadEarnPointsTable();
    } catch (err) {
      toast(err.message || 'Delete failed', 'error');
    }
  }

  function populateQuickTemplates() {
    const select = $('quickTemplate');
    if (!select) return;
    select.innerHTML = '<option value="">Select template</option>';
    const source = earnTasksState.mode === 'active' ? earnTemplates : earnTasksState.cachedActive;
    const rows = Array.isArray(source) ? source : [];
    for (const tpl of rows.filter((task) => (task.active !== undefined ? task.active : (task.status || '').toLowerCase() === 'active'))) {
      const opt = document.createElement('option');
      opt.value = tpl.id;
      opt.textContent = `${tpl.title} (+${tpl.points})`;
      select.appendChild(opt);
    }
  }

  $('btnQuickAward')?.addEventListener('click', async () => {
    const templateId = $('quickTemplate').value;
    const userId = $('quickUser').value.trim();
    if (!templateId || !userId) return toast('Select template and user', 'error');
    try {
      const familyId = requireFamilyId();
      const url = appendFamilyQuery('/api/earn/quick', familyId);
      const payload = withFamilyInBody({ templateId, userId }, familyId);
      const { res, body } = await adminFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.status === 401){ toast(ADMIN_INVALID_MSG, 'error'); return; }
      if (!res.ok) {
        const msg = presentError(body?.error, 'Quick award failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      const amount = data.amount ?? '??';
      const user = data.userId || userId;
      toast(`Awarded ${amount} to ${user}`);
      $('quickUser').value = '';
      applyStateHints(data.hints || latestHints);
      const info = normalizeMemberInput();
      if (info?.normalized && info.normalized === String(user).toLowerCase()) {
        await refreshMemberLedger(info.normalized, { showPanels: true, familyId });
      }
    } catch (err) {
      toast(presentError(err.message, 'Quick award failed'), 'error');
    }
  });

  // ===== History modal =====
  const historyModal = $('historyModal');
  const historyTable = $('historyTable')?.querySelector('tbody');
  function openHistory(preset = {}) {
    if (!historyModal) return;
    historyModal.style.display = 'flex';
    if (preset.type) $('historyType').value = preset.type;
    if (preset.userId) $('historyUser').value = preset.userId;
    if (preset.source) $('historySource').value = preset.source;
    loadHistory();
  }
  function closeHistory() {
    if (historyModal) historyModal.style.display = 'none';
  }
  $('btnHistoryClose')?.addEventListener('click', closeHistory);
  $('btnHistoryRefresh')?.addEventListener('click', loadHistory);
  $('btnHistoryCsv')?.addEventListener('click', () => {
    const params = buildHistoryParams();
    const familyId = requireFamilyId();
    const qs = new URLSearchParams({ ...params, family_id: familyId, format: 'csv' }).toString();
    window.open(`/api/history?${qs}`, '_blank');
  });

  function buildHistoryParams() {
    const type = $('historyType').value;
    const source = $('historySource').value;
    const userId = $('historyUser').value.trim();
    const fromDate = $('historyFrom').value;
    const toDate = $('historyTo').value;
    const params = { limit: '50' };
    if (type !== 'all') params.type = type;
    if (source !== 'all') params.source = source;
    if (userId) params.userId = userId;
    if (fromDate) params.from = fromDate;
    if (toDate) params.to = toDate;
    return params;
  }

  async function loadHistory() {
    if (!historyTable) return;
    historyTable.innerHTML = '<tr><td colspan="17" class="muted">Loading...</td></tr>';
    let familyId;
    try {
      familyId = requireFamilyId();
    } catch {
      historyTable.innerHTML = '<tr><td colspan="17" class="muted">Select a family to view history.</td></tr>';
      return;
    }
    try {
      const params = buildHistoryParams();
      const qs = new URLSearchParams({ ...params, family_id: familyId }).toString();
      const { res, body } = await adminFetch(`/api/history?${qs}`);
      if (res.status === 401){
        toast(ADMIN_INVALID_MSG, 'error');
        historyTable.innerHTML = '<tr><td colspan="17" class="muted">Admin key invalid.</td></tr>';
        return;
      }
      if (!res.ok) {
        const msg = (body && body.error) || (typeof body === 'string' ? body : 'history failed');
        throw new Error(msg);
      }
      const data = body && typeof body === 'object' ? body : {};
      historyTable.innerHTML = '';
      for (const row of data.rows || []) {
        const tr = document.createElement('tr');
        const values = [
          formatTime(row.at),
          row.userId || '',
          row.verb || '',
          row.action || '',
          row.delta ?? '',
          row.balance_after ?? '',
          row.note || '',
          row.notes || '',
          row.templates ? JSON.stringify(row.templates) : '',
          row.itemId || '',
          row.holdId || '',
          row.parent_tx_id || '',
          row.finalCost ?? '',
          row.refund_reason || '',
          row.refund_notes || '',
          row.actor || '',
          row.idempotency_key || ''
        ];
        for (const value of values) {
          const td = document.createElement('td');
          td.textContent = value === undefined || value === null ? '' : String(value);
          tr.appendChild(td);
        }
        historyTable.appendChild(tr);
      }
      if (!historyTable.children.length) {
        historyTable.innerHTML = '<tr><td colspan="17" class="muted">No history</td></tr>';
      }
    } catch (err) {
      historyTable.innerHTML = `<tr><td colspan="17" class="muted">${err.message || 'Failed'}</td></tr>`;
    }
  }

  const historyButtons = Array.from(
    new Set([
      ...document.querySelectorAll('.view-history'),
      document.querySelector('#btn-view-history')
    ].filter(Boolean))
  );
  historyButtons.forEach((btn) => {
    btn.addEventListener('click', async () => {
      const preset = {};
      const type = btn.dataset?.historyType;
      if (type && type !== 'all') {
        preset.type = type;
      }
      const scope = btn.dataset?.historyScope;
      if (scope === 'member') {
        try {
          const userId = await resolveMemberForActions();
          preset.userId = userId;
        } catch (error) {
          const message = error?.message || 'Member not found';
          if (message !== 'Select a family first') {
            toast(message, 'error');
          }
          return;
        }
      } else if (btn.dataset?.historyUser) {
        preset.userId = btn.dataset.historyUser;
      }
      openHistory(preset);
    });
  });

  if (btnActivityRefresh) btnActivityRefresh.addEventListener('click', () => loadActivity());
  if (activityVerb) activityVerb.addEventListener('change', () => loadActivity());
  if (activityActor) activityActor.addEventListener('input', triggerActivityLoad);
  [activityFrom, activityTo].forEach(el => {
    if (el) el.addEventListener('change', () => loadActivity());
  });
  if (activityTableBody) {
    renderActivity([], { emptyMessage: 'Enter a user ID to view activity.' });
    if (activityStatus) activityStatus.textContent = 'Enter a user ID to view activity.';
  }

  loadFeatureFlagsFromServer();
}

  function setupCollapsibles() {
    document.querySelectorAll('[data-collapsible].card').forEach((card) => {
      const btn = card.querySelector('.card-toggle');
      if (!btn) return;
      card.classList.toggle('collapsed', btn.getAttribute('aria-expanded') !== 'true');
      if (btn.dataset.collapsibleBound === 'true') return;
      btn.dataset.collapsibleBound = 'true';
      btn.addEventListener('click', () => {
        const open = btn.getAttribute('aria-expanded') === 'true';
        btn.setAttribute('aria-expanded', open ? 'false' : 'true');
        card.classList.toggle('collapsed', open);
      });
    });

    document
      .querySelectorAll('#card-member-management .subcard[data-collapsible]')
      .forEach((sub) => {
        const btn = sub.querySelector('.subcard-toggle');
        if (!btn) return;
        sub.classList.toggle('collapsed', btn.getAttribute('aria-expanded') !== 'true');
        if (btn.dataset.collapsibleBound === 'true') return;
        btn.dataset.collapsibleBound = 'true';
        btn.addEventListener('click', () => {
          const open = btn.getAttribute('aria-expanded') === 'true';
          btn.setAttribute('aria-expanded', open ? 'false' : 'true');
          sub.classList.toggle('collapsed', open);
        });
      });
  }

  function expandForJump(el) {
    const card = el.closest('.card');
    const cardBtn = card?.querySelector('.card-toggle');
    if (card && card.classList.contains('collapsed')) {
      card.classList.remove('collapsed');
      cardBtn?.setAttribute('aria-expanded', 'true');
    }
    if (el.classList.contains('subcard')) {
      const subBtn = el.querySelector('.subcard-toggle');
      el.classList.remove('collapsed');
      subBtn?.setAttribute('aria-expanded', 'true');
    }
  }

  function jumpTo(selector) {
    const el = document.querySelector(selector);
    if (!el) return;
    expandForJump(el);
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function setupShortcuts() {
    document.querySelectorAll('.shortcut[data-jump]').forEach((btn) => {
      if (btn.dataset.jumpBound === 'true') return;
      btn.dataset.jumpBound = 'true';
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        jumpTo(btn.getAttribute('data-jump'));
      });
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    console.log('admin.js loaded ok');
    initI18n();
    loadAdminKey();
    setupCollapsibles();
    setupShortcuts();
    initAdmin();
  });
})();
