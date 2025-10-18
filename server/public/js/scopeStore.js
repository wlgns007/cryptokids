const STORAGE_KEY = 'ck_admin_scope';

function persistScope(scope) {
  try {
    if (typeof window === 'undefined' || !window.localStorage) {
      return scope;
    }
    if (!scope) {
      window.localStorage.removeItem(STORAGE_KEY);
      return null;
    }
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(scope));
    return scope;
  } catch (error) {
    console.warn('[scopeStore] persist failed', error);
    return scope;
  }
}

export function setFamilyScope({ id, key = null, name = null, status = null } = {}) {
  const uuid = typeof id === 'string' ? id : id != null ? String(id) : '';
  if (!uuid) {
    persistScope(null);
    return null;
  }
  const scope = {
    uuid,
    key: key ?? null,
    name: name ?? null,
    status: status ?? null,
  };
  persistScope(scope);
  return scope;
}

export function clearFamilyScope() {
  persistScope(null);
}

export function getFamilyScope() {
  try {
    if (typeof window === 'undefined' || !window.localStorage) {
      return null;
    }
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    return {
      uuid: parsed.uuid ?? null,
      key: parsed.key ?? null,
      name: parsed.name ?? null,
      status: parsed.status ?? null,
    };
  } catch (error) {
    console.warn('[scopeStore] read failed', error);
    return null;
  }
}
