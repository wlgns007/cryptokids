const KEY = 'ck_admin_scope';

function persistScope(scope) {
  try {
    if (typeof window === 'undefined' || !window.localStorage) {
      return scope;
    }
    if (!scope) {
      window.localStorage.removeItem(KEY);
      return null;
    }
    window.localStorage.setItem(KEY, JSON.stringify(scope));
    return scope;
  } catch (error) {
    console.warn('[scopeStore] persist failed', error);
    return scope;
  }
}

export function setFamilyScope({ uuid, id, key = null, name = null, status = null } = {}) {
  const normalized =
    typeof uuid === 'string' && uuid.trim()
      ? uuid.trim()
      : id != null && String(id).trim()
        ? String(id).trim()
        : '';

  if (!normalized) {
    persistScope(null);
    return null;
  }

  const scope = {
    uuid: normalized,
    key: key ?? null,
    name: name ?? null,
    status: status ?? null
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
    const raw = window.localStorage.getItem(KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    return {
      uuid: parsed.uuid ?? null,
      key: parsed.key ?? null,
      name: parsed.name ?? null,
      status: parsed.status ?? null
    };
  } catch (error) {
    console.warn('[scopeStore] read failed', error);
    return null;
  }
}
