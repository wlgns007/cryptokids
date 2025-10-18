const KEY = 'ck_admin_scope';
const UUID_RX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function getStorage() {
  try {
    if (typeof window === 'undefined') return null;
    return window.localStorage || null;
  } catch {
    return null;
  }
}

export function isUUID(value) {
  return typeof value === 'string' && UUID_RX.test(value);
}

export function setFamilyScope(obj = {}) {
  const storage = getStorage();
  const candidate = obj?.uuid ?? obj?.id ?? null;
  const scope = {
    uuid: isUUID(candidate) ? candidate : null,
    key: obj?.key ?? null,
    name: obj?.name ?? null,
    status: obj?.status ?? null,
  };

  if (storage) {
    try {
      storage.setItem(KEY, JSON.stringify(scope));
    } catch {
      // ignore storage failures
    }
  }

  return scope;
}

export function getFamilyScope() {
  const storage = getStorage();
  if (!storage) return null;
  try {
    const raw = storage.getItem(KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    if (!isUUID(parsed.uuid)) {
      clearFamilyScope();
      return null;
    }
    return {
      uuid: parsed.uuid,
      key: parsed.key ?? null,
      name: parsed.name ?? null,
      status: parsed.status ?? null,
    };
  } catch {
    return null;
  }
}

export function clearFamilyScope() {
  const storage = getStorage();
  if (!storage) return;
  try {
    storage.removeItem(KEY);
  } catch {
    // ignore storage failures
  }
}
