const MULTITENANT_ENFORCE =
  (process.env.MULTITENANT_ENFORCE ?? 'true').toString().toLowerCase() !== 'false';

const COOKIE_NAME = 'ck_admin_key';

export { MULTITENANT_ENFORCE, COOKIE_NAME };
export default {
  MULTITENANT_ENFORCE,
  COOKIE_NAME
};
