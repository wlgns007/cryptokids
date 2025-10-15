const MULTITENANT_ENFORCE =
  (process.env.MULTITENANT_ENFORCE ?? "true").toString().toLowerCase() !== "false";

export { MULTITENANT_ENFORCE };
export default {
  MULTITENANT_ENFORCE
};
