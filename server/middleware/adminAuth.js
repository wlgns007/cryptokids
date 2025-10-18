import { resolveAdmin } from "./resolveAdmin.js";

export default function adminAuth(req, res, next) {
  return resolveAdmin(req, res, next);
}
