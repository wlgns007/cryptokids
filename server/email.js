const { SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM } = process.env;

let transporterPromise = null;

async function getTransporter() {
  if (!SMTP_HOST || !SMTP_FROM) return null;
  if (!transporterPromise) {
    transporterPromise = (async () => {
      try {
        const nodemailerModule = await import("nodemailer");
        const nodemailer = nodemailerModule?.default ?? nodemailerModule;
        return nodemailer.createTransport({
          host: SMTP_HOST,
          port: Number(SMTP_PORT || 587),
          secure: Number(SMTP_PORT || 587) === 465,
          auth: SMTP_USER && SMTP_PASS ? { user: SMTP_USER, pass: SMTP_PASS } : undefined
        });
      } catch (error) {
        console.warn("[email] failed to load nodemailer, using noop transport", error?.message || error);
        return null;
      }
    })();
  }
  return transporterPromise;
}

export async function sendMail(to, subject, html) {
  const transporter = await getTransporter();
  if (!transporter) {
    console.warn("[email] transporter not configured; pretending to send", { to, subject });
    return { ok: true, simulated: true };
  }
  const info = await transporter.sendMail({ from: SMTP_FROM, to, subject, html });
  return { ok: true, messageId: info?.messageId };
}
