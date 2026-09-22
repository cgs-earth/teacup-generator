import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";

export function randomToken() {
  return randomBytes(32).toString("base64url");
}
export function challenge(verifier) {
  return createHash("sha256").update(verifier).digest("base64url");
}
export function equal(a, b) {
  return (
    typeof a === "string" &&
    typeof b === "string" &&
    a.length > 0 &&
    Buffer.byteLength(a) === Buffer.byteLength(b) &&
    timingSafeEqual(Buffer.from(a), Buffer.from(b))
  );
}
export function seal(value, secret, purpose) {
  const iv = randomBytes(12);
  const cipher = createCipheriv(
    "aes-256-gcm",
    createHash("sha256").update(secret).digest(),
    iv,
  );
  cipher.setAAD(Buffer.from(purpose));
  const encrypted = Buffer.concat([
    cipher.update(JSON.stringify(value)),
    cipher.final(),
  ]);
  return Buffer.concat([iv, cipher.getAuthTag(), encrypted]).toString(
    "base64url",
  );
}
export function unseal(value, secret, purpose, now = Date.now()) {
  try {
    if (typeof value !== "string" || value.length > 6000) return null;
    const data = Buffer.from(value, "base64url");
    const decipher = createDecipheriv(
      "aes-256-gcm",
      createHash("sha256").update(secret).digest(),
      data.subarray(0, 12),
    );
    decipher.setAAD(Buffer.from(purpose));
    decipher.setAuthTag(data.subarray(12, 28));
    const result = JSON.parse(
      Buffer.concat([
        decipher.update(data.subarray(28)),
        decipher.final(),
      ]).toString(),
    );
    return Number.isFinite(result.expires) && result.expires > now
      ? result
      : null;
  } catch {
    return null;
  }
}
export function readCookie(req, name) {
  const entry = (req.headers.cookie || "")
    .split(";")
    .map((v) => v.trim())
    .find((v) => v.startsWith(`${name}=`));
  return entry ? entry.slice(name.length + 1) : undefined;
}
