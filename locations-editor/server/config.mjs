import { randomToken } from "./security.mjs";

export function readConfig(env = process.env) {
  const production = env.NODE_ENV === "production";
  const configuredOrigin = env.APP_ORIGIN || env.RENDER_EXTERNAL_URL;
  const origin = configuredOrigin || "http://127.0.0.1:4080";
  if (
    production &&
    (!env.SESSION_SECRET ||
      env.SESSION_SECRET.length < 32 ||
      !configuredOrigin ||
      !env.GITHUB_CLIENT_ID ||
      !env.GITHUB_CLIENT_SECRET)
  ) {
    throw new Error(
      "Set APP_ORIGIN (or Render's RENDER_EXTERNAL_URL), SESSION_SECRET (at least 32 characters), GITHUB_CLIENT_ID, and GITHUB_CLIENT_SECRET before starting production.",
    );
  }
  if (
    new URL(origin).origin !== origin ||
    (production && !origin.startsWith("https://"))
  ) {
    throw new Error(
      "The public origin must have no path and must use HTTPS in production.",
    );
  }
  return {
    origin,
    repository: env.GITHUB_REPOSITORY || "cgs-earth/teacup-generator",
    repositoryId: env.GITHUB_REPOSITORY_ID || "1135150705",
    clientId: env.GITHUB_CLIENT_ID,
    clientSecret: env.GITHUB_CLIENT_SECRET,
    sessionSecret: env.SESSION_SECRET || randomToken(),
  };
}
