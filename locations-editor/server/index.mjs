import { createApp } from "./app.mjs";
import { randomToken } from "./security.mjs";

const production = process.env.NODE_ENV === "production";
const origin = process.env.APP_ORIGIN || "http://127.0.0.1:4080";
if (
  production &&
  (!process.env.SESSION_SECRET ||
    process.env.SESSION_SECRET.length < 32 ||
    !process.env.APP_ORIGIN ||
    !process.env.GITHUB_CLIENT_ID ||
    !process.env.GITHUB_CLIENT_SECRET)
) {
  throw new Error(
    "Set APP_ORIGIN, SESSION_SECRET (at least 32 characters), GITHUB_CLIENT_ID, and GITHUB_CLIENT_SECRET before starting production.",
  );
}
if (
  new URL(origin).origin !== origin ||
  (production && !origin.startsWith("https://"))
)
  throw new Error(
    "APP_ORIGIN must be an origin without a path, using HTTPS in production.",
  );
const app = createApp({
  origin,
  repository: process.env.GITHUB_REPOSITORY || "cgs-earth/teacup-generator",
  repositoryId: process.env.GITHUB_REPOSITORY_ID || "1135150705",
  clientId: process.env.GITHUB_CLIENT_ID,
  clientSecret: process.env.GITHUB_CLIENT_SECRET,
  sessionSecret: process.env.SESSION_SECRET || randomToken(),
});
app.listen(
  Number(process.env.PORT || 4080),
  process.env.HOST || "0.0.0.0",
  () =>
    console.log(
      `Reservoir editor listening on port ${process.env.PORT || 4080}`,
    ),
);
