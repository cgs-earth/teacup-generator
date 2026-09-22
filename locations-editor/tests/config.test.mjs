import test from "node:test";
import assert from "node:assert/strict";
import { readConfig } from "../server/config.mjs";

const production = {
  NODE_ENV: "production",
  RENDER_EXTERNAL_URL: "https://editor.onrender.com",
  GITHUB_CLIENT_ID: "test-client",
  GITHUB_CLIENT_SECRET: "test-secret",
  SESSION_SECRET: "a-test-session-secret-at-least-32-characters",
};

test("Render supplies the public OAuth origin while an explicit custom domain takes precedence", () => {
  assert.equal(readConfig(production).origin, production.RENDER_EXTERNAL_URL);
  assert.equal(
    readConfig({ ...production, APP_ORIGIN: "https://editor.example" }).origin,
    "https://editor.example",
  );
});

test("production still rejects missing credentials, short session secrets, and insecure origins", () => {
  for (const key of [
    "GITHUB_CLIENT_ID",
    "GITHUB_CLIENT_SECRET",
    "SESSION_SECRET",
    "RENDER_EXTERNAL_URL",
  ]) {
    assert.throws(() => readConfig({ ...production, [key]: "" }));
  }
  assert.throws(() => readConfig({ ...production, SESSION_SECRET: "short" }));
  for (const origin of [
    "http://editor.example",
    "https://editor.example/path",
    "https://editor.example/",
  ]) {
    assert.throws(() =>
      readConfig({ ...production, RENDER_EXTERNAL_URL: origin }),
    );
  }
});
