import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createApp } from "../server/app.mjs";
import { challenge, seal, unseal } from "../server/security.mjs";
const secret = "a-test-session-secret-that-is-not-used-in-production";
test("encrypted sessions reject tampering, the wrong cookie purpose, and expiry", () => {
  const value = { token: "private-token", expires: 1000 },
    cookie = seal(value, secret, "session");
  assert(!cookie.includes("private-token"));
  assert.deepEqual(unseal(cookie, secret, "session", 999), value);
  assert.equal(unseal(cookie, secret, "oauth", 999), null);
  assert.equal(unseal(cookie, secret, "session", 1000), null);
  assert.equal(
    unseal(
      cookie.slice(0, 10) + "XYZ" + cookie.slice(13),
      secret,
      "session",
      999,
    ),
    null,
  );
});
test("mutations require a session, same origin, and CSRF token", async (t) => {
  let submits = 0;
  const app = createApp(
    { origin: "http://127.0.0.1:4080", sessionSecret: secret },
    {
      github: {
        repository: "cgs-earth/teacup-generator",
        openPullRequest: async () => {
          submits++;
          return { number: 1 };
        },
      },
    },
  );
  const server = app.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => server.close());
  const url = `http://127.0.0.1:${server.address().port}/api/pull-requests`;
  const cookie = `reservoir-session=${seal({ token: "private-token", csrf: "csrf-token", expires: Date.now() + 60000 }, secret, "session")}`;
  const post = (headers) =>
    fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: "{}",
    });
  assert.equal((await post({})).status, 401);
  assert.equal(
    (
      await post({
        Cookie: cookie,
        Origin: "https://wrong.example",
        "X-CSRF-Token": "csrf-token",
      })
    ).status,
    403,
  );
  assert.equal(
    (await post({ Cookie: cookie, Origin: "http://127.0.0.1:4080" })).status,
    403,
  );
  assert.equal(
    (
      await post({
        Cookie: cookie,
        Origin: "http://127.0.0.1:4080",
        "X-CSRF-Token": "csrf-token",
      })
    ).status,
    201,
  );
  assert.equal(submits, 1);
});
test("OAuth uses state and PKCE and rejects an invalid callback before exchanging the code", async (t) => {
  let exchanges = 0;
  const app = createApp(
    {
      origin: "http://127.0.0.1:4080",
      sessionSecret: secret,
      clientId: "app-id",
      clientSecret: "app-secret",
    },
    {
      fetcher: async () => {
        exchanges++;
      },
      github: { repository: "cgs-earth/teacup-generator" },
    },
  );
  const server = app.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}`;
  const login = await fetch(`${base}/auth/login`, { redirect: "manual" });
  const location = new URL(login.headers.get("location"));
  assert.equal(location.origin, "https://github.com");
  assert.equal(location.searchParams.get("code_challenge_method"), "S256");
  assert.equal(location.searchParams.get("code_challenge").length, 43);
  const callback = await fetch(`${base}/auth/callback?state=wrong&code=code`, {
    headers: { Cookie: login.headers.get("set-cookie").split(";")[0] },
    redirect: "manual",
  });
  assert.equal(callback.headers.get("location"), "/?auth_error=expired");
  assert.equal(exchanges, 0);
});
test("OAuth exchanges a valid code and exposes only identity and CSRF to the browser", async (t) => {
  let exchanged;
  const identity = { login: "writer", id: 123 };
  const app = createApp(
    {
      origin: "https://editor.example",
      sessionSecret: secret,
      clientId: "app-id",
      clientSecret: "app-secret",
      repositoryId: "1135150705",
    },
    {
      fetcher: async (url, options) => {
        assert.equal(url, "https://github.com/login/oauth/access_token");
        exchanged = JSON.parse(options.body);
        return new Response(
          JSON.stringify({ access_token: "private-token", expires_in: 28800 }),
        );
      },
      github: {
        repository: "cgs-earth/teacup-generator",
        access: async (token) => {
          assert.equal(token, "private-token");
          return identity;
        },
      },
    },
  );
  const server = app.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => server.close());
  const base = `http://127.0.0.1:${server.address().port}`;
  const login = await fetch(`${base}/auth/login`, { redirect: "manual" });
  const location = new URL(login.headers.get("location"));
  const response = await fetch(
    `${base}/auth/callback?state=${location.searchParams.get("state")}&code=one-time-code`,
    {
      headers: { Cookie: login.headers.get("set-cookie").split(";")[0] },
      redirect: "manual",
    },
  );
  assert.equal(response.headers.get("location"), "/");
  assert.equal(exchanged.code, "one-time-code");
  assert.equal(exchanged.repository_id, "1135150705");
  assert.equal(exchanged.redirect_uri, "https://editor.example/auth/callback");
  assert.equal(
    challenge(exchanged.code_verifier),
    location.searchParams.get("code_challenge"),
  );
  const cookie = response.headers
    .getSetCookie()
    .find((value) => value.startsWith("__Host-reservoir-session="));
  assert.match(cookie, /Secure/);
  assert.match(cookie, /HttpOnly/);
  assert.match(cookie, /SameSite=Lax/);
  assert(!cookie.includes("private-token"));
  const session = await fetch(`${base}/api/session`, {
    headers: { Cookie: cookie.split(";")[0] },
  });
  const text = await session.text(),
    info = JSON.parse(text);
  assert.deepEqual(info.user, identity);
  assert(info.csrf);
  assert(!text.includes("private-token"));
  assert(!text.includes("app-secret"));
});
