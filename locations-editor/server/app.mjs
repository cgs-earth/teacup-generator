import express from "express";
import helmet from "helmet";
import { fileURLToPath } from "node:url";
import { GitHub, AppError } from "./github.mjs";
import {
  challenge,
  equal,
  randomToken,
  readCookie,
  seal,
  unseal,
} from "./security.mjs";

export function createApp(
  config,
  {
    fetcher = fetch,
    github = new GitHub({ repository: config.repository, fetcher }),
  } = {},
) {
  const app = express();
  const secure = new URL(config.origin).protocol === "https:";
  const prefix = secure ? "__Host-" : "";
  const sessionName = `${prefix}reservoir-session`,
    flowName = `${prefix}reservoir-oauth`;
  const cookieOptions = { httpOnly: true, secure, sameSite: "lax", path: "/" };
  const configured = Boolean(config.clientId && config.clientSecret);
  const callback = `${config.origin}/auth/callback`;
  app.disable("x-powered-by");
  app.use(
    helmet({
      contentSecurityPolicy: {
        directives: {
          "script-src": ["'self'"],
          "style-src": ["'self'"],
          "img-src": ["'self'", "data:"],
          "connect-src": ["'self'"],
          "frame-ancestors": ["'none'"],
          "form-action": ["'self'", "https://github.com"],
          "upgrade-insecure-requests": secure ? [] : null,
        },
      },
    }),
  );
  app.use(express.json({ limit: "1mb" }));
  app.use(["/api", "/auth"], (_req, res, next) => {
    res.set("Cache-Control", "no-store");
    next();
  });
  const session = (req) =>
    unseal(readCookie(req, sessionName), config.sessionSecret, "session");
  const requireSession = (req) => {
    const s = session(req);
    if (!s)
      throw new AppError(
        401,
        "Sign in with GitHub to open a pull request. Your draft stays in this tab.",
      );
    return s;
  };
  const mutationSession = (req) => {
    const s = requireSession(req);
    if (
      req.headers.origin !== config.origin ||
      !equal(req.headers["x-csrf-token"], s.csrf)
    )
      throw new AppError(
        403,
        "This request could not be verified. Reload the editor and try again.",
      );
    return s;
  };
  app.get("/healthz", (_req, res) => res.json({ ok: true }));
  app.get("/api/session", async (req, res) => {
    const s = session(req);
    const info = {
      configured,
      repository: github.repository,
      baseBranch: "main",
    };
    if (!s) return res.json({ ...info, user: null });
    const user = await github.access(s.token);
    res.json({ ...info, user, csrf: s.csrf });
  });
  let publicCache;
  app.get("/api/locations", async (req, res) => {
    const s = session(req);
    if (!s && publicCache && publicCache.until > Date.now())
      return res.json(publicCache.value);
    const value = await github.snapshot(s?.token);
    if (!s) publicCache = { value, until: Date.now() + 60_000 };
    res.json(value);
  });
  app.get("/auth/login", (_req, res) => {
    if (!configured)
      throw new AppError(
        503,
        "GitHub sign-in has not been configured. Contact the repository administrator.",
      );
    const flow = {
      state: randomToken(),
      verifier: randomToken(),
      expires: Date.now() + 10 * 60_000,
    };
    res.cookie(flowName, seal(flow, config.sessionSecret, "oauth"), {
      ...cookieOptions,
      maxAge: 10 * 60_000,
    });
    const url = new URL("https://github.com/login/oauth/authorize");
    url.search = new URLSearchParams({
      client_id: config.clientId,
      redirect_uri: callback,
      state: flow.state,
      code_challenge: challenge(flow.verifier),
      code_challenge_method: "S256",
      allow_signup: "false",
    }).toString();
    res.redirect(url.href);
  });
  app.get("/auth/callback", async (req, res) => {
    const flow = unseal(
      readCookie(req, flowName),
      config.sessionSecret,
      "oauth",
    );
    res.clearCookie(flowName, cookieOptions);
    if (
      !configured ||
      !flow ||
      !equal(req.query.state, flow.state) ||
      typeof req.query.code !== "string"
    )
      return res.redirect("/?auth_error=expired");
    try {
      const response = await fetcher(
        "https://github.com/login/oauth/access_token",
        {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            client_id: config.clientId,
            client_secret: config.clientSecret,
            code: req.query.code,
            redirect_uri: callback,
            code_verifier: flow.verifier,
            repository_id: config.repositoryId,
          }),
          signal: AbortSignal.timeout(15_000),
        },
      );
      const data = await response.json();
      if (!response.ok || !data.access_token || data.error)
        return res.redirect("/?auth_error=github");
      await github.access(data.access_token);
      const age = Math.min(Number(data.expires_in) || 28_800, 28_800) * 1000;
      const s = {
        token: data.access_token,
        csrf: randomToken(),
        expires: Date.now() + age,
      };
      res.cookie(sessionName, seal(s, config.sessionSecret, "session"), {
        ...cookieOptions,
        maxAge: age,
      });
      res.redirect("/");
    } catch (error) {
      res.redirect(
        `/?auth_error=${error.status === 403 ? "access" : "github"}`,
      );
    }
  });
  app.post("/auth/logout", (req, res) => {
    mutationSession(req);
    res.clearCookie(sessionName, cookieOptions);
    res.status(204).end();
  });
  app.post("/api/pull-requests", async (req, res) => {
    const s = mutationSession(req);
    const result = await github.openPullRequest(s.token, req.body);
    publicCache = undefined;
    res.status(201).json(result);
  });
  app.use(express.static(fileURLToPath(new URL("../dist", import.meta.url))));
  app.get("/", (_req, res) =>
    res.sendFile(fileURLToPath(new URL("../dist/index.html", import.meta.url))),
  );
  app.use((err, _req, res, _next) => {
    const status =
      err instanceof AppError
        ? err.status
        : err.type === "entity.too.large"
          ? 413
          : 500;
    res
      .status(status)
      .json({
        message:
          err instanceof AppError
            ? err.message
            : status === 413
              ? "The request is too large."
              : "The request could not be completed. Try again.",
        ...(err instanceof AppError ? err.details : {}),
      });
  });
  return app;
}
