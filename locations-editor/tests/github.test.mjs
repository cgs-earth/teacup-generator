import test from "node:test";
import assert from "node:assert/strict";
import { GitHub, CSV_PATH } from "../server/github.mjs";

test("public previews avoid the REST API and preserve the exact Git blob version", async () => {
  const csv = "\ufeffName,reservoir_notes\r\nAgate,café\r\n";
  const github = new GitHub({
    fetcher: async (url, options) => {
      assert.equal(
        url,
        `https://raw.githubusercontent.com/cgs-earth/teacup-generator/refs/heads/main/${CSV_PATH}`,
      );
      assert.equal(options.headers?.Authorization, undefined);
      return new Response(Buffer.from(csv));
    },
  });
  const snapshot = await github.snapshot();
  assert.equal(snapshot.text, csv);
  // Expected value from git hash-object, including the BOM, Unicode and CRLF.
  assert.equal(snapshot.sha, "8d9773c0207e63eae8fcb28f913932b4da8e7d13");
  assert.equal(snapshot.commitSha, null);
});

test("public file failures do not return error pages as CSV", async () => {
  const github = new GitHub({
    fetcher: async () => new Response("Unavailable", { status: 503 }),
  });
  await assert.rejects(github.snapshot(), (error) => error.status === 502);
});

function fixture({ writable = true, failPrOnce = false } = {}) {
  const original = "Name,reservoir_notes\r\nAgate,\r\n";
  const sha = "a".repeat(40),
    mainCommit = "b".repeat(40),
    branches = new Map(),
    calls = [];
  let pr,
    prFailed = false;
  const json = (data, status = 200) =>
    new Response(JSON.stringify(data), { status });
  const fetcher = async (url, options) => {
    const method = options.method || "GET",
      path = new URL(url).pathname.replace(
        "/repos/cgs-earth/teacup-generator",
        "",
      );
    const params = new URL(url).searchParams,
      body = options.body ? JSON.parse(options.body) : null;
    calls.push({ method, path, body });
    if (path === "/user") return json({ login: "editor", id: 1 });
    if (path === "") return json({ permissions: { push: writable } });
    if (path === "/pulls" && method === "GET") return json(pr ? [pr] : []);
    if (path === "/git/ref/heads/main")
      return json({ object: { sha: mainCommit } });
    if (path.startsWith("/git/ref/heads/"))
      return branches.has(path.slice("/git/ref/heads/".length))
        ? json({ object: { sha: mainCommit } })
        : json({}, 404);
    if (path === "/git/refs") {
      const branch = body.ref.replace("refs/heads/", "");
      if (branches.has(branch)) return json({}, 422);
      branches.set(branch, { text: original, sha });
      return json({}, 201);
    }
    if (path === `/contents/${CSV_PATH}` && method === "GET") {
      const file = branches.get(params.get("ref")) || { text: original, sha };
      return json({
        type: "file",
        encoding: "base64",
        content: Buffer.from(file.text).toString("base64"),
        sha: file.sha,
      });
    }
    if (path === `/contents/${CSV_PATH}` && method === "PUT") {
      assert.notEqual(body.branch, "main");
      assert.notEqual(body.branch, "allison-edits");
      const file = branches.get(body.branch);
      if (!file || file.sha !== body.sha) return json({}, 409);
      branches.set(body.branch, {
        text: Buffer.from(body.content, "base64").toString(),
        sha: "c".repeat(40),
      });
      return json({ commit: { sha: "d".repeat(40) } }, 201);
    }
    if (path === "/pulls" && method === "POST") {
      if (failPrOnce && !prFailed) {
        prFailed = true;
        return json({}, 503);
      }
      assert.equal(body.base, "main");
      pr = {
        number: 42,
        html_url: "https://github.com/cgs-earth/teacup-generator/pull/42",
      };
      return json(pr, 201);
    }
    throw new Error(`Unexpected GitHub request: ${method} ${path}`);
  };
  return { github: new GitHub({ fetcher }), calls, branches };
}
const input = {
  sha: "a".repeat(40),
  requestId: "d3ae128d-8c72-42f7-8fbb-f02f5c4be270",
  title: "Update Agate notes",
  body: "Clarify reservoir notes.",
  changes: [
    { row: 0, column: "reservoir_notes", before: "", after: "Reviewed" },
  ],
};
test("a writer opens a PR and only the new branch receives the CSV commit", async () => {
  const { github, calls, branches } = fixture();
  const result = await github.openPullRequest("user-token", input);
  assert.equal(result.number, 42);
  assert.equal(
    branches.get(result.branch).text,
    "Name,reservoir_notes\r\nAgate,Reviewed\r\n",
  );
  await github.openPullRequest("user-token", input);
  assert.equal(calls.filter((c) => c.method === "PUT").length, 1);
  assert.equal(
    calls.filter((c) => c.path === "/pulls" && c.method === "POST").length,
    1,
  );
});
test("read-only users and stale source versions cannot create a branch", async () => {
  for (const [f, value, status] of [
    [fixture({ writable: false }), input, 403],
    [fixture(), { ...input, sha: "e".repeat(40) }, 409],
  ]) {
    await assert.rejects(
      f.github.openPullRequest("token", value),
      (e) => e.status === status,
    );
    assert.equal(f.calls.filter((c) => c.method !== "GET").length, 0);
  }
});
test("a retry after PR failure reuses the branch without repeating the commit", async () => {
  const { github, calls } = fixture({ failPrOnce: true });
  await assert.rejects(github.openPullRequest("token", input), (e) =>
    e.details.recoveryUrl.includes("/tree/locations/editor-editor-"),
  );
  assert.equal((await github.openPullRequest("token", input)).number, 42);
  assert.equal(calls.filter((c) => c.method === "PUT").length, 1);
});
