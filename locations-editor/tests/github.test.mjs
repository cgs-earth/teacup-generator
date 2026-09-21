import test from "node:test";
import assert from "node:assert/strict";
import { GitHub, CSV_PATH } from "../server/github.mjs";

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
