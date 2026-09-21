import { applyChanges, parseCsv } from "../shared/csv.mjs";

export const CSV_PATH = "R-workflow/config/locations.csv";
export const BASE_BRANCH = "main";

export class AppError extends Error {
  constructor(status, message, details = {}) {
    super(message);
    this.status = status;
    this.details = details;
  }
}

export class GitHub {
  constructor({ repository = "cgs-earth/teacup-generator", fetcher = fetch }) {
    if (!/^[\w.-]+\/[\w.-]+$/.test(repository))
      throw new Error("Invalid repository configuration.");
    this.repository = repository;
    this.repoPath = `/repos/${repository}`;
    this.fetcher = fetcher;
  }

  async request(path, token, method = "GET", body) {
    const response = await this.fetcher(`https://api.github.com${path}`, {
      method,
      headers: {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "teacup-locations-editor",
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(15_000),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      let message = "GitHub could not complete this request. Try again.";
      if (response.status === 401)
        message =
          "Your GitHub session expired. Sign in again; your draft is still in this tab.";
      if (response.status === 403)
        message =
          "GitHub denied this request. Check your repository access, app approval, and API rate limit.";
      if (response.status === 404)
        message =
          "GitHub could not find the repository or file. Check that the app is installed on this repository.";
      if (response.status === 409 || response.status === 422)
        message =
          "GitHub could not apply the update. The branch may have changed or a repository rule may require attention.";
      throw new AppError(response.status, message);
    }
    return data;
  }

  async access(token) {
    const [user, repo] = await Promise.all([
      this.request("/user", token),
      this.request(this.repoPath, token),
    ]);
    if (!repo.permissions?.push)
      throw new AppError(
        403,
        `You need write access to ${this.repository} to propose edits through this app.`,
      );
    return { login: user.login, name: user.name || user.login, id: user.id };
  }

  async file(token, ref) {
    const file = await this.request(
      `${this.repoPath}/contents/${CSV_PATH}?ref=${encodeURIComponent(ref)}`,
      token,
    );
    if (
      file.type !== "file" ||
      file.encoding !== "base64" ||
      typeof file.content !== "string"
    )
      throw new AppError(502, "GitHub returned an unsupported file.");
    return {
      sha: file.sha,
      text: Buffer.from(file.content, "base64").toString("utf8"),
    };
  }

  async snapshot(token) {
    const ref = await this.request(
      `${this.repoPath}/git/ref/heads/${BASE_BRANCH}`,
      token,
    );
    const file = await this.file(token, ref.object.sha);
    return {
      ...file,
      commitSha: ref.object.sha,
      repository: this.repository,
      baseBranch: BASE_BRANCH,
      path: CSV_PATH,
    };
  }

  async openPullRequest(token, input) {
    const user = await this.access(token);
    if (
      !input ||
      !/^[0-9a-f]{40}$/.test(input.sha || "") ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
        input.requestId || "",
      ) ||
      typeof input.title !== "string" ||
      input.title.trim().length < 5 ||
      input.title.length > 200 ||
      typeof input.body !== "string" ||
      input.body.length > 10_000
    ) {
      throw new AppError(
        400,
        "Provide a title, a valid file version, and your reviewed changes.",
      );
    }
    const branch = `locations/editor-${user.login}-${input.requestId}`;
    const recoveryUrl = `https://github.com/${this.repository}/tree/${branch}`;
    const [owner] = this.repository.split("/");
    const findPr = async () => {
      const prs = await this.request(
        `${this.repoPath}/pulls?state=open&base=${BASE_BRANCH}&head=${encodeURIComponent(`${owner}:${branch}`)}`,
        token,
      );
      return prs[0];
    };
    // Repeated submissions return the existing PR instead of creating duplicates.
    const existingPr = await findPr();
    if (existingPr)
      return { url: existingPr.html_url, number: existingPr.number, branch };

    const current = await this.snapshot(token);
    if (current.sha !== input.sha)
      throw new AppError(
        409,
        "locations.csv changed on main while you were editing. Download your draft, then reload the latest file and reapply your changes.",
      );
    let text;
    try {
      text = applyChanges(parseCsv(current.text), input.changes);
    } catch (error) {
      throw new AppError(400, error.message);
    }
    let branchCreated = false;
    try {
      try {
        await this.request(`${this.repoPath}/git/refs`, token, "POST", {
          ref: `refs/heads/${branch}`,
          sha: current.commitSha,
        });
        branchCreated = true;
      } catch (error) {
        if (error.status !== 422) throw error;
        // A retry can reuse only this user's UUID-named branch, with matching content.
        await this.request(`${this.repoPath}/git/ref/heads/${branch}`, token);
        branchCreated = true;
      }
      const branchFile = await this.file(token, branch);
      if (branchFile.text !== text) {
        if (branchFile.sha !== current.sha)
          throw new AppError(
            409,
            "The draft branch changed after your previous attempt. Review it on GitHub before continuing.",
          );
        await this.request(
          `${this.repoPath}/contents/${CSV_PATH}`,
          token,
          "PUT",
          {
            message: input.title.trim(),
            branch,
            sha: branchFile.sha,
            content: Buffer.from(text).toString("base64"),
          },
        );
      }
      let pr;
      try {
        pr = await this.request(`${this.repoPath}/pulls`, token, "POST", {
          title: input.title.trim(),
          head: branch,
          base: BASE_BRANCH,
          body: `${input.body.trim()}${input.body.trim() ? "\n\n" : ""}Proposed through the reservoir editor by @${user.login}.\n\n${input.changes.length} field change(s) in \`${CSV_PATH}\`. The existing main workflow regenerates GeoJSON after this PR is merged.`,
        });
      } catch (error) {
        if (error.status !== 422 || !(pr = await findPr())) throw error;
      }
      return { url: pr.html_url, number: pr.number, branch };
    } catch (error) {
      if (branchCreated)
        throw new AppError(error.status || 502, error.message, { recoveryUrl });
      throw error;
    }
  }
}
