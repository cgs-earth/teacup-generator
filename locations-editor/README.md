# Reservoir editor

Hosted editor: [teacup-reservoir-editor.onrender.com](https://teacup-reservoir-editor.onrender.com).

Browse and edit `R-workflow/config/locations.csv`, review the old and proposed values, and open a pull request against `main`. The editor supports existing reservoir rows and every CSV column. It preserves untouched fields, quoting, and line endings.

Users sign in with GitHub. The server checks repository write access before allowing edits and checks it again before submitting. Each submission creates a `locations/editor-<login>-<uuid>` branch with a CSV commit, then opens a PR. The editor never commits to `main` or `allison-edits` and never merges PRs. Repository rules still apply.

After a PR is merged, the existing `main` workflow regenerates `locations.geojson` and merges main into `allison-edits`. The editor does not change those workflows.

## Local development

Use Node.js 24 and pnpm 11.25.0. From this directory:

```sh
pnpm install --frozen-lockfile
cp .env.example .env
pnpm build
pnpm server
```

Open `http://127.0.0.1:4080`. Without GitHub credentials, the editor shows the public reservoir list with editing disabled. For Vite's development server, set `APP_ORIGIN=http://127.0.0.1:5173` in `.env`, run `pnpm server`, and run `pnpm dev` in another terminal. Register the matching callback URL below. Use the same hostname in the browser and `APP_ORIGIN`.

```sh
pnpm test
pnpm build
```

Tests cover CSV preservation and validation, writer permissions, stale files, PR retries, session encryption, OAuth, and CSRF. GitHub calls are mocked in tests. Live sign-in requires a registered GitHub App.

## GitHub App setup

Register a GitHub App under the repository's owner, then install it on **only `cgs-earth/teacup-generator`**. The user must both authorize the app and have repository write access.

- Set the homepage to the editor's public HTTPS origin.
- Set the callback URL to `<APP_ORIGIN>/auth/callback`.
- Enable expiring user access tokens. The editor requires users to sign in again after at most eight hours and does not retain refresh tokens.
- Disable webhooks. They are not used.
- Grant repository **Contents: read and write** and **Pull requests: read and write**. GitHub includes **Metadata: read**. Do not add organization or account permissions.
- Store the app's client ID and client secret in the hosting service. This app uses user authorization, so it does not need a GitHub App private key or installation token.

Use a separate GitHub App with a loopback callback for local testing if the production app is already in use. Users never paste personal access tokens into the editor.

See GitHub's [user access token documentation](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app) for registration and authorization details.

## Free hosting on Render

The service runs in [the Render dashboard](https://dashboard.render.com/web/srv-dap972tg1s2s739so6mg) from `codex/locations-web-editor`. Its [GitHub App settings](https://github.com/organizations/cgs-earth/settings/apps/cgs-teacup-reservoir-editor) use the hosted editor URL above and `https://teacup-reservoir-editor.onrender.com/auth/callback`. The app is installed only on `cgs-earth/teacup-generator`.

The repository's `render.yaml` defines one Docker web service on Render's **Free** compute plan. It has no database, disk, or paid service. The server serves the React build and GitHub sign-in API from one HTTPS origin. This deployment is separate from the Western Water Datahub's GCP infrastructure.

Create a Render Blueprint from a tested branch containing the editor. The service deploys the same branch selected for the blueprint, so a deployment does not require merging its code PR. The blueprint prompts for the GitHub App's client ID and client secret and generates a persistent random session secret. Render provides `RENDER_EXTERNAL_URL`, which the server uses for the OAuth callback and origin checks. Set the GitHub App's homepage to that URL and its callback to `<RENDER_EXTERNAL_URL>/auth/callback`. For a custom domain, set `APP_ORIGIN` to the custom HTTPS origin and update the GitHub App callback to match.

When Render has a GitHub repository connection, it deploys changes under `locations-editor/` after the linked branch's CI checks pass. Deploying from the public repository URL avoids granting Render repository access, but requires manual deployments for later code updates. Keep the compute plan set to **Free**, leave PR previews disabled, and use a workspace without a payment method so usage limits cannot result in charges. Free instances sleep after 15 minutes without traffic and take about one minute to wake. The free allowance is 750 instance hours per workspace per month. See [Render's free hosting limits](https://render.com/docs/free) and [billing FAQ](https://render.com/docs/faq).

GitHub stores committed data, encrypted cookies hold sessions, and the browser keeps unsent drafts. The app does not depend on Render's temporary filesystem surviving a restart. GitHub Pages alone cannot run the authentication backend.

## Container configuration

Build from this directory:

```sh
docker build -t teacup-locations-editor .
```

Set these runtime values in the hosting service:

| Variable | Value |
| --- | --- |
| `NODE_ENV` | `production` (already set in the container) |
| `APP_ORIGIN` | The exact public HTTPS origin, with no trailing slash or path; defaults to `RENDER_EXTERNAL_URL` on Render |
| `PORT` | The platform's listening port; the container defaults to `8080` |
| `GITHUB_CLIENT_ID` | The registered GitHub App's client ID |
| `GITHUB_CLIENT_SECRET` | The app's client secret, stored as a hosting secret |
| `SESSION_SECRET` | At least 32 random characters, stored as a hosting secret |
| `GITHUB_REPOSITORY` | `cgs-earth/teacup-generator` |
| `GITHUB_REPOSITORY_ID` | `1135150705` |

Render generates `SESSION_SECRET` from the blueprint. For another container host, generate it with `openssl rand -base64 48` and enter it directly into the host's secret manager. Keep the same secret across replicas. Rotating it signs everyone out. Never commit `.env`, app secrets, or session values. The app does not need the data pipeline's Google Cloud or HydroShare credentials.

The service must accept browser traffic; GitHub authentication and repository permissions control editing. `/healthz` is the health endpoint. Set the GitHub callback to the final service URL before enabling sign-in. Production startup fails when the origin or required secrets are missing.

After deployment, check `/healthz`, browse the reservoir list, complete GitHub sign-in, and verify repository write access. If sign-in fails or unauthorized edits become possible, suspend the service in Render while investigating. For a code regression, use Render's rollback to the previous successful deploy. Suspending or rolling back the editor does not change repository CSV data or existing PRs.

## Request handling

OAuth uses state and PKCE. Access tokens stay in authenticated, encrypted HttpOnly cookies. Production cookies require HTTPS, use the `__Host-` prefix, and expire within eight hours. API responses never expose the access token. Mutations require the configured origin and a session CSRF token.

The server fixes the repository, file path, and base branch. It loads an immutable snapshot of `main` and rejects a submission if the CSV changed while the user was editing. It validates the reviewed field edits again before writing. A retry uses the same branch and returns the existing open PR if one was already created. If GitHub creates the branch but cannot open the PR, the response links to that branch for recovery.

Unsent edits stay in the tab's session storage through GitHub sign-in. Users can download an unfinished CSV draft, including values that still need correction. Invalid values cannot be submitted. If `main` changes, download the draft before reloading and reapplying edits. The editor does not automatically merge concurrent edits or add and delete reservoirs.
