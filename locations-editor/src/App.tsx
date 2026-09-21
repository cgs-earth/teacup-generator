import { useEffect, useMemo, useRef, useState } from "react";
import {
  CheckCircle2,
  Download,
  ExternalLink,
  GitBranch,
  GitPullRequest,
  RefreshCw,
} from "lucide-react";
import { applyChanges, parseCsv } from "../shared/csv.mjs";
import { api, ApiError } from "./api";
import { ReservoirTable } from "./components/ReservoirTable";
import { ReservoirForm } from "./components/ReservoirForm";
import { ReviewDialog } from "./components/ReviewDialog";
import type {
  Change,
  CsvDocument,
  PullRequest,
  Session,
  Snapshot,
} from "./types";

const draftKey = "teacup-reservoir-draft-v1";
const authErrors: Record<string, string> = {
  expired: "The sign-in request expired. Sign in again.",
  access:
    "GitHub sign-in succeeded, but you need repository write access and the app must be installed on teacup-generator.",
  github:
    "GitHub sign-in could not finish. Try again or contact the repository administrator.",
};

export default function App() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [session, setSession] = useState<Session | null>(null);
  const [changes, setChanges] = useState<Change[]>([]);
  const [selected, setSelected] = useState(1);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [reviewOpen, setReviewOpen] = useState(false);
  const [reviewError, setReviewError] = useState("");
  const [recoveryUrl, setRecoveryUrl] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [pr, setPr] = useState<PullRequest | null>(null);
  const requestId = useRef(crypto.randomUUID());
  const initialized = useRef(false);
  const document = useMemo(
    () => (snapshot ? (parseCsv(snapshot.text) as CsvDocument) : null),
    [snapshot],
  );
  const rows = useMemo(() => {
    if (!document) return [];
    const result = document.rows.map((r) => [...r]);
    for (const c of changes)
      if (result[c.row])
        result[c.row][document.headers.indexOf(c.column)] = c.after;
    return result;
  }, [document, changes]);
  const changedRows = useMemo(
    () => new Set(changes.map((c) => c.row)),
    [changes],
  );
  const editable = Boolean(session?.user);

  async function load(discard = false) {
    if (
      discard &&
      changes.length &&
      !window.confirm(
        "Discard your unsent changes and reload main? Download a draft first if you want to keep a copy.",
      )
    )
      return;
    setLoading(true);
    setError("");
    try {
      const [sessionResult, file] = await Promise.all([
        api<Session>("/api/session").catch((e: ApiError) => {
          setError(e.message);
          return null;
        }),
        api<Snapshot>("/api/locations"),
      ]);
      setSession(sessionResult);
      parseCsv(file.text);
      if (discard) {
        setChanges([]);
        setNotice("");
        setPr(null);
        requestId.current = crypto.randomUUID();
      }
      let restored = false;
      if (!initialized.current && !discard) {
        try {
          const saved = JSON.parse(sessionStorage.getItem(draftKey) || "null");
          if (
            saved?.snapshot?.repository === file.repository &&
            saved?.changes?.length
          ) {
            applyChanges(parseCsv(saved.snapshot.text), saved.changes, {
              validateValues: false,
            });
            setSnapshot(saved.snapshot);
            setChanges(saved.changes);
            requestId.current = saved.requestId;
            setNotice(
              saved.snapshot.sha === file.sha
                ? "Your unsent draft has been restored."
                : "Your draft has been restored, but main changed. Download the draft before reloading the latest file.",
            );
            restored = true;
          }
        } catch {
          sessionStorage.removeItem(draftKey);
        }
      }
      if (!restored) setSnapshot(file);
      initialized.current = true;
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    const authError = new URLSearchParams(window.location.search).get(
      "auth_error",
    );
    if (authError) {
      setNotice(authErrors[authError] || authErrors.github);
      history.replaceState(null, "", "/");
    }
    void load();
  }, []);
  useEffect(() => {
    if (!initialized.current) return;
    try {
      if (changes.length && snapshot)
        sessionStorage.setItem(
          draftKey,
          JSON.stringify({ snapshot, changes, requestId: requestId.current }),
        );
      else sessionStorage.removeItem(draftKey);
    } catch {
      setNotice(
        "This browser cannot save drafts between visits. Keep this tab open or download your draft.",
      );
    }
    const handler = (event: BeforeUnloadEvent) => {
      if (changes.length) {
        event.preventDefault();
        event.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [changes, snapshot]);
  function changeField(column: string, after: string) {
    if (!document || !editable) return;
    requestId.current = crypto.randomUUID();
    const before = document.rows[selected][document.headers.indexOf(column)];
    setChanges((current) => [
      ...current.filter((c) => !(c.row === selected && c.column === column)),
      ...(before === after ? [] : [{ row: selected, column, before, after }]),
    ]);
    setPr(null);
  }
  function review() {
    if (!document) return;
    try {
      applyChanges(document, changes);
      setReviewError("");
      setRecoveryUrl(undefined);
      setReviewOpen(true);
    } catch (e) {
      setError((e as Error).message);
    }
  }
  function download() {
    if (!document) return;
    try {
      const text = changes.length
        ? applyChanges(document, changes, { validateValues: false })
        : document.text;
      const url = URL.createObjectURL(
        new Blob([text], { type: "text/csv;charset=utf-8" }),
      );
      const a = window.document.createElement("a");
      a.href = url;
      a.download = "locations-draft.csv";
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError((e as Error).message);
    }
  }
  async function submit(title: string, body: string) {
    if (!snapshot || !session?.csrf) return;
    setBusy(true);
    setReviewError("");
    try {
      const result = await api<PullRequest>("/api/pull-requests", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": session.csrf,
        },
        body: JSON.stringify({
          sha: snapshot.sha,
          requestId: requestId.current,
          changes,
          title,
          body,
        }),
      });
      setPr(result);
      setChanges([]);
      setReviewOpen(false);
      requestId.current = crypto.randomUUID();
      setNotice("");
    } catch (e) {
      const failure = e as ApiError;
      setReviewError(failure.message);
      setRecoveryUrl(failure.recoveryUrl);
      if (failure.status === 401) {
        setReviewOpen(false);
        setSession((s) => (s ? { ...s, user: null } : null));
        setError(failure.message);
      }
    } finally {
      setBusy(false);
    }
  }
  async function logout() {
    if (!session?.csrf) return;
    try {
      await api("/auth/logout", {
        method: "POST",
        headers: { "X-CSRF-Token": session.csrf },
      });
      setSession((s) => (s ? { ...s, user: null } : null));
    } catch (e) {
      setError((e as Error).message);
    }
  }
  return (
    <div className="app-shell">
      <header className="app-header">
        <span className="brand">Reservoir editor</span>
        <a
          className="repo-link"
          href="https://github.com/cgs-earth/teacup-generator"
          target="_blank"
          rel="noreferrer"
        >
          cgs-earth / teacup-generator
        </a>
        <div className="account">
          {session?.user ? (
            <>
              <span>{session.user.login}</span>
              <button onClick={logout}>Sign out</button>
            </>
          ) : session && !session.configured ? (
            <button disabled title="GitHub sign-in needs administrator setup">
              Sign in with GitHub
            </button>
          ) : (
            <a className="connect" href="/auth/login">
              Sign in with GitHub
            </a>
          )}
        </div>
      </header>
      <main>
        <div className="workspace-heading">
          <div>
            <h1>Reservoir locations</h1>
            <p>Edit the source list for the Western Water Datahub.</p>
          </div>
          <div className="workspace-actions">
            <span className="base-branch">
              <GitBranch size={18} />
              PRs target <strong>main</strong>
            </span>
            <button
              className="primary"
              onClick={review}
              disabled={!changes.length || !editable || busy}
            >
              Review changes
              {changes.length > 0 && (
                <span className="button-count">{changes.length}</span>
              )}
            </button>
          </div>
        </div>
        <div className="workspace-body">
          {session && !session.configured && (
            <div className="notice">
              GitHub sign-in needs administrator setup. You can browse the
              current reservoir list.
            </div>
          )}
          {error && (
            <div className="notice error" role="alert">
              {error}
              <button className="text-button" onClick={() => setError("")}>
                Dismiss
              </button>
            </div>
          )}
          {notice && (
            <div className="notice" role="status">
              {notice}
            </div>
          )}
          {pr && (
            <div className="notice success" role="status">
              <GitPullRequest size={21} />
              <div>
                <strong>Pull request #{pr.number} is open.</strong>
                <span>
                  Main stays unchanged until someone merges it. GeoJSON is
                  regenerated after merge.
                </span>
              </div>
              <a href={pr.url} target="_blank" rel="noreferrer">
                View pull request
                <ExternalLink size={17} />
              </a>
            </div>
          )}
          {loading && !document ? (
            <div className="loading" role="status">
              <span className="spinner" />
              Loading reservoir locations…
            </div>
          ) : document && rows[selected] ? (
            <div className="editor">
              <ReservoirTable
                headers={document.headers}
                rows={rows}
                selected={selected}
                changedRows={changedRows}
                onSelect={setSelected}
              />
              <ReservoirForm
                headers={document.headers}
                row={rows[selected]}
                baseline={document.rows[selected]}
                allRows={document.rows}
                editable={editable && !busy}
                onChange={changeField}
                onReset={() => {
                  requestId.current = crypto.randomUUID();
                  setChanges((current) =>
                    current.filter((c) => c.row !== selected),
                  );
                }}
              />
            </div>
          ) : (
            <div className="empty">
              The reservoir list could not be loaded.
              <button className="secondary" onClick={() => void load()}>
                Try again
              </button>
            </div>
          )}
        </div>
      </main>
      <footer className="status-bar">
        <div>
          <CheckCircle2
            size={19}
            className={changes.length ? "pending-icon" : "saved-icon"}
          />
          <span>
            {changes.length
              ? `${changes.length} unsent field ${changes.length === 1 ? "change" : "changes"} in ${changedRows.size} ${changedRows.size === 1 ? "reservoir" : "reservoirs"}`
              : "No unsent changes"}
          </span>
        </div>
        <div className="status-actions">
          {changes.length > 0 && (
            <button onClick={download}>
              <Download size={16} />
              Download draft
            </button>
          )}
          <button onClick={() => void load(true)} disabled={loading || busy}>
            <RefreshCw size={16} />
            Reload main
          </button>
          <span className="file-path">R-workflow/config/locations.csv</span>
        </div>
      </footer>
      {document && (
        <ReviewDialog
          open={reviewOpen}
          changes={changes}
          document={document}
          busy={busy}
          error={reviewError}
          recoveryUrl={recoveryUrl}
          onClose={() => setReviewOpen(false)}
          onSubmit={submit}
        />
      )}
    </div>
  );
}
