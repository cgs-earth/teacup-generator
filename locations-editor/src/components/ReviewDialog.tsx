import { useEffect, useRef, useState } from "react";
import { ExternalLink, GitPullRequest, X } from "lucide-react";
import type { Change, CsvDocument } from "../types";

type Props = {
  open: boolean;
  changes: Change[];
  document: CsvDocument;
  busy: boolean;
  error: string;
  recoveryUrl?: string;
  onClose: () => void;
  onSubmit: (title: string, body: string) => void;
};
export function ReviewDialog({
  open,
  changes,
  document,
  busy,
  error,
  recoveryUrl,
  onClose,
  onSubmit,
}: Props) {
  const ref = useRef<HTMLDialogElement>(null);
  const [title, setTitle] = useState("Update reservoir metadata");
  const [body, setBody] = useState("");
  const reservoirs = new Set(changes.map((c) => c.row)).size;
  useEffect(() => {
    if (open && !ref.current?.open) ref.current?.showModal();
    else if (!open && ref.current?.open) ref.current?.close();
  }, [open]);
  return (
    <dialog
      ref={ref}
      className="review-dialog"
      aria-labelledby="review-title"
      onCancel={(e) => {
        e.preventDefault();
        if (!busy) onClose();
      }}
    >
      <div className="dialog-heading">
        <div>
          <h2 id="review-title">Review changes</h2>
          <p>
            {changes.length} field {changes.length === 1 ? "change" : "changes"}{" "}
            in {reservoirs} {reservoirs === 1 ? "reservoir" : "reservoirs"}
          </p>
        </div>
        <button
          aria-label="Close review"
          className="icon-button"
          disabled={busy}
          onClick={onClose}
        >
          <X size={21} />
        </button>
      </div>
      <p className="review-context">
        This opens a pull request against <strong>main</strong>. Your edits stay
        on a separate branch until the PR is merged.
      </p>
      <div className="review-table">
        <table>
          <thead>
            <tr>
              <th>Reservoir / field</th>
              <th>Current value</th>
              <th>Proposed value</th>
            </tr>
          </thead>
          <tbody>
            {changes.map((c) => (
              <tr key={`${c.row}:${c.column}`}>
                <td>
                  <strong>
                    {document.rows[c.row][document.headers.indexOf("Name")]}
                  </strong>
                  <span>{c.column}</span>
                </td>
                <td className="before" data-label="Current value">
                  {c.before || <em>Blank</em>}
                </td>
                <td className="after" data-label="Proposed value">
                  {c.after || <em>Blank</em>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          onSubmit(title, body);
        }}
      >
        <label htmlFor="pr-title">Pull request title</label>
        <input
          id="pr-title"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          minLength={5}
          maxLength={200}
          required
          disabled={busy}
        />
        <label htmlFor="pr-body">
          Why are you making these changes?{" "}
          <span className="muted">Optional</span>
        </label>
        <textarea
          id="pr-body"
          value={body}
          onChange={(e) => setBody(e.target.value)}
          maxLength={10_000}
          rows={3}
          disabled={busy}
        />
        {error && (
          <div className="notice error" role="alert">
            {error}
            {recoveryUrl && (
              <a href={recoveryUrl} target="_blank" rel="noreferrer">
                Open the draft branch
                <ExternalLink size={15} />
              </a>
            )}
          </div>
        )}
        <div className="dialog-footer">
          <button
            type="button"
            className="secondary"
            disabled={busy}
            onClick={onClose}
          >
            Keep editing
          </button>
          <button
            type="submit"
            className="primary"
            disabled={busy || title.trim().length < 5}
          >
            <GitPullRequest size={18} />
            {busy ? "Opening pull request…" : "Open pull request"}
          </button>
        </div>
      </form>
    </dialog>
  );
}
