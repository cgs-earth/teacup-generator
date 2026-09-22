import { useState } from "react";
import { validateField } from "../../shared/csv.mjs";

const groups: Record<string, string[]> = {
  Overview: [
    "Name",
    "Post-Review Decision",
    "Preferred Label for Map and Table",
    "Preferred Label for PopUp and Modal",
    "mng_reg_name",
    "reservoir_notes",
  ],
  Capacity: [
    "Total Capacity",
    "Active Capacity",
    "Live Capacity",
    "storage_capacity_label",
    "capacity_value",
    "Has Exclusive Flood Control Pool?",
    "Source for Capacity",
  ],
  "Data sources": [
    "Source_Name",
    "Identifier",
    "Source for Storage Data",
    "Storage Data Type",
    "Historical API URL",
    "RISE Parameter ID for Storage Data",
    "USGS Parameter Code",
    "wwdh_30yr_api_url",
    "source_30yr_api_url",
    "Data Source Notes",
  ],
};
const labels: Record<string, string> = {
  "Post-Review Decision": "Review decision",
  "Preferred Label for Map and Table": "Map and table label",
  "Preferred Label for PopUp and Modal": "Popup and modal label",
  mng_reg_name: "Management region",
  reservoir_notes: "Reservoir notes",
  Source_Name: "Data source",
  capacity_value: "Display capacity",
  storage_capacity_label: "Capacity used for display",
  huc6: "HUC6",
  doiRegion: "DOI region",
  state: "State",
  mng_reg_num: "Management region number",
  mng_reg_abbr: "Management region abbreviation",
};
type Props = {
  headers: string[];
  row: string[];
  baseline: string[];
  allRows: string[][];
  editable: boolean;
  onChange: (column: string, value: string) => void;
  onReset: () => void;
};
export function ReservoirForm({
  headers,
  row,
  baseline,
  allRows,
  editable,
  onChange,
  onReset,
}: Props) {
  const [tab, setTab] = useState("Overview");
  const fields = tab === "All fields" ? headers : groups[tab];
  const changed = row.some((v, i) => v !== baseline[i]);
  return (
    <section className="detail-pane" aria-label="Reservoir details">
      <div className="detail-heading">
        <h2>{row[headers.indexOf("Name")] || "Unnamed reservoir"}</h2>
        <p>Identifier {row[headers.indexOf("Identifier")] || "not set"}</p>
      </div>
      <div className="tabs" aria-label="Field groups">
        {[...Object.keys(groups), "All fields"].map((name) => (
          <button
            key={name}
            aria-pressed={tab === name}
            className={tab === name ? "active" : ""}
            onClick={() => setTab(name)}
          >
            {name}
          </button>
        ))}
      </div>
      {!editable && (
        <p className="readonly-note">
          Sign in with GitHub to edit. You need repository write access.
        </p>
      )}
      <div className="fields">
        {fields.map((column) => {
          const index = headers.indexOf(column),
            value = row[index];
          if (index < 0) return null;
          const edited = value !== baseline[index];
          let error = "";
          if (edited) {
            try {
              validateField(column, value);
            } catch (e) {
              error = (e as Error).message;
            }
          }
          const id = `field-${index}`;
          const props = {
            id,
            value,
            disabled: !editable,
            onChange: (
              e: React.ChangeEvent<
                HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement
              >,
            ) => onChange(column, e.target.value),
            "aria-invalid": Boolean(error),
            "aria-describedby": error ? `${id}-error` : undefined,
          };
          const multiline = /notes|Actions to Allow Inclusion/i.test(column);
          const choices =
            column === "Post-Review Decision"
              ? [...new Set(allRows.map((r) => r[index]))]
                  .filter(Boolean)
                  .sort()
              : null;
          return (
            <div className={`field${edited ? " edited" : ""}`} key={column}>
              <label htmlFor={id}>
                {labels[column] || column}
                {edited && <span aria-hidden="true">Edited</span>}
              </label>
              {choices ? (
                <select {...props}>
                  {choices.map((v) => (
                    <option key={v}>{v}</option>
                  ))}
                </select>
              ) : multiline ? (
                <textarea
                  {...props}
                  rows={column === "reservoir_notes" ? 3 : 4}
                />
              ) : (
                <input
                  {...props}
                  type="text"
                  autoComplete="off"
                  spellCheck={!/URL|Identifier|huc6|Code/.test(column)}
                />
              )}
              {tab === "All fields" && labels[column] && (
                <span className="field-key">{column}</span>
              )}
              {error && (
                <span id={`${id}-error`} className="field-error">
                  {error}
                </span>
              )}
            </div>
          );
        })}
      </div>
      <div className="form-footer">
        <button
          className="secondary"
          onClick={onReset}
          disabled={!changed || !editable}
        >
          Reset this reservoir
        </button>
      </div>
    </section>
  );
}
