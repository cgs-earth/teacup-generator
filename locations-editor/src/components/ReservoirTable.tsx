import { ChevronLeft, ChevronRight, Search } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

type Props = {
  headers: string[];
  rows: string[][];
  selected: number;
  changedRows: Set<number>;
  onSelect: (row: number) => void;
};
export function ReservoirTable({
  headers,
  rows,
  selected,
  changedRows,
  onSelect,
}: Props) {
  const [search, setSearch] = useState("");
  const [source, setSource] = useState("");
  const [page, setPage] = useState(0);
  const nameCol = headers.indexOf("Name"),
    sourceCol = headers.indexOf("Source_Name");
  const decisionCol = headers.indexOf("Post-Review Decision"),
    capacityCol = headers.indexOf("capacity_value");
  const sources = useMemo(
    () => [...new Set(rows.map((r) => r[sourceCol]).filter(Boolean))].sort(),
    [rows, sourceCol],
  );
  const filtered = useMemo(
    () =>
      rows
        .map((row, index) => ({ row, index }))
        .filter(
          ({ row }) =>
            (!source || row[sourceCol] === source) &&
            row.some((value) =>
              value.toLowerCase().includes(search.toLowerCase()),
            ),
        ),
    [rows, search, source, sourceCol],
  );
  const pages = Math.max(1, Math.ceil(filtered.length / 20));
  useEffect(() => {
    setPage(0);
  }, [source, search]);
  return (
    <section className="list-pane" aria-label="Reservoir list">
      <div className="filters">
        <div className="search">
          <Search size={19} aria-hidden="true" />
          <input
            aria-label="Search reservoirs"
            placeholder="Search reservoirs"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <select
          aria-label="Filter by source"
          value={source}
          onChange={(e) => setSource(e.target.value)}
        >
          <option value="">All sources</option>
          {sources.map((s) => (
            <option key={s}>{s}</option>
          ))}
        </select>
      </div>
      <div className="count" aria-live="polite">
        {filtered.length.toLocaleString()}{" "}
        {filtered.length === 1 ? "reservoir" : "reservoirs"}
        {filtered.length !== rows.length ? ` of ${rows.length}` : ""}
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th scope="col">Reservoir</th>
              <th scope="col">Source</th>
              <th scope="col">Decision</th>
              <th scope="col" className="number">
                Capacity
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.slice(page * 20, page * 20 + 20).map(({ row, index }) => (
              <tr key={index} className={selected === index ? "selected" : ""}>
                <td>
                  <button
                    className="row-button"
                    aria-pressed={selected === index}
                    onClick={() => onSelect(index)}
                  >
                    {row[nameCol]}
                    {changedRows.has(index) && (
                      <span className="changed-dot" aria-label="Edited" />
                    )}
                  </button>
                </td>
                <td>
                  {row[sourceCol] || <span className="muted">Not set</span>}
                </td>
                <td>{row[decisionCol]}</td>
                <td className="number">
                  {row[capacityCol] &&
                  Number.isFinite(Number(row[capacityCol].replaceAll(",", "")))
                    ? Number(
                        row[capacityCol].replaceAll(",", ""),
                      ).toLocaleString("en-US", { maximumFractionDigits: 8 })
                    : row[capacityCol] || (
                        <span className="muted">Not set</span>
                      )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div className="empty">
            No reservoirs match these filters.
            <button
              className="text-button"
              onClick={() => {
                setSearch("");
                setSource("");
              }}
            >
              Clear filters
            </button>
          </div>
        )}
      </div>
      <div className="pagination">
        <button
          className="secondary"
          disabled={page === 0}
          onClick={() => setPage((p) => p - 1)}
        >
          <ChevronLeft size={17} />
          Previous
        </button>
        <span>
          Page {page + 1} of {pages}
        </span>
        <button
          className="secondary"
          disabled={page >= pages - 1}
          onClick={() => setPage((p) => p + 1)}
        >
          Next
          <ChevronRight size={17} />
        </button>
      </div>
    </section>
  );
}
