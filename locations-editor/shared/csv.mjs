/** Parse CSV while retaining the byte-for-byte source spans of every field. */
export function parseCsv(text) {
  if (typeof text !== "string" || text.length > 2_000_000)
    throw new Error("The CSV is missing or too large.");
  const records = [];
  let row = [],
    spans = [],
    i = text.startsWith("\uFEFF") ? 1 : 0;
  while (i < text.length) {
    const start = i;
    let value = "";
    if (text[i] === '"') {
      i++;
      let closed = false;
      while (i < text.length) {
        if (text[i] === '"') {
          if (text[i + 1] === '"') {
            value += '"';
            i += 2;
          } else {
            i++;
            closed = true;
            break;
          }
        } else {
          value += text[i++];
        }
      }
      if (!closed) throw new Error("The CSV has an unclosed quoted field.");
      if (i < text.length && ![",", "\r", "\n"].includes(text[i]))
        throw new Error("Unexpected text after a quoted CSV field.");
    } else {
      while (i < text.length && ![",", "\r", "\n"].includes(text[i])) {
        if (text[i] === '"')
          throw new Error("Unexpected quote in an unquoted CSV field.");
        value += text[i++];
      }
    }
    row.push(value);
    spans.push({ start, end: i });
    if (text[i] === ",") {
      i++;
      if (i === text.length) {
        row.push("");
        spans.push({ start: i, end: i });
      } else continue;
    } else if (text[i] === "\r" || text[i] === "\n") {
      if (text[i] === "\r" && text[i + 1] === "\n") i++;
      i++;
    }
    records.push({ values: row, spans });
    row = [];
    spans = [];
  }
  if (!records.length) throw new Error("The CSV is empty.");
  const headers = records[0].values;
  if (headers.some((h) => !h) || new Set(headers).size !== headers.length)
    throw new Error("The CSV has blank or duplicate column names.");
  for (const [index, record] of records.entries()) {
    if (record.values.length !== headers.length)
      throw new Error(
        `CSV row ${index + 1} has ${record.values.length} fields; expected ${headers.length}.`,
      );
  }
  return {
    headers,
    rows: records.slice(1).map((r) => r.values),
    spans: records.slice(1).map((r) => r.spans),
    text,
  };
}

export function encodeField(value, wasQuoted = false) {
  return wasQuoted || /[",\r\n]/.test(value)
    ? `"${value.replaceAll('"', '""')}"`
    : value;
}

/** Apply only the fields the user reviewed; retain all other source characters. */
export function applyChanges(
  document,
  changes,
  { validateValues = true } = {},
) {
  if (
    !Array.isArray(changes) ||
    changes.length === 0 ||
    changes.length > 10_000
  )
    throw new Error("Choose between 1 and 10,000 field changes.");
  const seen = new Set();
  const replacements = changes.map((change) => {
    if (
      !change ||
      !Number.isInteger(change.row) ||
      change.row < 0 ||
      change.row >= document.rows.length
    )
      throw new Error("A changed reservoir no longer exists.");
    const column = document.headers.indexOf(change.column);
    if (
      column < 0 ||
      typeof change.before !== "string" ||
      typeof change.after !== "string" ||
      change.after.length > 20_000
    )
      throw new Error("A field change is invalid.");
    const key = `${change.row}:${column}`;
    if (seen.has(key))
      throw new Error("A field was changed more than once in this request.");
    seen.add(key);
    if (document.rows[change.row][column] !== change.before)
      throw new Error("A field no longer matches the version you reviewed.");
    if (change.before === change.after)
      throw new Error("The request contains an unchanged field.");
    if (validateValues) validateField(change.column, change.after);
    const span = document.spans[change.row][column];
    return {
      ...span,
      value: encodeField(change.after, document.text[span.start] === '"'),
    };
  });
  let text = document.text;
  for (const r of replacements.sort((a, b) => b.start - a.start))
    text = text.slice(0, r.start) + r.value + text.slice(r.end);
  return text;
}

export function validateField(column, value) {
  if (value.includes("\0"))
    throw new Error(`${column} cannot contain a null character.`);
  if (column === "Name" && !value.trim())
    throw new Error("A reservoir needs a name.");
  if (!value.trim()) return;
  const numeric = value.replaceAll(",", "");
  if (
    [
      "Total Capacity",
      "Active Capacity",
      "Live Capacity",
      "capacity_value",
    ].includes(column)
  ) {
    if (
      !/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(numeric) ||
      !Number.isFinite(Number(numeric))
    )
      throw new Error(`${column} must be a nonnegative number or blank.`);
  }
  if (column === "Latitude" || column === "Longitude") {
    const limit = column === "Latitude" ? 90 : 180;
    if (!Number.isFinite(Number(value)) || Math.abs(Number(value)) > limit)
      throw new Error(`${column} must be between -${limit} and ${limit}.`);
  }
  if (column === "huc6" && !/^\d{6}$/.test(value))
    throw new Error("HUC6 must contain six digits or be blank.");
  if (
    ["Historical API URL", "wwdh_30yr_api_url", "source_30yr_api_url"].includes(
      column,
    )
  ) {
    let url;
    try {
      url = new URL(value);
    } catch {
      throw new Error(`${column} must be a complete HTTP or HTTPS URL.`);
    }
    if (!["http:", "https:"].includes(url.protocol))
      throw new Error(`${column} must use HTTP or HTTPS.`);
  }
}
