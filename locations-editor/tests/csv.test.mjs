import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { applyChanges, parseCsv } from "../shared/csv.mjs";

test("the source file parses and a field edit changes only its original span", async () => {
  const text = await readFile(
    new URL("../../R-workflow/config/locations.csv", import.meta.url),
    "utf8",
  );
  const doc = parseCsv(text);
  assert(doc.rows.length > 0);
  const column = doc.headers.indexOf("Name");
  assert(column >= 0);
  const result = applyChanges(doc, [
    {
      row: 0,
      column: "Name",
      before: doc.rows[0][column],
      after: "Example reservoir",
    },
  ]);
  const span = doc.spans[0][column];
  const replacement =
    text[span.start] === '"' ? '"Example reservoir"' : "Example reservoir";
  assert.equal(
    result,
    text.slice(0, span.start) + replacement + text.slice(span.end),
  );
  assert.equal(parseCsv(result).rows[0][column], "Example reservoir");
});
test("unfinished invalid values can be saved as a draft but cannot be submitted", () => {
  const doc = parseCsv("Name,Latitude\nAgate,45\n");
  const edits = [{ row: 0, column: "Latitude", before: "45", after: "-" }];
  assert.equal(
    applyChanges(doc, edits, { validateValues: false }),
    "Name,Latitude\nAgate,-\n",
  );
  assert.throws(() => applyChanges(doc, edits), /Latitude/);
});
test("quoted commas, multiline text, Unicode, BOM, CRLF and trailing blanks survive", () => {
  const text =
    '\uFEFFName,Notes,Identifier\r\n"A, B","Line one\r\n""Line two""",001\r\nC,,\r\n';
  const doc = parseCsv(text);
  assert.deepEqual(doc.rows, [
    ["A, B", 'Line one\r\n"Line two"', "001"],
    ["C", "", ""],
  ]);
  const result = applyChanges(doc, [
    { row: 1, column: "Notes", before: "", after: 'Café, "water"\nnotes' },
  ]);
  assert.equal(
    result,
    text.replace("C,,\r\n", 'C,"Café, ""water""\nnotes",\r\n'),
  );
  assert.equal(parseCsv(result).rows[1][1], 'Café, "water"\nnotes');
});
test("stale fields, duplicate edits, missing rows, malformed CSV and bad values fail", () => {
  const doc = parseCsv("Name,Latitude\nAgate,45\n");
  const valid = { row: 0, column: "Latitude", before: "45", after: "44" };
  for (const changes of [
    [{ ...valid, before: "1" }],
    [valid, valid],
    [{ ...valid, row: 99 }],
    [{ ...valid, after: "100" }],
  ])
    assert.throws(() => applyChanges(doc, changes));
  assert.throws(() => parseCsv('a,b\n"unclosed,b'));
  assert.throws(() => parseCsv("a,a\n1,2"));
  assert.throws(() => parseCsv("a,b\n1,2,3"));
});
