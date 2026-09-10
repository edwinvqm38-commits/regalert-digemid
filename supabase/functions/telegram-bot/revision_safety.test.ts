import assert from "node:assert/strict";
import test from "node:test";

import {
  type OrdinalRoundtripEvidence,
  buildOrdinalEditableTableBlock,
  buildTableRegionCoverage,
  canMarkTableVerified,
  extractMarkdownTableMatrices,
  extractMarkdownTableHeaders,
  hasValidMarkdownTable,
  inspectMarkdownTables,
  isSyntheticColumnPlaceholder,
  isTrustedTableVerification,
  ORDINAL_COLUMN_IDENTITY_METHOD,
  rebuildOrdinalEditableTables,
  splitReviewedPageText,
  tableVerificationDiagnostics,
  TABLE_VERIFICATION_METHOD,
} from "./revision_safety.ts";

const sha256 = "a".repeat(64);
const validTable = "| Categoría | Valor |\n| --- | --- |\n| A | 1 |";
const secondTable = "| Tipo | Plazo |\n| --- | --- |\n| B | 10 días |";
const twoTables = `Tabla 1:\n\n${validTable}\n\nTabla 2:\n\n${secondTable}`;
const confirmed = "# NORMA: X\n# VERIFICACION_TABLAS: CONFIRMADA";

function ordinalEvidence(
  tableCount = 1,
  overrides: Partial<OrdinalRoundtripEvidence> = {},
): OrdinalRoundtripEvidence {
  return {
    column_identity: ORDINAL_COLUMN_IDENTITY_METHOD,
    editable_table_count: tableCount,
    invalid_block_count: 0,
    cardinality_preserved: true,
    source_headers_matched: true,
    synthetic_headers_present: false,
    empty_headers_present: false,
    duplicate_header_ambiguity: false,
    duplicate_headers: [],
    ...overrides,
  };
}

function withoutRoundtripText(roundtrip: ReturnType<typeof rebuildOrdinalEditableTables>) {
  const { text: _text, ...evidence } = roundtrip;
  return evidence;
}

function trustedPage(
  overrides: Record<string, unknown> = {},
  ordinal = ordinalEvidence(),
): any {
  return {
    has_tables: true,
    tabla_verificada: true,
    revisado_manual: true,
    metadata: {
      source_pdf_sha256: sha256,
      tables_markdown: validTable,
      table_regions: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
      manual_review: {
        table_verified: true,
        table_confirmation_requested: true,
        table_structure_present: true,
        table_verification: {
          confirmation_explicit: true,
          verification_method: TABLE_VERIFICATION_METHOD,
          verified_at: "2026-09-01T00:00:00.000Z",
          source_pdf_sha256: sha256,
          ordinal_columns: ordinal,
          region_coverage: {
            detected_regions: 1,
            verified_regions: 1,
            unresolved_regions: 0,
            coverage_unknown: false,
          },
        },
      },
      ...overrides,
    },
  };
}

test("revision textual sin confirmacion no verifica tabla", () => {
  assert.equal(canMarkTableVerified("# NORMA: X", true, validTable, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(),
  }), false);
});

test("confirmacion exige sha256 de la fuente", () => {
  assert.equal(canMarkTableVerified(confirmed, true, validTable, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    ordinalRoundtrip: ordinalEvidence(),
  }), false);
});

test("confirmacion, estructura estricta, hash y cobertura completa permiten verificar", () => {
  assert.equal(canMarkTableVerified(confirmed, true, validTable, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(),
  }), true);
});

test("markdown con filas de ancho inconsistente se rechaza", () => {
  const ragged = "| A | B |\n| --- | --- |\n| solo una |";
  assert.equal(hasValidMarkdownTable(ragged), false);
  assert.ok(inspectMarkdownTables(ragged).errors.includes("inconsistent_column_count"));
});

test("tabla vacia o decorativa se rechaza", () => {
  const decorative = "|  |  |\n| --- | --- |\n|  |  |";
  assert.equal(hasValidMarkdownTable(decorative), false);
});

test("una tabla no cubre dos regiones detectadas", () => {
  const options = {
    regionSummary: { detected_regions: 2, accepted_regions: 2, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(),
  };
  assert.equal(canMarkTableVerified(confirmed, true, validTable, options), false);
  assert.deepEqual(buildTableRegionCoverage(validTable, options.regionSummary, true), {
    detected_regions: 2,
    verified_regions: 1,
    unresolved_regions: 1,
    coverage_unknown: false,
  });
});

test("dos tablas validas cubren exactamente dos regiones", () => {
  assert.equal(canMarkTableVerified(confirmed, true, twoTables, {
    regionSummary: { detected_regions: 2, accepted_regions: 2, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(2),
  }), true);
});

test("separa estructura revisada sin contaminar el texto fuente", () => {
  const page = [
    "Texto narrativo original.",
    "===== ESTRUCTURAS TABULARES SEPARADAS DE TEXT_RAW =====",
    validTable,
    "===== FIN ESTRUCTURAS TABULARES =====",
  ].join("\n\n");

  const result = splitReviewedPageText(page);
  assert.equal(result.text, "Texto narrativo original.");
  assert.equal(result.tablesMarkdown, validTable);
});

test("tabla_verificada legacy sin evidencia explicita no es confiable", () => {
  assert.equal(isTrustedTableVerification({ tabla_verificada: true, metadata: {} }), false);
});

test("revisado_manual y markdown no bastan", () => {
  assert.equal(isTrustedTableVerification({
    has_tables: true,
    tabla_verificada: true,
    metadata: {
      source_pdf_sha256: sha256,
      tables_markdown: validTable,
      manual_review: { table_verified: true, text_reviewed: true },
    },
  }), false);
});

test("evidencia moderna completa es confiable", () => {
  assert.equal(isTrustedTableVerification(trustedPage()), true);
});

test("etiqueta Tabla de una unica estructura conserva una tabla valida", () => {
  const labeledTable = `Tabla:\n\n${validTable}`;

  assert.equal(hasValidMarkdownTable(labeledTable), true);
  assert.equal(canMarkTableVerified(confirmed, true, labeledTable, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(),
  }), true);
});

test("region pendiente invalida evidencia moderna", () => {
  const page = trustedPage({
    table_regions: { detected_regions: 2, accepted_regions: 1, unresolved_regions: 1 },
  });
  assert.equal(isTrustedTableVerification(page), false);
});

test("hash de revision debe coincidir con la fuente", () => {
  const page = trustedPage();
  (page.metadata.manual_review.table_verification as Record<string, unknown>)
    .source_pdf_sha256 = "b".repeat(64);
  assert.equal(isTrustedTableVerification(page), false);
});

test("caso A: placeholder visual conserva encabezado fuente vacio y no eleva confianza", () => {
  const headers = ["", "BOTICA"];
  const editable = buildOrdinalEditableTableBlock(headers, [["A", "10"]], 1).join("\n");
  const roundtrip = rebuildOrdinalEditableTables(editable, [headers]);

  assert.ok(editable.includes("C1 [Columna 1]: A"));
  assert.deepEqual(extractMarkdownTableHeaders(roundtrip.text)[0], headers);
  assert.equal(roundtrip.synthetic_headers_present, true);
  assert.equal(roundtrip.empty_headers_present, true);
  assert.equal(hasValidMarkdownTable(roundtrip.text), false);
  assert.equal(canMarkTableVerified(confirmed, true, roundtrip.text, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: roundtrip,
  }), false);
});

test("caso B: encabezados duplicados conservan tres columnas y valores por posicion", () => {
  const headers = ["TIPO", "VALOR", "VALOR"];
  const editable = buildOrdinalEditableTableBlock(headers, [["A", "10", "20"]], 1).join("\n");
  const roundtrip = rebuildOrdinalEditableTables(editable, [headers]);

  assert.deepEqual(extractMarkdownTableHeaders(roundtrip.text)[0], headers);
  assert.ok(roundtrip.text.includes("| A | 10 | 20 |"));
  assert.equal(roundtrip.cardinality_preserved, true);
  assert.equal(roundtrip.invalid_block_count, 0);
});

test("caso C: dos encabezados vacios conservan C1 y C2 distintos sin persistir placeholders", () => {
  const headers = ["", ""];
  const editable = buildOrdinalEditableTableBlock(headers, [["izquierda", "derecha"]], 1)
    .join("\n");
  const roundtrip = rebuildOrdinalEditableTables(editable, [headers]);

  assert.ok(editable.includes("C1 [Columna 1]: izquierda"));
  assert.ok(editable.includes("C2 [Columna 2]: derecha"));
  assert.deepEqual(extractMarkdownTableHeaders(roundtrip.text)[0], headers);
  assert.ok(roundtrip.text.includes("| izquierda | derecha |"));
  assert.equal(roundtrip.synthetic_headers_present, true);
});

test("caso D: reenviar plantilla ambigua sin cambios mantiene autoridad no confiable", () => {
  const headers = ["", "BOTICA"];
  const roundtrip = rebuildOrdinalEditableTables(
    buildOrdinalEditableTableBlock(headers, [["A", "10"]], 1).join("\n"),
    [headers],
  );
  const page = trustedPage(
    { tables_markdown: roundtrip.text },
    withoutRoundtripText(roundtrip),
  );

  assert.equal(isTrustedTableVerification(page), false);
});

test("caso E: cambiar Columna 1 no repara el encabezado y rompe el esquema ordinal", () => {
  const headers = ["", "BOTICA"];
  const original = buildOrdinalEditableTableBlock(headers, [["A", "10"]], 1).join("\n");
  const edited = original.replace("C1 [Columna 1]:", "C1 [INFRACCION]:");
  const roundtrip = rebuildOrdinalEditableTables(edited, [headers]);

  assert.equal(roundtrip.invalid_block_count, 1);
  assert.equal(roundtrip.source_headers_matched, false);
  assert.equal(roundtrip.cardinality_preserved, false);
  assert.equal(canMarkTableVerified(confirmed, true, roundtrip.text, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: roundtrip,
  }), false);
});

test("alterar la cardinalidad ordinal de una fila se rechaza", () => {
  const headers = ["TIPO", "VALOR", "UNIDAD"];
  const original = buildOrdinalEditableTableBlock(headers, [["A", "10", "UIT"]], 1)
    .join("\n");
  const edited = original.replace("C2 [VALOR]: 10\n", "");
  const roundtrip = rebuildOrdinalEditableTables(edited, [headers]);

  assert.equal(roundtrip.invalid_block_count, 1);
  assert.equal(roundtrip.cardinality_preserved, false);
  assert.equal(roundtrip.source_headers_matched, false);
});

test("caso F: encabezados normales y unicos conservan el flujo valido", () => {
  const headers = ["TIPO", "VALOR"];
  const roundtrip = rebuildOrdinalEditableTables(
    buildOrdinalEditableTableBlock(headers, [["A", "10"]], 1).join("\n"),
    [headers],
  );

  assert.equal(hasValidMarkdownTable(roundtrip.text), true);
  assert.equal(canMarkTableVerified(confirmed, true, roundtrip.text, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: roundtrip,
  }), true);
  assert.equal(isTrustedTableVerification(trustedPage(
    { tables_markdown: roundtrip.text },
    withoutRoundtripText(roundtrip),
  )), true);
});

test("caso G: encabezados duplicados validos no colisionan ni pierden columnas", () => {
  const headers = ["TIPO", "VALOR", "VALOR"];
  const rows = [["A", "10", "20"], ["B", "30", "40"]];
  const roundtrip = rebuildOrdinalEditableTables(
    buildOrdinalEditableTableBlock(headers, rows, 1).join("\n"),
    [headers],
  );

  assert.equal(hasValidMarkdownTable(roundtrip.text), true);
  assert.ok(roundtrip.text.includes("| A | 10 | 20 |"));
  assert.ok(roundtrip.text.includes("| B | 30 | 40 |"));
  assert.equal(isTrustedTableVerification(trustedPage(
    { tables_markdown: roundtrip.text },
    withoutRoundtripText(roundtrip),
  )), true);
});

test("placeholder exacto solo es sintetico cuando el encabezado fuente era vacio", () => {
  assert.equal(isSyntheticColumnPlaceholder("Columna 1", 0, ""), true);
  assert.equal(isSyntheticColumnPlaceholder("Columna 2", 0, ""), false);
  assert.equal(isSyntheticColumnPlaceholder("Columna 1", 0, "Columna 1"), false);
});

test("cobertura A: has_tables sin table_regions no permite marcar verificada", () => {
  assert.equal(canMarkTableVerified(confirmed, true, validTable, {
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(),
  }), false);
  assert.equal(buildTableRegionCoverage(validTable, undefined, true).coverage_unknown, true);
});

test("cobertura B y C: possible_table o booleano legacy sin regiones no son trusted", () => {
  const withoutRegions = trustedPage({ table_regions: undefined });
  withoutRegions.possible_table = true;
  assert.equal(isTrustedTableVerification(withoutRegions), false);
  assert.equal(tableVerificationDiagnostics(withoutRegions).coverage_unknown, true);

  const legacy = {
    has_tables: true,
    tabla_verificada: true,
    revisado_manual: true,
    metadata: {},
  };
  assert.equal(isTrustedTableVerification(legacy), false);
  assert.equal(tableVerificationDiagnostics(legacy).coverage_unknown, true);

  const metadataOnly = {
    has_tables: false,
    possible_table: false,
    metadata: { possible_table: true },
  };
  assert.equal(tableVerificationDiagnostics(metadataOnly).coverage_unknown, true);
});

test("table_requires_review es veto absoluto y diagnostica contradiccion", () => {
  const contradictory = { ...trustedPage(), table_requires_review: true };
  assert.equal(isTrustedTableVerification(contradictory), false);
  assert.deepEqual(tableVerificationDiagnostics(contradictory), {
    coverage_unknown: false,
    explicit_review_pending: true,
    inconsistent_pending_and_verified: true,
  });
  assert.equal(canMarkTableVerified(confirmed, true, validTable, {
    regionSummary: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    sourcePdfSha256: sha256,
    ordinalRoundtrip: ordinalEvidence(),
    tableRequiresReview: true,
  }), false);
});

test("encabezados duplicados conservan señal semantica sin invalidar estructura", () => {
  const duplicateTable = "| TIPO | VALOR | VALOR |\n| --- | --- | --- |\n| A | 10 | 20 |";
  const inspection = inspectMarkdownTables(duplicateTable);
  assert.equal(inspection.valid, true);
  assert.equal(inspection.duplicate_header_ambiguity, true);
  assert.deepEqual(inspection.duplicate_headers, ["VALOR"]);
  assert.deepEqual(inspection.table_column_counts, [3]);
});

test("pipes y backslashes sobreviven tres roundtrips ordinales exactos", () => {
  const original = [
    ["A|B", "Ruta\\legal", "Texto \\| literal"],
    ["x|y", "C:\\normas\\2026", "\\|"],
    ["barra\\", "pipe|final", "\\\\|combinado"],
  ];
  let matrix = structuredClone(original);

  for (let round = 1; round <= 3; round++) {
    const rebuilt = rebuildOrdinalEditableTables(
      buildOrdinalEditableTableBlock(matrix[0], matrix.slice(1), 1).join("\n"),
      [matrix[0]],
    );
    const parsed = extractMarkdownTableMatrices(rebuilt.text);
    assert.deepEqual(parsed, [original], `roundtrip ${round}`);
    assert.equal(rebuilt.invalid_block_count, 0);
    matrix = parsed[0];
  }
});
