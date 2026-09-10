const START_MARKER = "===== ESTRUCTURAS TABULARES SEPARADAS DE TEXT_RAW =====";
const END_MARKER = "===== FIN ESTRUCTURAS TABULARES =====";

export const TABLE_VERIFICATION_METHOD = "telegram_structural_manual_review_v1";
export const ORDINAL_COLUMN_IDENTITY_METHOD = "regalert_ordinal_columns_v1";

export type TableRegionSummary = {
  detected_regions?: number | null;
  accepted_regions?: number | null;
  unresolved_regions?: number | null;
};

export type ValidTableRegionSummary = {
  detected_regions: number;
  accepted_regions: number;
  unresolved_regions: number;
};

export type TableRegionCoverage = {
  detected_regions: number;
  verified_regions: number;
  unresolved_regions: number;
  coverage_unknown: boolean;
};

export type MarkdownTableInspection = {
  valid: boolean;
  table_count: number;
  errors: string[];
  duplicate_header_ambiguity: boolean;
  duplicate_headers: string[];
  table_headers: string[][];
  table_column_counts: number[];
};

export type TableVerificationOptions = {
  regionSummary?: TableRegionSummary | null;
  sourcePdfSha256?: unknown;
  ordinalRoundtrip?: OrdinalRoundtripEvidence | null;
  tableRequiresReview?: boolean | null;
};

export type OrdinalRoundtripEvidence = {
  column_identity: string;
  editable_table_count: number;
  invalid_block_count: number;
  cardinality_preserved: boolean;
  source_headers_matched: boolean;
  synthetic_headers_present: boolean;
  empty_headers_present: boolean;
  duplicate_header_ambiguity: boolean;
  duplicate_headers: string[];
};

export type EditableTableRoundtrip = OrdinalRoundtripEvidence & {
  text: string;
};

export type TableVerificationEvidence = {
  has_tables?: boolean | null;
  possible_table?: boolean | null;
  table_requires_review?: boolean | null;
  tabla_verificada?: boolean | null;
  revisado_manual?: boolean | null;
  metadata?: Record<string, unknown> | null;
};

export type TableVerificationDiagnostics = {
  coverage_unknown: boolean;
  explicit_review_pending: boolean;
  inconsistent_pending_and_verified: boolean;
};

export function splitReviewedPageText(text: string): {
  text: string;
  tablesMarkdown: string;
} {
  const start = text.indexOf(START_MARKER);
  if (start < 0) return { text: text.trim(), tablesMarkdown: "" };
  const end = text.indexOf(END_MARKER, start + START_MARKER.length);
  if (end < 0) return { text: text.trim(), tablesMarkdown: "" };

  const tablesMarkdown = text.slice(start + START_MARKER.length, end).trim();
  const sourceText = `${text.slice(0, start)}${text.slice(end + END_MARKER.length)}`
    .trim();
  return {
    text: sourceText,
    tablesMarkdown: tablesMarkdown.startsWith("[SIN ESTRUCTURA AUTOMÁTICA ACEPTADA")
      ? ""
      : tablesMarkdown,
  };
}

/**
 * Parsea la representación que genera renderMarkdownTableRow. Solo `\\` y
 * `\|` son escapes propios del formato; así un backslash real y un pipe real
 * sobreviven cualquier cantidad de roundtrips sin acumular escapes.
 */
export function parseMarkdownTableRow(line: string): string[] {
  const input = line.trim();
  const cells: string[] = [];
  let cell = "";

  for (let index = 0; index < input.length; index++) {
    const character = input[index];
    if (character === "\\" && index + 1 < input.length) {
      const escaped = input[index + 1];
      if (escaped === "\\" || escaped === "|") {
        cell += escaped;
        index++;
        continue;
      }
    }
    if (character === "|") {
      cells.push(cell.trim());
      cell = "";
      continue;
    }
    cell += character;
  }
  cells.push(cell.trim());
  if (cells[0] === "") cells.shift();
  if (cells[cells.length - 1] === "") cells.pop();
  return cells;
}

export function syntheticColumnPlaceholder(columnIndex: number): string {
  if (!Number.isInteger(columnIndex) || columnIndex < 0) {
    throw new RangeError("columnIndex debe ser un entero no negativo");
  }
  return `Columna ${columnIndex + 1}`;
}

/**
 * Reconoce solo el placeholder exacto generado para una posición cuyo
 * encabezado fuente era vacío. Un encabezado real llamado "Columna 1" no es
 * sintético porque sourceHeader conserva ese texto.
 */
export function isSyntheticColumnPlaceholder(
  value: string,
  columnIndex: number,
  sourceHeader: string,
): boolean {
  return sourceHeader === "" && value === syntheticColumnPlaceholder(columnIndex);
}

function displayColumnHeader(sourceHeader: string, columnIndex: number): string {
  return sourceHeader || syntheticColumnPlaceholder(columnIndex);
}

function ordinalColumnSchemaLine(sourceHeader: string, columnIndex: number): string {
  return `@@ REGALERT C${columnIndex + 1} SOURCE_HEADER_JSON: ${JSON.stringify(sourceHeader)}`;
}

function ordinalFieldPrefix(sourceHeader: string, columnIndex: number): string {
  return `C${columnIndex + 1} [${displayColumnHeader(sourceHeader, columnIndex)}]: `;
}

export function renderMarkdownTableRow(cells: string[]): string {
  const escaped = cells.map((cell) => cell.replaceAll("\\", "\\\\").replaceAll("|", "\\|"));
  return `| ${escaped.join(" | ")} |`;
}

/**
 * Construye el bloque editable que usa Telegram. C1..Cn son la identidad
 * técnica; el texto entre corchetes es solo display y nunca identifica una
 * columna. El esquema fuente queda serializado para detectar manipulación,
 * pero al reconstruir también se compara con la evidencia ya almacenada.
 */
export function buildOrdinalEditableTableBlock(
  sourceHeaders: string[],
  rows: string[][],
  tableNumber: number,
): string[] {
  if (!sourceHeaders.length || !Number.isInteger(tableNumber) || tableNumber < 1) {
    throw new Error("Esquema ordinal de tabla inválido");
  }
  if (rows.some((row) => row.length !== sourceHeaders.length)) {
    throw new Error("La cardinalidad de una fila no coincide con el esquema ordinal");
  }

  const lines = [
    `===== TABLA ${tableNumber} (edita solo valores; C1..Cn y encabezados son inmutables) =====`,
    ...sourceHeaders.map(ordinalColumnSchemaLine),
    "",
  ];
  rows.forEach((row, rowIndex) => {
    lines.push(`-- Fila ${rowIndex + 1} --`);
    row.forEach((value, columnIndex) => {
      lines.push(`${ordinalFieldPrefix(sourceHeaders[columnIndex], columnIndex)}${value}`);
    });
  });
  lines.push(`===== FIN TABLA ${tableNumber} =====`);
  return lines;
}

export function extractMarkdownTableHeaders(text: string): string[][] {
  return extractMarkdownTableMatrices(text).map((table) => table[0] ?? []);
}

/** Extrae matrices canónicas (sin la fila separadora) de cada bloque Markdown. */
export function extractMarkdownTableMatrices(text: string): string[][][] {
  const lines = (text ?? "").split("\n");
  const matrices: string[][][] = [];
  let index = 0;
  while (index < lines.length) {
    if (!lines[index].trim().startsWith("|")) {
      index++;
      continue;
    }
    const block: string[] = [];
    while (index < lines.length && lines[index].trim().startsWith("|")) {
      block.push(lines[index]);
      index++;
    }
    if (block.length >= 2) {
      const rows = block.map(parseMarkdownTableRow);
      matrices.push([rows[0], ...rows.slice(2)]);
    }
  }
  return matrices;
}

function normalizedHeaderIdentity(value: string): string {
  return value.trim().normalize("NFC").toLocaleLowerCase("es");
}

export function duplicateNonemptyHeaders(headers: string[][]): string[] {
  const duplicates = new Map<string, string>();
  for (const tableHeaders of headers) {
    const seen = new Map<string, string>();
    for (const header of tableHeaders) {
      const identity = normalizedHeaderIdentity(header);
      if (!identity) continue;
      if (seen.has(identity)) duplicates.set(identity, seen.get(identity) ?? header);
      else seen.set(identity, header);
    }
  }
  return [...duplicates.values()].sort((a, b) => a.localeCompare(b, "es"));
}

type ParsedOrdinalTable = {
  markdown: string[];
  syntheticHeadersPresent: boolean;
  emptyHeadersPresent: boolean;
};

function parseOrdinalEditableTableBlock(
  block: string[],
  expectedHeaders: string[],
  tableNumber: number,
): ParsedOrdinalTable | null {
  if (block.length < 5 || !expectedHeaders.length) return null;
  if (!new RegExp(`^=====\\s*TABLA\\s+${tableNumber}\\b.*=====\\s*$`).test(block[0].trim())) {
    return null;
  }
  if (block[block.length - 1].trim() !== `===== FIN TABLA ${tableNumber} =====`) {
    return null;
  }

  let cursor = 1;
  for (let columnIndex = 0; columnIndex < expectedHeaders.length; columnIndex++) {
    if (block[cursor] !== ordinalColumnSchemaLine(expectedHeaders[columnIndex], columnIndex)) {
      return null;
    }
    cursor++;
  }
  if (block[cursor] !== "") return null;
  cursor++;

  const rows: string[][] = [];
  let expectedRowNumber = 1;
  while (cursor < block.length - 1) {
    if (block[cursor].trim() !== `-- Fila ${expectedRowNumber} --`) return null;
    cursor++;
    const row: string[] = [];
    for (let columnIndex = 0; columnIndex < expectedHeaders.length; columnIndex++) {
      const prefix = ordinalFieldPrefix(expectedHeaders[columnIndex], columnIndex);
      const line = block[cursor];
      if (typeof line !== "string" || !line.startsWith(prefix)) return null;
      row.push(line.slice(prefix.length));
      cursor++;
    }
    rows.push(row);
    expectedRowNumber++;
  }
  if (!rows.length || cursor !== block.length - 1) return null;

  const syntheticHeadersPresent = expectedHeaders.some((header, columnIndex) =>
    isSyntheticColumnPlaceholder(
      displayColumnHeader(header, columnIndex),
      columnIndex,
      header,
    )
  );
  return {
    markdown: [
      renderMarkdownTableRow(expectedHeaders),
      renderMarkdownTableRow(expectedHeaders.map(() => "---")),
      ...rows.map(renderMarkdownTableRow),
    ],
    syntheticHeadersPresent,
    emptyHeadersPresent: expectedHeaders.some((header) => header === ""),
  };
}

/**
 * Reconstruye bloques ordinales usando como autoridad los encabezados del
 * Markdown almacenado antes de enviar la plantilla. Un bloque agregado,
 * renumerado o editado en sus etiquetas queda inválido y no puede verificar.
 */
export function rebuildOrdinalEditableTables(
  text: string,
  expectedTableHeaders: string[][],
): EditableTableRoundtrip {
  const lines = (text ?? "").split("\n");
  const result: string[] = [];
  const parsedNumbers = new Set<number>();
  let tableCount = 0;
  let invalidBlockCount = 0;
  let syntheticHeadersPresent = false;
  let emptyHeadersPresent = false;
  let index = 0;

  while (index < lines.length) {
    const preview = lines[index].trim().match(
      /^-----\s*VISTA PREVIA TABLA\s+(\d+)\b.*-----\s*$/,
    );
    if (preview) {
      const endPreview = new RegExp(
        `^-----\\s*FIN VISTA PREVIA TABLA\\s+${preview[1]}\\s*-----\\s*$`,
      );
      let end = index + 1;
      while (end < lines.length && !endPreview.test(lines[end].trim())) end++;
      if (end >= lines.length) {
        invalidBlockCount++;
        result.push(lines[index]);
        index++;
      } else {
        index = end + 1;
        if (index < lines.length && lines[index] === "") index++;
      }
      continue;
    }

    const start = lines[index].trim().match(/^=====\s*TABLA\s+(\d+)\b.*=====\s*$/);
    if (!start) {
      result.push(lines[index]);
      index++;
      continue;
    }

    const tableNumber = Number(start[1]);
    const endPattern = new RegExp(
      `^=====\\s*FIN TABLA\\s+${tableNumber}\\s*=====\\s*$`,
    );
    let end = index + 1;
    while (end < lines.length && !endPattern.test(lines[end].trim())) end++;
    if (end >= lines.length) {
      invalidBlockCount++;
      result.push(lines[index]);
      index++;
      continue;
    }

    const block = lines.slice(index, end + 1);
    const expectedHeaders = expectedTableHeaders[tableNumber - 1];
    const parsed = expectedHeaders &&
        tableNumber === tableCount + 1 &&
        !parsedNumbers.has(tableNumber)
      ? parseOrdinalEditableTableBlock(block, expectedHeaders, tableNumber)
      : null;
    if (!parsed) {
      invalidBlockCount++;
      result.push(...block);
    } else {
      result.push(...parsed.markdown);
      parsedNumbers.add(tableNumber);
      tableCount++;
      syntheticHeadersPresent ||= parsed.syntheticHeadersPresent;
      emptyHeadersPresent ||= parsed.emptyHeadersPresent;
    }
    index = end + 1;
  }

  const sourceHeadersMatched = invalidBlockCount === 0 &&
    tableCount === expectedTableHeaders.length;
  const duplicateHeaders = duplicateNonemptyHeaders(expectedTableHeaders);
  return {
    text: result.join("\n"),
    column_identity: ORDINAL_COLUMN_IDENTITY_METHOD,
    editable_table_count: tableCount,
    invalid_block_count: invalidBlockCount,
    cardinality_preserved: sourceHeadersMatched,
    source_headers_matched: sourceHeadersMatched,
    synthetic_headers_present: syntheticHeadersPresent,
    empty_headers_present: emptyHeadersPresent,
    duplicate_header_ambiguity: duplicateHeaders.length > 0,
    duplicate_headers: duplicateHeaders,
  };
}

function isMarkdownSeparator(cells: string[]): boolean {
  return cells.length >= 2 && cells.every((cell) => /^:?-{3,}:?$/.test(cell));
}

function validateMarkdownTableBlock(block: string[]): string[] {
  const errors: string[] = [];
  const rows = block.map(parseMarkdownTableRow);
  if (rows.length < 3) errors.push("table_requires_header_separator_and_data");

  const columns = rows[0]?.length ?? 0;
  if (columns < 2) errors.push("table_requires_two_columns");
  if (rows.some((row) => row.length !== columns)) {
    errors.push("inconsistent_column_count");
  }
  if (!rows[1] || !isMarkdownSeparator(rows[1]) || rows[1].length !== columns) {
    errors.push("invalid_markdown_separator");
  }
  if (rows.slice(2).some(isMarkdownSeparator)) {
    errors.push("unexpected_separator_row");
  }

  const header = rows[0] ?? [];
  if (header.filter(Boolean).length < 2) errors.push("insufficient_header_cells");
  if (header.some((cell) => cell === "")) errors.push("ambiguous_or_empty_header");

  const dataRows = rows.slice(2);
  if (!dataRows.length) errors.push("table_requires_data_row");
  if (!dataRows.some((row) => row.filter(Boolean).length >= 2)) {
    errors.push("insufficient_nonempty_data_cells");
  }
  const nonemptyCells = [header, ...dataRows]
    .reduce((total, row) => total + row.filter(Boolean).length, 0);
  if (nonemptyCells < 4) errors.push("decorative_or_empty_table");

  return [...new Set(errors)];
}

/**
 * Valida el documento tabular completo, no solo la presencia de un bloque que
 * parezca Markdown. Fuera de las tablas solo se permiten las etiquetas
 * editoriales "Tabla:" o "Tabla N:" que produce el extractor.
 */
export function inspectMarkdownTables(text: string): MarkdownTableInspection {
  const lines = (text ?? "").split("\n");
  const errors: string[] = [];
  const headers: string[][] = [];
  const tableColumnCounts: number[] = [];
  let tableCount = 0;
  let index = 0;

  while (index < lines.length) {
    const line = lines[index].trim();
    if (!line) {
      index++;
      continue;
    }
    if (!line.startsWith("|")) {
      if (!/^Tabla(?:\s+\d+)?:$/i.test(line)) errors.push("unexpected_non_table_content");
      index++;
      continue;
    }

    const block: string[] = [];
    while (index < lines.length && lines[index].trim().startsWith("|")) {
      block.push(lines[index]);
      index++;
    }
    tableCount++;
    const header = parseMarkdownTableRow(block[0] ?? "");
    headers.push(header);
    tableColumnCounts.push(header.length);
    errors.push(...validateMarkdownTableBlock(block));
  }

  if (tableCount === 0) errors.push("missing_markdown_table");
  const uniqueErrors = [...new Set(errors)];
  const duplicateHeaders = duplicateNonemptyHeaders(headers);
  return {
    valid: uniqueErrors.length === 0,
    table_count: tableCount,
    errors: uniqueErrors,
    duplicate_header_ambiguity: duplicateHeaders.length > 0,
    duplicate_headers: duplicateHeaders,
    table_headers: headers,
    table_column_counts: tableColumnCounts,
  };
}

export function hasValidMarkdownTable(text: string): boolean {
  return inspectMarkdownTables(text).valid;
}

export function isTableVerificationExplicit(content: string): boolean {
  return /^#\s*VERIFICACION_TABLAS:\s*CONFIRMADA\s*$/im.test(content);
}

export function isValidSha256(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/i.test(value);
}

export function isValidTableRegionSummary(
  regionSummary: TableRegionSummary | null | undefined,
): regionSummary is ValidTableRegionSummary {
  const detected = regionSummary?.detected_regions;
  const accepted = regionSummary?.accepted_regions;
  const unresolved = regionSummary?.unresolved_regions;
  return Number.isInteger(detected) && Number(detected) > 0 &&
    Number.isInteger(accepted) && Number(accepted) >= 0 &&
    Number.isInteger(unresolved) && Number(unresolved) >= 0 &&
    Number(accepted) <= Number(detected) &&
    Number(unresolved) === Number(detected) - Number(accepted);
}

export function buildTableRegionCoverage(
  tablesMarkdown: string,
  regionSummary: TableRegionSummary | null | undefined,
  pageHasTable: boolean,
): TableRegionCoverage {
  const inspection = inspectMarkdownTables(tablesMarkdown);
  if (!isValidTableRegionSummary(regionSummary)) {
    return {
      detected_regions: 0,
      verified_regions: 0,
      unresolved_regions: 0,
      coverage_unknown: pageHasTable,
    };
  }
  const detected = regionSummary.detected_regions;
  const verified = inspection.valid ? inspection.table_count : 0;
  return {
    detected_regions: detected,
    verified_regions: verified,
    unresolved_regions: Math.max(
      regionSummary.unresolved_regions,
      detected - verified,
      0,
    ),
    coverage_unknown: false,
  };
}

function sameStringArray(left: unknown, right: string[]): boolean {
  return Array.isArray(left) && left.length === right.length &&
    left.every((value, index) => value === right[index]);
}

export function canMarkTableVerified(
  templateContent: string,
  pageHasTable: boolean,
  tablesMarkdown: string,
  options: TableVerificationOptions = {},
): boolean {
  const inspection = inspectMarkdownTables(tablesMarkdown);
  const coverage = buildTableRegionCoverage(
    tablesMarkdown,
    options.regionSummary,
    pageHasTable,
  );
  const ordinal = options.ordinalRoundtrip;
  return pageHasTable &&
    options.tableRequiresReview !== true &&
    isTableVerificationExplicit(templateContent) &&
    isValidSha256(options.sourcePdfSha256) &&
    inspection.valid &&
    ordinal?.column_identity === ORDINAL_COLUMN_IDENTITY_METHOD &&
    ordinal.editable_table_count === inspection.table_count &&
    ordinal.invalid_block_count === 0 &&
    ordinal.cardinality_preserved === true &&
    ordinal.source_headers_matched === true &&
    ordinal.synthetic_headers_present === false &&
    ordinal.empty_headers_present === false &&
    ordinal.duplicate_header_ambiguity === inspection.duplicate_header_ambiguity &&
    sameStringArray(ordinal.duplicate_headers, inspection.duplicate_headers) &&
    coverage.coverage_unknown === false &&
    coverage.detected_regions > 0 &&
    coverage.verified_regions === coverage.detected_regions &&
    coverage.unresolved_regions === 0;
}

function isIsoTimestamp(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && !Number.isNaN(Date.parse(value));
}

/** Única autoridad para decidir si una tabla puede tratarse como verificada. */
export function isTrustedTableVerification(page: TableVerificationEvidence): boolean {
  const metadata = page.metadata && typeof page.metadata === "object"
    ? page.metadata as Record<string, any>
    : {};
  const review = metadata.manual_review;
  const verification = review?.table_verification;
  const sourcePdfSha256 = metadata.source_pdf_sha256;
  const tablesMarkdown = metadata.tables_markdown;
  const inspection: MarkdownTableInspection = typeof tablesMarkdown === "string"
    ? inspectMarkdownTables(tablesMarkdown)
    : {
      valid: false,
      table_count: 0,
      errors: ["missing_markdown_table"],
      duplicate_header_ambiguity: false,
      duplicate_headers: [],
      table_headers: [],
      table_column_counts: [],
    };
  const regionSummary = metadata.table_regions as TableRegionSummary | undefined;
  const regionSummaryValid = isValidTableRegionSummary(regionSummary);
  const expectedRegions = regionSummaryValid ? regionSummary.detected_regions : 0;
  const coverage = verification?.region_coverage;
  const ordinal = verification?.ordinal_columns;
  const explicitPending = page.table_requires_review === true ||
    metadata.table_requires_review === true;

  return !explicitPending &&
    regionSummaryValid &&
    regionSummary.accepted_regions === regionSummary.detected_regions &&
    regionSummary.unresolved_regions === 0 &&
    page.tabla_verificada === true &&
    page.revisado_manual === true &&
    review?.table_verified === true &&
    review?.table_confirmation_requested === true &&
    review?.table_structure_present === true &&
    verification?.confirmation_explicit === true &&
    verification?.verification_method === TABLE_VERIFICATION_METHOD &&
    isIsoTimestamp(verification?.verified_at) &&
    isValidSha256(sourcePdfSha256) &&
    verification?.source_pdf_sha256 === sourcePdfSha256 &&
    inspection.valid &&
    ordinal?.column_identity === ORDINAL_COLUMN_IDENTITY_METHOD &&
    ordinal?.editable_table_count === inspection.table_count &&
    ordinal?.invalid_block_count === 0 &&
    ordinal?.cardinality_preserved === true &&
    ordinal?.source_headers_matched === true &&
    ordinal?.synthetic_headers_present === false &&
    ordinal?.empty_headers_present === false &&
    ordinal?.duplicate_header_ambiguity === inspection.duplicate_header_ambiguity &&
    sameStringArray(ordinal?.duplicate_headers, inspection.duplicate_headers) &&
    expectedRegions > 0 &&
    coverage?.coverage_unknown === false &&
    coverage?.detected_regions === expectedRegions &&
    coverage?.verified_regions === inspection.table_count &&
    coverage?.verified_regions === expectedRegions &&
    coverage?.unresolved_regions === 0;
}

export function tableVerificationDiagnostics(
  page: TableVerificationEvidence,
): TableVerificationDiagnostics {
  const metadata = page.metadata && typeof page.metadata === "object"
    ? page.metadata as Record<string, any>
    : {};
  const explicitPending = page.table_requires_review === true ||
    metadata.table_requires_review === true;
  const tableSignals = page.has_tables === true || page.possible_table === true ||
    metadata.possible_table === true || explicitPending;
  const verificationClaimed = page.tabla_verificada === true ||
    metadata.manual_review?.table_verified === true;
  return {
    coverage_unknown: tableSignals &&
      !isValidTableRegionSummary(metadata.table_regions as TableRegionSummary | undefined),
    explicit_review_pending: explicitPending,
    inconsistent_pending_and_verified: explicitPending && verificationClaimed,
  };
}
