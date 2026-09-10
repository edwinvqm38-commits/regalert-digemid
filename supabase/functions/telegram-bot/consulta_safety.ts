import {
  inspectMarkdownTables,
  isTrustedTableVerification,
} from "./revision_safety.ts";

export type ConsultaEvidence = {
  document_key?: string | null;
  page_number?: number | null;
  detail_url?: string | null;
  has_tables?: boolean | null;
  possible_table?: boolean | null;
  table_requires_review?: boolean | null;
  table_quality_score?: number | null;
  tabla_verificada?: boolean | null;
  revisado_manual?: boolean | null;
  text_content?: string | null;
  tables_markdown?: string | null;
  metadata?: Record<string, unknown> | null;
};

export type ConsultaSafetyReason =
  | "unverified_table_structure"
  | "unseparated_pending_table_content"
  | "ambiguous_duplicate_header"
  | "invalid_ordinal_reference";

export type ConsultaSafetyDecision = {
  blocked: boolean;
  requires_structured_evidence: boolean;
  pending_evidence: ConsultaEvidence[];
  permitted_evidence: ConsultaEvidence[];
  reason: ConsultaSafetyReason | null;
};

export type ConsultaGateResult<T> =
  | { blocked: true; answer: string; evidence: ConsultaEvidence[]; value?: never }
  | { blocked: false; answer?: never; value: T; evidence: ConsultaEvidence[] };

function normalizar(texto: string): string {
  return (texto ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9%]+/g, " ")
    .trim();
}

export function hasUnverifiedTableStructure(evidence: ConsultaEvidence): boolean {
  const metadata = evidence.metadata && typeof evidence.metadata === "object"
    ? evidence.metadata as Record<string, any>
    : {};
  // Un pendiente explícito es veto absoluto, incluso si otros campos afirman
  // de forma contradictoria que la tabla fue verificada.
  if (evidence.table_requires_review === true || metadata.table_requires_review === true) {
    return true;
  }
  // OR conservador: un `false` moderno nunca borra una señal histórica true.
  const possibleTable = evidence.has_tables === true ||
    evidence.possible_table === true || metadata.possible_table === true;

  if (!possibleTable) return false;

  // Esta es la única autoridad. Ni tabla_verificada ni revisado_manual son
  // suficientes sin evidencia moderna, estructura válida y cobertura total.
  return !isTrustedTableVerification(evidence);
}

function trustedTableInspections(evidence: ConsultaEvidence[]) {
  return evidence.flatMap((item) => {
    if (!isTrustedTableVerification(item)) return [];
    const metadata = item.metadata && typeof item.metadata === "object"
      ? item.metadata as Record<string, any>
      : {};
    if (typeof metadata.tables_markdown !== "string") return [];
    const inspection = inspectMarkdownTables(metadata.tables_markdown);
    return inspection.valid ? [inspection] : [];
  });
}

function containsNormalizedPhrase(question: string, phrase: string): boolean {
  const normalizedPhrase = normalizar(phrase);
  return Boolean(normalizedPhrase) && ` ${question} `.includes(` ${normalizedPhrase} `);
}

function semanticTableBlockReason(
  question: string,
  evidence: ConsultaEvidence[],
): ConsultaSafetyReason | null {
  const inspections = trustedTableInspections(evidence);
  if (!inspections.length) return null;
  const normalizedQuestion = normalizar(question);
  const ordinalReferences = [...normalizedQuestion.matchAll(/\bc\s*(\d+)\b/g)]
    .map((match) => Number(match[1]));
  if (ordinalReferences.some((ordinal) =>
    ordinal < 1 || !inspections.some((inspection) =>
      inspection.table_column_counts.some((columns) => ordinal <= columns)
    )
  )) {
    return "invalid_ordinal_reference";
  }

  for (const inspection of inspections) {
    for (const duplicateHeader of inspection.duplicate_headers) {
      if (!containsNormalizedPhrase(normalizedQuestion, duplicateHeader)) continue;
      const normalizedHeader = normalizar(duplicateHeader);
      const matchingOrdinals = inspection.table_headers.flatMap((headers) =>
        headers.flatMap((header, index) =>
          normalizar(header) === normalizedHeader ? [index + 1] : []
        )
      );
      if (!ordinalReferences.some((ordinal) => matchingOrdinals.includes(ordinal))) {
        return "ambiguous_duplicate_header";
      }
    }
  }
  return null;
}

function tienePeticionRelacionalExplicita(pregunta: string): boolean {
  const texto = normalizar(pregunta);
  if (!texto) return true;

  const formasEstructurales = [
    /\b(tabla|fila|columna|celda|casilla|cuadro|matriz)\b/,
    /\b(corresponde|correspondencia|asocia|asociado|relaciona|relacionado|equivale)\b/,
    /\b(compara|comparacion|diferencia|mayor|menor)\b.*\b(entre|respecto|frente)\b/,
    /\b(valor|monto|porcentaje|tasa|nivel|categoria|condicion|consecuencia)\b.*\b(de|para|por|segun)\b/,
    /\b(sancion|multa|infraccion|obligacion|plazo)\b.*\b(de|para|por|segun|aplicable|recibe)\b/,
    /\b(cual|que|cuanto|cuanta|cuantos|cuantas)\b.*\b(corresponde|aplica|para|segun)\b/,
    /\b(a|para)\s+cada\b/,
  ];
  return formasEstructurales.some((patron) => patron.test(texto));
}

function esPeticionNarrativaInequivoca(pregunta: string): boolean {
  const texto = normalizar(pregunta);
  // Allowlist de intención COMPLETA. Los anclajes finales impiden que una
  // subpregunta relacional se disfrace detrás de un prefijo narrativo.
  const formasNarrativas = [
    /^(resume|resumeme|haz un resumen|de que trata)( (el|la|esta|este))? (articulo|parrafo|disposicion|norma|texto)( [a-z0-9]+)?$/,
    /^(transcribe|cita|copiame|muestra)( literalmente)? (el|la)? ?(texto|parrafo|articulo|disposicion|numeral)( [a-z0-9]+)?$/,
    /^que (dice|establece|dispone) (literalmente )?(el |la )?(articulo|parrafo|disposicion|numeral)( [a-z0-9]+)?$/,
    /^cual es (el )?(objeto|finalidad|alcance)( (de|del) (la|el|esta|este) (norma|articulo|disposicion))?$/,
    /^explica (el |la )?(articulo|parrafo|texto|objeto|finalidad|alcance|disposicion|numeral)( [a-z0-9]+)?$/,
  ];
  return formasNarrativas.some((patron) => patron.test(texto));
}

/**
 * Decide si la pregunta necesita interpretar una relacion estructurada.
 * Cuando hay evidencia tabular pendiente, solo se permite continuar si la
 * peticion es inequivocamente narrativa; toda ambiguedad falla cerrada.
 */
export function requiresStructuredEvidence(
  question: string,
  evidence: ConsultaEvidence[],
): boolean {
  if (!evidence.some(hasUnverifiedTableStructure)) return false;
  if (tienePeticionRelacionalExplicita(question)) return true;
  // Toda intención que no coincide íntegramente con la allowlist narrativa
  // queda bloqueada. No se intenta adivinar que el sufijo era inocuo.
  return !esPeticionNarrativaInequivoca(question);
}

export function evaluateConsultaSafety(
  question: string,
  evidence: ConsultaEvidence[],
): ConsultaSafetyDecision {
  const pending = evidence.filter(hasUnverifiedTableStructure);
  const requiresStructured = requiresStructuredEvidence(question, evidence);
  if (pending.length > 0 && requiresStructured) {
    return {
      blocked: true,
      requires_structured_evidence: true,
      pending_evidence: pending,
      permitted_evidence: [],
      reason: "unverified_table_structure",
    };
  }

  const semanticReason = semanticTableBlockReason(question, evidence);
  if (semanticReason) {
    return {
      blocked: true,
      requires_structured_evidence: true,
      pending_evidence: pending,
      permitted_evidence: [],
      reason: semanticReason,
    };
  }

  // En esta fase no existe safe_prose_text con trazabilidad de layout. Una
  // página pendiente se excluye COMPLETA; no se intenta adivinar qué líneas
  // son prosa. Si quedan otras páginas seguras, solo ellas llegan al modelo.
  const permitted = evidence.filter((item) => !hasUnverifiedTableStructure(item));
  if (pending.length > 0 && permitted.length === 0) {
    return {
      blocked: true,
      requires_structured_evidence: false,
      pending_evidence: pending,
      permitted_evidence: [],
      reason: "unseparated_pending_table_content",
    };
  }
  return {
    blocked: false,
    requires_structured_evidence: requiresStructured,
    pending_evidence: pending,
    permitted_evidence: permitted,
    reason: null,
  };
}

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

export function buildUnconfirmedStructuredAnswer(
  pendingEvidence: ConsultaEvidence[],
  reason: ConsultaSafetyReason = "unverified_table_structure",
): string {
  const seen = new Set<string>();
  const references: string[] = [];
  for (const evidence of pendingEvidence) {
    const key = evidence.document_key || "documento sin identificar";
    const page = evidence.page_number ?? "?";
    const identity = `${key}:${page}`;
    if (seen.has(identity)) continue;
    seen.add(identity);
    references.push(`<b>${escapeHtml(key)}</b>, pág. ${escapeHtml(page)}`);
  }

  const sourceLine = references.length
    ? `\n\n📌 Revisar en el PDF oficial: ${references.slice(0, 4).join("; ")}.`
    : "";
  const explanation = reason === "unseparated_pending_table_content"
    ? "La página relevante contiene una estructura tabular pendiente de revisión y RegAlert no puede separar todavía de forma verificable la prosa de las celdas de tabla."
    : reason === "ambiguous_duplicate_header"
    ? "La pregunta referencia un encabezado duplicado sin identificar su columna ordinal. Indica C2, C3 o una posición inequívoca para evitar asociar el valor equivocado."
    : reason === "invalid_ordinal_reference"
    ? "La pregunta referencia una identidad ordinal que no existe en la estructura tabular verificada."
    : "La información solicitada depende de una tabla cuya correspondencia fila-columna todavía no tiene verificación estructural humana. Por seguridad regulatoria no se generará esa relación automáticamente.";
  return `<b>NO CONFIRMADO AUTOMÁTICAMENTE</b>\n\n${explanation}${sourceLine}`;
}

/** Ejecuta el callback del modelo solo si la politica permite la consulta. */
export async function executeConsultaWithSafetyGate<T>(
  question: string,
  evidence: ConsultaEvidence[],
  invokeModel: (permittedEvidence: ConsultaEvidence[]) => Promise<T>,
): Promise<ConsultaGateResult<T>> {
  const decision = evaluateConsultaSafety(question, evidence);
  if (decision.blocked) {
    return {
      blocked: true,
      answer: buildUnconfirmedStructuredAnswer(decision.pending_evidence, decision.reason ?? undefined),
      evidence: decision.pending_evidence,
    };
  }
  return {
    blocked: false,
    value: await invokeModel(decision.permitted_evidence),
    evidence: decision.permitted_evidence,
  };
}
