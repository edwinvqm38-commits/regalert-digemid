export type ConsultaTheme =
  | "poes"
  | "almacenamiento"
  | "dispensacion"
  | "farmacovigilancia";

export type RetrievalCandidate = Record<string, unknown> & {
  document_key?: string;
  title?: string;
  text_content?: string;
  page_number?: number;
  published_date?: string | null;
};

export type ThematicRetrievalPlan = {
  theme: ConsultaTheme;
  queries: string[];
  candidateLimit: number;
  evidenceLimit: number;
};

const THEME_QUERIES: Record<ConsultaTheme, string[]> = {
  poes: [
    "POE",
    "POES",
    "procedimientos operativos estandarizados",
    "POES establecimiento farmacéutico botica farmacia",
  ],
  almacenamiento: [
    "almacenamiento",
    "almacenamiento productos farmacéuticos",
    "almacenamiento establecimiento farmacéutico botica farmacia",
  ],
  dispensacion: [
    "dispensación",
    "dispensación productos farmacéuticos",
    "dispensación establecimiento farmacéutico botica farmacia",
  ],
  farmacovigilancia: [
    "farmacovigilancia",
    "farmacovigilancia productos farmacéuticos",
    "farmacovigilancia establecimiento farmacéutico botica farmacia",
  ],
};

function plain(value: unknown): string {
  return String(value ?? "").toLowerCase().normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function candidateText(candidate: RetrievalCandidate): string {
  return plain(`${candidate.title ?? ""}\n${candidate.text_content ?? ""}`);
}

export function detectConsultaTheme(value: string): ConsultaTheme | null {
  const text = plain(value);
  if (
    /\bpoes?\b/.test(text) ||
    /\bprocedimientos?\s+operativos?\s+estandarizados?\b/.test(text)
  ) return "poes";
  if (/\bfarmacovigilancia\b/.test(text)) return "farmacovigilancia";
  if (/\bdispensacion\b/.test(text)) return "dispensacion";
  if (/\balmacenamiento\b/.test(text)) return "almacenamiento";
  return null;
}

export function buildThematicRetrievalPlan(
  question: string,
  interpretedQuery = "",
): ThematicRetrievalPlan | null {
  // La pregunta original tiene precedencia: una expansión del modelo no puede
  // cambiar POES por un subtipo (por ejemplo, saneamiento).
  const theme = detectConsultaTheme(question) ??
    detectConsultaTheme(interpretedQuery);
  if (!theme) return null;
  return {
    theme,
    queries: THEME_QUERIES[theme],
    // Este tope limita el conjunto intermedio. Solo evidenceLimit llega al LLM.
    candidateLimit: 8,
    evidenceLimit: 4,
  };
}

function hasThemeAnchor(theme: ConsultaTheme, text: string): boolean {
  if (theme === "poes") {
    return /\bpoes?\b/.test(text) ||
      /\bprocedimientos?\s+operativos?\s+estandarizados?\b/.test(text);
  }
  return new RegExp(`\\b${theme}\\b`).test(text);
}

function statusIndicatorFromPage(
  text: string,
): "project_language" | "approval_language" | "none" {
  if (
    /\b(proyecto(?:s)?|propuesta(?:s)?|prepublica(?:cion|do|da)|consulta publica)\b/
      .test(text)
  ) return "project_language";
  if (
    /\b(aprueba|aprobar|aprobado|aprobada|decreta|se resuelve)\b/.test(text)
  ) return "approval_language";
  return "none";
}

function relevanceScore(
  theme: ConsultaTheme,
  candidate: RetrievalCandidate,
): number {
  const text = candidateText(candidate);
  if (!hasThemeAnchor(theme, text)) return Number.NEGATIVE_INFINITY;

  let score = 20;
  if (theme === "poes") {
    if (/\bpoes?\b/.test(text)) score += 12;
    if (/\bprocedimientos?\s+operativos?\s+estandarizados?\b/.test(text)) {
      score += 16;
    }
  } else {
    const occurrences = text.match(new RegExp(`\\b${theme}\\b`, "g"))?.length ??
      0;
    score += Math.min(occurrences, 4) * 4;
  }

  if (
    /\b(boticas?|farmacias?|establecimientos? farmac[eé]uticos?)\b/.test(text)
  ) {
    score += 10;
  }
  if (
    /\b(productos? farmac[eé]uticos?|medicamentos?|director tecnico)\b/.test(
      text,
    )
  ) {
    score += 5;
  }
  if (/\b(digemid|minsa|autoridad sanitaria)\b/.test(text)) score += 2;
  if (statusIndicatorFromPage(text) === "project_language") score -= 1;
  return score;
}

/** Clasifica localmente resultados OR del RPC. Exige el término temático en
 * título/texto, prioriza el contexto farmacéutico y evita que un documento
 * monopolice las cuatro páginas enviadas al modelo. */
export function rankThematicEvidence(
  theme: ConsultaTheme,
  candidates: RetrievalCandidate[],
  limit = 4,
): RetrievalCandidate[] {
  const unique = new Map<string, RetrievalCandidate & { _score: number }>();
  for (const candidate of candidates) {
    const score = relevanceScore(theme, candidate);
    if (!Number.isFinite(score)) continue;
    const key = `${candidate.document_key ?? ""}:${
      candidate.page_number ?? ""
    }`;
    const previous = unique.get(key);
    if (!previous || score > previous._score) {
      unique.set(key, { ...candidate, _score: score });
    }
  }

  const ordered = [...unique.values()].sort((a, b) =>
    b._score - a._score ||
    String(a.document_key ?? "").localeCompare(String(b.document_key ?? "")) ||
    Number(a.page_number ?? 0) - Number(b.page_number ?? 0)
  );
  const perDocument = new Map<string, number>();
  const selected: RetrievalCandidate[] = [];
  for (const candidate of ordered) {
    const documentKey = String(candidate.document_key ?? "");
    const count = perDocument.get(documentKey) ?? 0;
    if (count >= 2) continue;
    const { _score: _discarded, ...evidence } = candidate;
    selected.push({
      ...evidence,
      thematic_relevance: candidate._score,
      document_status_indicator: statusIndicatorFromPage(
        candidateText(candidate),
      ),
    });
    perDocument.set(documentKey, count + 1);
    if (selected.length >= Math.max(1, Math.min(limit, 4))) break;
  }
  return selected;
}

export function thematicGuidance(theme: ConsultaTheme): string {
  const definition = theme === "poes"
    ? "POE significa Procedimiento Operativo Estandarizado y POES significa Procedimientos Operativos Estandarizados; no lo reduzcas a procedimientos de saneamiento."
    : `El tema recuperado es ${theme}.`;
  return `CRITERIOS DE COBERTURA TEMATICA:\n- ${definition}\n` +
    "- La recuperación cubre únicamente páginas ya extraídas de la biblioteca; no es un inventario normativo exhaustivo.\n" +
    "- No afirmes que una norma aplica a una botica solo por mencionar el tema: explica el alcance que conste en la evidencia.\n" +
    "- Las etiquetas de proyecto o aprobación son solo indicios textuales de una página; no confirman el estado del documento completo. Indica siempre estado documental por verificar.\n" +
    "- Usa solo la fecha oficial de publicación indicada. Si falta, di que no está confirmada.\n" +
    "- No califiques una disposición como nueva, reciente o la última si la evidencia recuperada no permite una comparación cronológica suficiente.";
}

export function formatEvidenceChronology(
  candidate: RetrievalCandidate,
): string | null {
  // Solo los resultados que pasaron por la recuperación temática reciben
  // estos indicios. Las alertas y búsquedas generales conservan su contexto.
  if (typeof candidate.thematic_relevance !== "number") return null;
  const date = typeof candidate.published_date === "string" &&
      /^\d{4}-\d{2}-\d{2}$/.test(candidate.published_date)
    ? candidate.published_date
    : "sin fecha oficial de publicación confirmada";
  const indicator = candidate.document_status_indicator === "project_language"
    ? "la página contiene lenguaje de proyecto/propuesta"
    : candidate.document_status_indicator === "approval_language"
    ? "la página contiene lenguaje de aprobación"
    : "la página no aporta un indicio textual suficiente";
  return `Fecha oficial: ${date}. Indicio textual: ${indicator}. Estado documental: por verificar.`;
}
