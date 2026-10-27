export type RevisionReason =
  | "texto"
  | "tabla"
  | "formula"
  | "grafico"
  | "anexo";

export type RevisionPage = {
  page_number: number;
  quality_score?: number | null;
  revisado_manual?: boolean | null;
  has_tables?: boolean | null;
  tabla_verificada?: boolean | null;
  posible_formula?: boolean | null;
  posible_grafico?: boolean | null;
  es_formulario_anexo?: boolean | null;
};

export const REVISION_LABELS: Record<RevisionReason, string> = {
  texto: "Texto/OCR",
  tabla: "Tabla",
  formula: "Fórmula",
  grafico: "Gráfico/diagrama",
  anexo: "Anexo/formulario",
};

export function revisionReasons(
  page: RevisionPage,
  lowQualityThreshold = 0.5,
): RevisionReason[] {
  const reasons: RevisionReason[] = [];
  const manual = page.revisado_manual === true;

  if (!manual && (page.quality_score == null || page.quality_score < lowQualityThreshold)) {
    reasons.push("texto");
  }
  if (page.has_tables === true && page.tabla_verificada !== true) {
    reasons.push("tabla");
  }
  if (!manual && page.posible_formula === true) reasons.push("formula");
  if (!manual && page.posible_grafico === true) reasons.push("grafico");
  if (!manual && page.es_formulario_anexo === true) reasons.push("anexo");

  return reasons;
}

export function summarizeRevisionPages(
  pages: RevisionPage[],
  lowQualityThreshold = 0.5,
): Record<RevisionReason, number> {
  const summary: Record<RevisionReason, number> = {
    texto: 0,
    tabla: 0,
    formula: 0,
    grafico: 0,
    anexo: 0,
  };

  for (const page of pages) {
    for (const reason of revisionReasons(page, lowQualityThreshold)) {
      summary[reason] += 1;
    }
  }

  return summary;
}

export function filterRevisionPages(
  pages: RevisionPage[],
  reason: RevisionReason,
  lowQualityThreshold = 0.5,
): RevisionPage[] {
  return pages.filter((page) =>
    revisionReasons(page, lowQualityThreshold).includes(reason)
  );
}

export function hasPendingRevision(
  page: RevisionPage,
  lowQualityThreshold = 0.5,
): boolean {
  return revisionReasons(page, lowQualityThreshold).length > 0;
}
