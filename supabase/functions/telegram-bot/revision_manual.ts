export type ManualReviewKind = "contenido" | "contenido_completo" | "tabla";

export function parseManualReviewKind(contenido: string): ManualReviewKind {
  const match = contenido.match(/^#\s*TIPO_REVISION:\s*(\S+)\s*$/mi);
  const value = match?.[1]?.trim().toLowerCase();
  if (value === "tabla") return "tabla";
  if (value === "contenido_completo") return "contenido_completo";
  // Compatibilidad con plantillas antiguas: eran revisión de contenido.
  return "contenido";
}

export function buildManualReviewPageUpdate(
  textoCorregido: string,
  kind: ManualReviewKind,
  nowIso: string,
): Record<string, unknown> {
  const update: Record<string, unknown> = {
    // text_raw es evidencia OCR original e inmutable.
    text_normalized: textoCorregido,
    quality_score: 1,
    extraction_method: "revision_manual",
    revisado_manual: true,
    revisado_en: nowIso,
    updated_at: nowIso,
  };

  if (kind === "tabla") {
    update.tabla_verificada = true;
    update.tabla_verificada_en = nowIso;
  }

  return update;
}

export function parseSourceAssetId(descriptor: string): number | null {
  const match = descriptor.match(/source_asset_id:\s*(\d+)/i);
  return match ? Number(match[1]) : null;
}
