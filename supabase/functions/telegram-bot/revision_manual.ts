export type ManualReviewKind = "contenido" | "contenido_completo" | "tabla";

export function parseManualReviewKind(contenido: string): ManualReviewKind {
  const match = contenido.match(/^#\s*TIPO_REVISION:\s*(\S+)\s*$/mi);
  const value = match?.[1]?.trim().toLowerCase();
  if (value === "tabla") return "tabla";
  if (value === "contenido_completo") return "contenido_completo";
  // Compatibilidad con plantillas antiguas: eran revisión de contenido.
  return "contenido";
}

export function parseSourceAssetId(descriptor: string): number | null {
  const match = descriptor.match(/source_asset_id:\s*(\d+)/i);
  return match ? Number(match[1]) : null;
}
