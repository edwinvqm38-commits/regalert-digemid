import type { RevisionReason } from "./revision_queue.ts";

export type ManualReviewKind = "contenido" | "contenido_completo" | "tabla";

export function buildRevisionId(
  documentKey: string,
  pageNumber: number,
  reason: RevisionReason,
): string {
  const safeKey = documentKey.trim().toUpperCase().replace(/[^A-Z0-9-]+/g, "-");
  return `REV-${safeKey}-P${pageNumber}-${reason.toUpperCase()}`;
}

export function parseRevisionId(contenido: string): string | null {
  const match = contenido.match(/^#\s*REVISION_ID:\s*(\S+)\s*$/mi);
  return match?.[1]?.trim() || null;
}


export function parseManualReviewKind(contenido: string): ManualReviewKind {
  const match = contenido.match(/^#\s*TIPO_REVISION:\s*(\S+)\s*$/mi);
  const value = match?.[1]?.trim().toLowerCase();
  if (value === "tabla") return "tabla";
  if (value === "contenido_completo") return "contenido_completo";
  return "contenido";
}

export function parseManualReviewReason(contenido: string): RevisionReason | null {
  const match = contenido.match(/^#\s*MOTIVO_REVISION:\s*(\S+)\s*$/mi);
  const value = match?.[1]?.trim().toLowerCase();
  return ["texto", "tabla", "formula", "grafico", "anexo"].includes(value ?? "")
    ? value as RevisionReason
    : null;
}

export function parseSourceAssetId(descriptor: string): number | null {
  const match = descriptor.match(/source_asset_id:\s*(\d+)/i);
  return match ? Number(match[1]) : null;
}

export function countFormulaExpressions(texto: string): number {
  return (texto.match(/\b[A-Za-z]\s*=\s*[^\n]+/g) ?? []).length;
}

export function validateReviewedText(
  reason: RevisionReason | null,
  original: string,
  reviewed: string,
): string[] {
  const issues: string[] = [];
  const clean = reviewed.trim();

  if (!clean) issues.push("texto revisado vacío");
  if (clean.includes("�")) issues.push("contiene caracteres de reemplazo (�)");

  if (reason === "formula") {
    const originalCount = countFormulaExpressions(original);
    const reviewedCount = countFormulaExpressions(reviewed);
    const minimum = Math.max(1, originalCount);
    if (reviewedCount < minimum) {
      issues.push(`se esperaban al menos ${minimum} expresión(es) con '=' y se detectaron ${reviewedCount}`);
    }
  }

  return issues;
}
