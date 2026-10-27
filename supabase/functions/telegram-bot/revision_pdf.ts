import { PDFDocument } from "npm:pdf-lib@1.17.1";

export function normalizarPaginasPdf(pageNumbers: number[], totalPages: number): number[] {
  const unique = [...new Set(pageNumbers)]
    .filter((n) => Number.isInteger(n) && n >= 1 && n <= totalPages)
    .sort((a, b) => a - b);

  if (!unique.length) {
    throw new Error("No hay páginas PDF válidas para extraer.");
  }

  return unique;
}

export async function extraerPaginasPdf(
  pdfBytes: Uint8Array,
  pageNumbers: number[],
): Promise<Uint8Array> {
  const source = await PDFDocument.load(pdfBytes);
  const validPages = normalizarPaginasPdf(pageNumbers, source.getPageCount());
  const target = await PDFDocument.create();
  const copied = await target.copyPages(source, validPages.map((n) => n - 1));

  for (const page of copied) target.addPage(page);

  return await target.save();
}
