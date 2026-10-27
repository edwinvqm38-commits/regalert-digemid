import { strict as assert } from "node:assert";
import { PDFDocument } from "npm:pdf-lib@1.17.1";
import { extraerPaginasPdf, normalizarPaginasPdf } from "./revision_pdf.ts";

Deno.test("normaliza páginas físicas, elimina duplicados y ordena", () => {
  assert.deepEqual(normalizarPaginasPdf([48, 2, 48, 3], 50), [2, 3, 48]);
});

Deno.test("rechaza selección sin páginas válidas", () => {
  assert.throws(() => normalizarPaginasPdf([0, -1, 99], 10));
});

Deno.test("extrae únicamente las páginas seleccionadas", async () => {
  const source = await PDFDocument.create();
  source.addPage([300, 400]);
  source.addPage([310, 410]);
  source.addPage([320, 420]);
  const sourceBytes = await source.save();

  const out = await extraerPaginasPdf(sourceBytes, [3, 1]);
  const result = await PDFDocument.load(out);

  assert.equal(result.getPageCount(), 2);
  assert.deepEqual(result.getPage(0).getSize(), { width: 300, height: 400 });
  assert.deepEqual(result.getPage(1).getSize(), { width: 320, height: 420 });
});
