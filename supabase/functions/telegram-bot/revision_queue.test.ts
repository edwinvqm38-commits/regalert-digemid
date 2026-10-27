import { strict as assert } from "node:assert";
import {
  filterRevisionPages,
  hasPendingRevision,
  revisionReasons,
  summarizeRevisionPages,
} from "./revision_queue.ts";

Deno.test("clasifica razones de revisión sin mezclar tabla con texto", () => {
  assert.deepEqual(
    revisionReasons({
      page_number: 2,
      quality_score: 0.3,
      revisado_manual: false,
      has_tables: true,
      tabla_verificada: false,
      posible_formula: true,
    }),
    ["texto", "tabla", "formula"],
  );
});

Deno.test("página manualmente revisada conserva pendiente de tabla si no fue verificada", () => {
  assert.deepEqual(
    revisionReasons({
      page_number: 4,
      quality_score: 1,
      revisado_manual: true,
      has_tables: true,
      tabla_verificada: false,
      posible_formula: true,
      posible_grafico: true,
    }),
    ["tabla"],
  );
});

Deno.test("resumen cuenta razones por página", () => {
  const summary = summarizeRevisionPages([
    { page_number: 1, quality_score: 0.2, revisado_manual: false },
    { page_number: 2, quality_score: 0.9, revisado_manual: false, has_tables: true, tabla_verificada: false },
    { page_number: 3, quality_score: 0.9, revisado_manual: false, posible_grafico: true },
  ]);
  assert.deepEqual(summary, { texto: 1, tabla: 1, formula: 0, grafico: 1, anexo: 0 });
});

Deno.test("filtra una cola por motivo", () => {
  const pages = [
    { page_number: 1, quality_score: 0.2, revisado_manual: false },
    { page_number: 2, quality_score: 0.9, revisado_manual: false, posible_formula: true },
  ];
  assert.deepEqual(filterRevisionPages(pages, "formula").map((p) => p.page_number), [2]);
  assert.equal(hasPendingRevision(pages[0]), true);
  assert.equal(hasPendingRevision({ page_number: 9, quality_score: 1, revisado_manual: true }), false);
});
