import { strict as assert } from "node:assert";
import {
  buildManualReviewPageUpdate,
  parseManualReviewKind,
  parseSourceAssetId,
} from "./revision_manual.ts";

Deno.test("plantilla nueva distingue revisión de contenido y tabla", () => {
  assert.equal(parseManualReviewKind("# TIPO_REVISION: contenido\n"), "contenido");
  assert.equal(parseManualReviewKind("# TIPO_REVISION: contenido_completo\n"), "contenido_completo");
  assert.equal(parseManualReviewKind("# TIPO_REVISION: tabla\n"), "tabla");
});

Deno.test("plantilla legacy cae a contenido sin verificar tablas", () => {
  assert.equal(parseManualReviewKind("# NORMA: LEY-29459\n"), "contenido");
});

Deno.test("update manual nunca sobrescribe text_raw", () => {
  const update = buildManualReviewPageUpdate("Artículo 5.-", "contenido", "2026-10-26T00:00:00Z");
  assert.equal("text_raw" in update, false);
  assert.equal(update.text_normalized, "Artículo 5.-");
  assert.equal(update.revisado_manual, true);
  assert.equal("tabla_verificada" in update, false);
});

Deno.test("solo revisión de tabla marca tabla_verificada", () => {
  const update = buildManualReviewPageUpdate("| A | B |", "tabla", "2026-10-26T00:00:00Z");
  assert.equal(update.tabla_verificada, true);
  assert.equal(update.tabla_verificada_en, "2026-10-26T00:00:00Z");
  assert.equal("text_raw" in update, false);
});

Deno.test("source_asset_id se recupera del marcador de página", () => {
  assert.equal(parseSourceAssetId("calidad: 0.8, source_asset_id: 10"), 10);
  assert.equal(parseSourceAssetId("calidad: 0.8"), null);
});
