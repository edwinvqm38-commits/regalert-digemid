import { strict as assert } from "node:assert";
import {
  countFormulaExpressions,
  parseManualReviewKind,
  parseManualReviewReason,
  parseSourceAssetId,
  validateReviewedText,
} from "./revision_manual.ts";

Deno.test("plantilla distingue tipo y motivo de revisión", () => {
  assert.equal(parseManualReviewKind("# TIPO_REVISION: contenido\n"), "contenido");
  assert.equal(parseManualReviewReason("# MOTIVO_REVISION: formula\n"), "formula");
  assert.equal(parseManualReviewReason("# NORMA: X\n"), null);
});

Deno.test("plantilla legacy cae a contenido", () => {
  assert.equal(parseManualReviewKind("# NORMA: LEY-29459\n"), "contenido");
});

Deno.test("source_asset_id se recupera del marcador", () => {
  assert.equal(parseSourceAssetId("calidad: 0.8, source_asset_id: 10"), 10);
  assert.equal(parseSourceAssetId("calidad: 0.8"), null);
});

Deno.test("valida que una revisión de fórmula conserve expresiones", () => {
  const original = "L=K x F\nN=M x F\nS=i+K";
  assert.equal(countFormulaExpressions(original), 3);
  assert.deepEqual(validateReviewedText("formula", original, "L = K x F\nN = M x F\nS = i + K"), []);
  assert.equal(validateReviewedText("formula", original, "L = K x F").length, 1);
});

Deno.test("rechaza caracteres de reemplazo", () => {
  assert.equal(validateReviewedText("texto", "abc", "ab�c").length, 1);
});
