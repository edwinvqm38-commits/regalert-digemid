import { strict as assert } from "node:assert";
import {
  buildRevisionId,
  countFormulaExpressions,
  parseManualReviewKind,
  parseManualReviewReason,
  parseRevisionId,
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


Deno.test("revision id is stable and parseable", () => {
  const id = buildRevisionId("RM-1753-2002", 48, "formula");
  assert.equal(id, "REV-RM-1753-2002-P48-FORMULA");
  assert.equal(parseRevisionId(`# REVISION_ID: ${id}\n# NORMA: RM-1753-2002`), id);
});
