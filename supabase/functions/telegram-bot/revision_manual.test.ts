import { strict as assert } from "node:assert";
import {
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

Deno.test("source_asset_id se recupera del marcador de página", () => {
  assert.equal(parseSourceAssetId("calidad: 0.8, source_asset_id: 10"), 10);
  assert.equal(parseSourceAssetId("calidad: 0.8"), null);
});
