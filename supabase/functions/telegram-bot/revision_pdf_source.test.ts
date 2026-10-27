import { strict as assert } from "node:assert";
import { parseSupabaseStorageObjectUrl } from "./revision_pdf_source.ts";

Deno.test("extrae bucket y path de URL firmada aunque el token esté vencido", () => {
  const ref = parseSupabaseStorageObjectUrl(
    "https://abc.supabase.co/storage/v1/object/sign/digemid-documentos/normas/RM-1753-2002/RM-1753-2002.pdf?token=expired",
    "https://abc.supabase.co",
  );

  assert.deepEqual(ref, {
    bucket: "digemid-documentos",
    path: "normas/RM-1753-2002/RM-1753-2002.pdf",
  });
});

Deno.test("acepta URL pública del mismo Storage", () => {
  const ref = parseSupabaseStorageObjectUrl(
    "https://abc.supabase.co/storage/v1/object/public/docs/a%20b.pdf",
    "https://abc.supabase.co",
  );

  assert.deepEqual(ref, { bucket: "docs", path: "a b.pdf" });
});

Deno.test("no trata una URL externa como objeto propio", () => {
  assert.equal(
    parseSupabaseStorageObjectUrl(
      "https://www.digemid.minsa.gob.pe/documento.pdf",
      "https://abc.supabase.co",
    ),
    null,
  );
});
