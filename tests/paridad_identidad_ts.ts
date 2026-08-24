// Ejecuta los casos compartidos contra el motor TypeScript y escribe el
// resultado como JSON en stdout. Lo invoca tests/test_paridad_identidad.py,
// que compara ese JSON con la salida del motor Python: si difieren en un solo
// caso, la suite falla.
import { readFileSync } from "node:fs";
import {
  claveDedupe,
  construirIdentidad,
  normalizarTipoNorma,
  resolverIdentidad,
  type FilaNorma,
} from "../supabase/functions/telegram-bot/identidad_normativa.ts";

const casos = JSON.parse(readFileSync(new URL("./fixtures/identidad_casos.json", import.meta.url), "utf-8"));
const catalogo: FilaNorma[] = casos.catalogo;
const esStub = (f: FilaNorma) =>
  String(f.process_status ?? "").startsWith("stub") || f.document_key.startsWith("NORM-");

const salida = {
  tipo: casos.casos_tipo.map((c: any) => normalizarTipoNorma(c.entrada)),
  identidad: casos.casos_identidad.map((c: any) => construirIdentidad(c.tipo, c.numero, c.anio)),
  resolucion: casos.casos_resolucion.map((c: any) => {
    const base = c.excluir_stubs ? catalogo.filter((f) => !esStub(f)) : catalogo;
    const r = resolverIdentidad(construirIdentidad(c.tipo, c.numero, c.anio), base);
    return {
      nivel: r.nivel,
      key: r.norma ? r.norma.document_key : null,
      confianza: r.confianza,
      candidatas: r.candidatas.map((f) => f.document_key).sort(),
    };
  }),
  dedupe: casos.casos_dedupe.map((c: any) =>
    claveDedupe(c.origen, c.tipo_relacion, construirIdentidad(c.tipo, c.numero, c.anio), c.articulos, c.descripcion)
  ),
};

console.log(JSON.stringify(salida, null, 2));
