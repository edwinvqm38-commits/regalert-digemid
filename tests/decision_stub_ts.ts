// Ejecuta los casos de decision de stub contra el motor TypeScript real del
// bot y escribe el resultado como JSON. Lo invoca tests/test_decision_stub.py.
import { readFileSync } from "node:fs";
import { decidirVinculoNorma } from "../supabase/functions/telegram-bot/decision_stub.ts";

const casos = JSON.parse(readFileSync(new URL("./fixtures/decision_stub_casos.json", import.meta.url), "utf-8"));

const salida = casos.casos.map((c: any) => {
  const d = decidirVinculoNorma(c.relacion, c.candidatas ?? []);
  return {
    nombre: c.nombre,
    accion: d.accion,
    normaId: d.normaId,
    documentKeyStub: d.documentKeyStub,
    estadoVigencia: d.estadoVigencia,
    bloqueadoPorAlcanceParcial: d.bloqueadoPorAlcanceParcial,
    candidatas: [...d.candidatas].sort(),
  };
});

console.log(JSON.stringify(salida, null, 2));
