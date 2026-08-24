"""Reglas del bot al confirmar una relacion: cuando NACE un stub (H-08).

El bot es TypeScript, asi que los casos se ejecutan contra el modulo real
(supabase/functions/telegram-bot/decision_stub.ts) y aqui se comparan con lo
esperado. Los casos estan en tests/fixtures/decision_stub_casos.json, con
catalogos calcados de produccion.

Requisito que se esta verificando: un stub solo puede nacer si la norma esta
suficientemente identificada, NO existe una norma real equivalente, NO hay
ambiguedad y la cita tiene evidencia verificada.
"""

import json
import shutil
import subprocess
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CASOS = json.loads((RAIZ / "tests" / "fixtures" / "decision_stub_casos.json").read_text(encoding="utf-8"))


def runtime_js() -> list[str] | None:
    runner = str(RAIZ / "tests" / "decision_stub_ts.ts")
    if shutil.which("deno"):
        return ["deno", "run", "--allow-read", runner]
    if shutil.which("node"):
        return ["node", "--experimental-strip-types", "--no-warnings", runner]
    return None


class TestDecisionDeStub(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cmd = runtime_js()
        if not cmd:
            raise unittest.SkipTest("NO VERIFICADO: no hay deno ni node para ejecutar el modulo del bot")
        proceso = subprocess.run(cmd, capture_output=True, text=True, cwd=str(RAIZ))
        if proceso.returncode != 0:
            raise AssertionError(proceso.stderr)
        cls.resultados = {r["nombre"]: r for r in json.loads(proceso.stdout)}

    def _caso(self, nombre: str) -> dict:
        self.assertIn(nombre, self.resultados)
        return self.resultados[nombre]

    def test_todos_los_casos_del_fixture(self):
        for caso in CASOS["casos"]:
            obtenido = self._caso(caso["nombre"])
            esperado = dict(caso["esperado"])
            esperado["candidatas"] = sorted(esperado["candidatas"])
            for campo, valor in esperado.items():
                self.assertEqual(obtenido[campo], valor, f"{caso['nombre']} · campo {campo}")

    def test_ningun_caso_con_norma_real_crea_stub(self):
        for caso in CASOS["casos"]:
            candidatas_reales = [
                c for c in caso.get("candidatas", [])
                if not str(c.get("process_status") or "").startswith("stub")
                and not c["document_key"].startswith("NORM-")
            ]
            if candidatas_reales and caso["esperado"]["accion"] == "crear_stub":
                # Solo es legitimo si ninguna candidata real comparte identidad,
                # cosa que el propio motor ya decidio: aqui se exige que la
                # clave del stub no coincida con ninguna candidata real.
                claves = {c["document_key"] for c in candidatas_reales}
                self.assertNotIn(caso["esperado"]["documentKeyStub"], claves, caso["nombre"])

    def test_afectacion_parcial_nunca_escribe_vigencia(self):
        parciales = [
            c for c in CASOS["casos"]
            if c["relacion"].get("alcance") == "parcial" or c["relacion"].get("articulos_afectados")
        ]
        self.assertTrue(parciales, "el fixture debe cubrir afectaciones parciales")
        for caso in parciales:
            self.assertIsNone(
                self._caso(caso["nombre"])["estadoVigencia"],
                f"{caso['nombre']}: una afectacion parcial no puede cambiar la vigencia global",
            )

    def test_es_idempotente_al_confirmar_dos_veces(self):
        ya = self._caso("I · confirmacion repetida: ya vinculada, no se crea nada")
        self.assertEqual(ya["accion"], "ya_vinculada")
        self.assertIsNone(ya["documentKeyStub"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
