"""Tests de scripts/reconciliar_stubs_normativa.py (correccion de H-08:
stubs que duplican la identidad de una norma real ya existente).

Ejecutar:  python tests/test_reconciliar_stubs_normativa.py
           (o `pytest tests/`)
"""

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

RAIZ = Path(__file__).resolve().parents[1]
SCRIPT = RAIZ / "scripts" / "reconciliar_stubs_normativa.py"


def cargar_modulo():
    for nombre, attrs in {
        "dotenv": {"load_dotenv": lambda *a, **k: None},
        "supabase": {"create_client": lambda *a, **k: None},
    }.items():
        if nombre not in sys.modules:
            mod = types.ModuleType(nombre)
            for k, v in attrs.items():
                setattr(mod, k, v)
            sys.modules[nombre] = mod

    import importlib.util

    spec = importlib.util.spec_from_file_location("reconciliar_stubs_bajo_test", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


R = cargar_modulo()


class FakeResultado:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    """Encadena select/eq/limit/update y devuelve la respuesta programada
    para la tabla y filtro correspondientes."""

    def __init__(self, respuestas_normas, respuestas_relaciones):
        self._respuestas_normas = respuestas_normas
        self._respuestas_relaciones = respuestas_relaciones
        self._tabla = None
        self._filtro_eq = None
        self._update_pendiente = None
        self.updates = []

    def table(self, nombre):
        self._tabla = nombre
        return self

    def select(self, *_a, **_k):
        return self

    def eq(self, columna, valor):
        self._filtro_eq = (columna, valor)
        if self._update_pendiente is not None:
            self._update_pendiente["filtro"] = (columna, valor)
        return self

    def limit(self, _n):
        return self

    def update(self, payload):
        entrada = {"tabla": self._tabla, "filtro": None, "payload": payload}
        self.updates.append(entrada)
        self._update_pendiente = entrada
        return self

    def execute(self):
        self._update_pendiente = None
        if self._tabla == "digemid_normas" and self._filtro_eq and self._filtro_eq[0] == "document_key":
            document_key = self._filtro_eq[1]
            return FakeResultado(self._respuestas_normas.get(document_key, []))
        if self._tabla == "digemid_norma_relaciones" and self._filtro_eq and self._filtro_eq[0] == "norma_afectada_id":
            stub_id = self._filtro_eq[1]
            return FakeResultado(self._respuestas_relaciones.get(stub_id, []))
        # Actualizaciones (update().eq().execute()): no necesitan devolver data.
        return FakeResultado([])


class TestReconciliarPar(unittest.TestCase):
    def test_repunta_relaciones_y_marca_stub_cuando_apply(self):
        normas = {
            "STUB-1": [{"id": "id-stub", "document_key": "STUB-1", "process_status": "stub_derogada", "raw": {}}],
            "REAL-1": [{"id": "id-real", "document_key": "REAL-1", "process_status": "drive_structured", "raw": {}}],
        }
        relaciones = {"id-stub": [{"id": "rel-1", "norma_origen_document_key": "X", "tipo_relacion": "deroga", "descripcion_afectada": "..."}]}
        supabase = FakeQuery(normas, relaciones)

        R.reconciliar_par(supabase, "STUB-1", "REAL-1", apply_=True)

        updates_relacion = [u for u in supabase.updates if u["tabla"] == "digemid_norma_relaciones"]
        self.assertEqual(len(updates_relacion), 1)
        self.assertEqual(updates_relacion[0]["payload"], {"norma_afectada_id": "id-real"})
        self.assertEqual(updates_relacion[0]["filtro"], ("id", "rel-1"))

        updates_norma = [u for u in supabase.updates if u["tabla"] == "digemid_normas"]
        self.assertEqual(len(updates_norma), 1)
        self.assertEqual(updates_norma[0]["payload"]["process_status"], "stub_derogada_reconciliada")
        self.assertEqual(updates_norma[0]["payload"]["raw"]["reconciliado_con_document_key"], "REAL-1")

    def test_dry_run_no_escribe_nada(self):
        normas = {
            "STUB-1": [{"id": "id-stub", "document_key": "STUB-1", "process_status": "stub_derogada", "raw": {}}],
            "REAL-1": [{"id": "id-real", "document_key": "REAL-1", "process_status": "drive_structured", "raw": {}}],
        }
        relaciones = {"id-stub": [{"id": "rel-1", "norma_origen_document_key": "X", "tipo_relacion": "deroga", "descripcion_afectada": "..."}]}
        supabase = FakeQuery(normas, relaciones)

        R.reconciliar_par(supabase, "STUB-1", "REAL-1", apply_=False)

        self.assertEqual(supabase.updates, [])

    def test_no_toca_estado_vigencia_de_la_norma_real(self):
        """H-09: repuntar la relacion NUNCA debe copiar estado_vigencia del
        stub hacia la norma real."""
        normas = {
            "LEY-29698-ART9": [
                {"id": "id-stub", "document_key": "LEY-29698-ART9", "process_status": "stub_derogada", "raw": {}}
            ],
            "LEY-29698": [{"id": "id-real", "document_key": "LEY-29698", "process_status": "drive_structured", "raw": {}}],
        }
        relaciones = {"id-stub": [{"id": "rel-1", "norma_origen_document_key": "X", "tipo_relacion": "deroga", "descripcion_afectada": "..."}]}
        supabase = FakeQuery(normas, relaciones)

        R.reconciliar_par(supabase, "LEY-29698-ART9", "LEY-29698", apply_=True)

        updates_norma_real = [
            u for u in supabase.updates if u["tabla"] == "digemid_normas" and u["filtro"] == ("id", "id-real")
        ]
        self.assertEqual(updates_norma_real, [])

    def test_stub_inexistente_no_hace_nada(self):
        normas = {"REAL-1": [{"id": "id-real", "document_key": "REAL-1", "process_status": "drive_structured", "raw": {}}]}
        supabase = FakeQuery(normas, {})

        R.reconciliar_par(supabase, "STUB-FANTASMA", "REAL-1", apply_=True)

        self.assertEqual(supabase.updates, [])

    def test_norma_real_inexistente_no_hace_nada(self):
        normas = {"STUB-1": [{"id": "id-stub", "document_key": "STUB-1", "process_status": "stub_derogada", "raw": {}}]}
        supabase = FakeQuery(normas, {})

        R.reconciliar_par(supabase, "STUB-1", "REAL-FANTASMA", apply_=True)

        self.assertEqual(supabase.updates, [])

    def test_par_ya_reconciliado_no_duplica_sufijo(self):
        self.assertEqual(R._proximo_process_status("stub_derogada_reconciliada"), "stub_derogada_reconciliada")

    def test_process_status_sin_prefijo_stub_se_corrige(self):
        # Defensivo: si algun par documentado apuntara por error a una fila
        # que ya no es un stub, no se le quita la trazabilidad de que sigue
        # siendolo.
        self.assertEqual(R._proximo_process_status("drive_structured"), "stub_drive_structured_reconciliada")

    def test_process_status_none_usa_stub_por_defecto(self):
        self.assertEqual(R._proximo_process_status(None), "stub_reconciliada")


class TestParArgs(unittest.TestCase):
    def test_par_mal_formado_lanza_error(self):
        import argparse

        args = argparse.Namespace(pares=["STUB-SIN-DOS-PUNTOS"], apply=False)
        with self.assertRaises(ValueError):
            pares = []
            for par in args.pares:
                if ":" not in par:
                    raise ValueError(f"--par debe tener la forma STUB:REAL, recibido: {par!r}")
                pares.append(tuple(par.split(":", 1)))


if __name__ == "__main__":
    unittest.main()
