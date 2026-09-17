"""aplicar_filtro_pendientes decide que paginas entran como candidatas a la
verificacion por vision (scripts/ocr_normativa_openai_pages.py). El filtro
"graficos-pendientes" existe porque una pagina con posible_grafico=true puede
tener quality_score alto y sin tablas -- sin ese filtro nunca entraba como
candidata a pesar de que el heuristico existe justo para marcarla.
"""

import sys
import unittest
from argparse import Namespace
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from scripts.ocr_normativa_openai_pages import aplicar_filtro_pendientes  # noqa: E402


class FakeQuery:
    """Registra las llamadas encadenadas (.lt/.eq/.or_) en vez de pegarle a
    Supabase, para poder afirmar QUE filtro se armo sin red ni credenciales."""

    def __init__(self):
        self.llamadas: list[tuple[str, tuple]] = []

    def lt(self, *args):
        self.llamadas.append(("lt", args))
        return self

    def eq(self, *args):
        self.llamadas.append(("eq", args))
        return self

    def or_(self, *args):
        self.llamadas.append(("or_", args))
        return self


class TestAplicarFiltroPendientes(unittest.TestCase):
    def test_calidad_baja_filtra_por_quality_score(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="calidad-baja", quality_below=0.85))
        self.assertEqual(query.llamadas, [("lt", ("quality_score", 0.85))])

    def test_tablas_pendientes_filtra_por_tabla_no_verificada(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="tablas-pendientes", quality_below=0.85))
        self.assertEqual(
            query.llamadas,
            [("eq", ("has_tables", True)), ("eq", ("tabla_verificada", False))],
        )

    def test_graficos_pendientes_filtra_por_grafico_no_revisado(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="graficos-pendientes", quality_below=0.85))
        self.assertEqual(
            query.llamadas,
            [("eq", ("posible_grafico", True)), ("eq", ("revisado_manual", False))],
        )

    def test_todas_pendientes_incluye_las_tres_condiciones(self):
        query = FakeQuery()
        aplicar_filtro_pendientes(query, Namespace(filtro="todas-pendientes", quality_below=0.85))
        self.assertEqual(len(query.llamadas), 1)
        _, (clausula,) = query.llamadas[0]
        self.assertIn("quality_score.lt.0.85", clausula)
        self.assertIn("has_tables.eq.true,tabla_verificada.eq.false", clausula)
        self.assertIn("posible_grafico.eq.true,revisado_manual.eq.false", clausula)


if __name__ == "__main__":
    unittest.main()
