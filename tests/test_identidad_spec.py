"""Casos compartidos de identidad, ejecutados contra el motor Python.

Los mismos casos (tests/fixtures/identidad_casos.json) se ejecutan tambien
contra el motor TypeScript del bot en tests/test_paridad_identidad.py, que
llega con el PR de runtime. Aqui se verifica el LADO PYTHON: que los valores
esperados -que son la especificacion juridica, no un snapshot- se cumplen.

Separado a proposito del test de paridad: este archivo no depende de que exista
el motor TypeScript ni de que haya un runtime de JavaScript instalado, asi que
puede vivir en un PR que no toque supabase/functions/** (y por tanto no
dispare el despliegue automatico del bot).
"""

import json
import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts"))

from identidad_normativa import (  # noqa: E402
    clave_dedupe,
    construir_identidad,
    normalizar_tipo_norma,
    resolver_identidad,
)

CASOS = json.loads((RAIZ / "tests" / "fixtures" / "identidad_casos.json").read_text(encoding="utf-8"))
CATALOGO = CASOS["catalogo"]


def es_stub(fila: dict) -> bool:
    return (
        str(fila.get("process_status") or "").startswith("stub")
        or fila["document_key"].startswith("NORM-")
    )


def resoluciones() -> list[dict]:
    salida = []
    for caso in CASOS["casos_resolucion"]:
        base = [f for f in CATALOGO if not es_stub(f)] if caso["excluir_stubs"] else CATALOGO
        r = resolver_identidad(construir_identidad(caso["tipo"], caso["numero"], caso["anio"]), base)
        salida.append({
            "nivel": r.nivel,
            "key": r.norma["document_key"] if r.norma else None,
            "candidatas": sorted(c["document_key"] for c in r.candidatas),
        })
    return salida


def claves_dedupe() -> dict[str, str]:
    return {
        c["nombre"]: clave_dedupe(
            c["origen"], c["tipo_relacion"],
            construir_identidad(c["tipo"], c["numero"], c["anio"]),
            c["articulos"], c["descripcion"],
        )
        for c in CASOS["casos_dedupe"]
    }


class TestCasosCompartidos(unittest.TestCase):
    def test_tipos(self):
        for caso in CASOS["casos_tipo"]:
            self.assertEqual(normalizar_tipo_norma(caso["entrada"]), caso["esperado"], caso["entrada"])

    def test_identidades(self):
        for caso in CASOS["casos_identidad"]:
            ident = construir_identidad(caso["tipo"], caso["numero"], caso["anio"])
            self.assertEqual(
                {"tipo": ident.tipo, "numero": ident.numero, "anio": ident.anio, "sector": ident.sector},
                caso["esperado"], caso["nombre"],
            )

    def test_resoluciones(self):
        for caso, obtenido in zip(CASOS["casos_resolucion"], resoluciones()):
            self.assertEqual(obtenido["nivel"], caso["esperado_nivel"], caso["nombre"])
            self.assertEqual(obtenido["key"], caso["esperado_key"], caso["nombre"])
            self.assertEqual(obtenido["candidatas"], sorted(caso["esperado_candidatas"]), caso["nombre"])


class TestDeduplicacionCompartida(unittest.TestCase):
    def test_converge_y_separa(self):
        claves = list(claves_dedupe().values())
        self.assertEqual(claves[0], claves[1], "la misma relacion redactada distinto debe converger")
        self.assertNotEqual(claves[0], claves[2], "articulos distintos no se fusionan")

    def test_dos_afectaciones_distintas_sin_articulos_no_se_fusionan(self):
        """Casos REALES hallados en el DRY-RUN de H-08 (ambos en produccion).

        DS-15-2025 modifica el articulo 43 del Reglamento y, aparte, la
        infraccion 30 de su Anexo 01: misma norma afectada, mismo verbo,
        articulos_afectados NULL en las dos. Son relaciones juridicas DISTINTAS
        y no pueden colapsar en una sola clave.
        """
        k = claves_dedupe()
        self.assertNotEqual(
            k["DS-15-2025 · modifica el articulo 43 del Reglamento"],
            k["DS-15-2025 · modifica la infraccion 30 del Anexo 01 del MISMO Reglamento"],
        )
        self.assertNotEqual(
            k["RM-680-2021 · modifica el articulo 2 de la RM 1053-2020"],
            k["RM-680-2021 · modifica numerales del Manual aprobado por esa RM"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
