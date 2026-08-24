"""Paridad Python ↔ TypeScript de la identidad normativa (H-08).

Hay dos motores porque el detector corre en Python (GitHub Actions) y el bot en
TypeScript (Edge Function de Deno). Para que NO sean dos fuentes de verdad:

  1. la tabla de tipos y las constantes viven en un solo archivo
     (config/identidad_normativa.spec.json);
  2. el .ts de esa spec se genera desde el JSON y aqui se verifica que este
     sincronizado;
  3. los MISMOS casos (tests/fixtures/identidad_casos.json) se ejecutan contra
     los dos motores y la salida debe ser identica caso por caso.

El lado Python de esos casos se verifica en tests/test_identidad_spec.py, que
no necesita runtime de JavaScript. Aqui se compara un motor contra el otro.

Si no hay runtime de JavaScript disponible, la paridad se salta con un mensaje
explicito -PARIDAD NO VERIFICADA-, nunca en silencio.
"""

import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts"))
sys.path.insert(0, str(RAIZ / "tests"))

from identidad_normativa import (  # noqa: E402
    clave_dedupe,
    construir_identidad,
    normalizar_tipo_norma,
    resolver_identidad,
)
from test_identidad_spec import CASOS, CATALOGO, es_stub  # noqa: E402


def salida_python() -> dict:
    resolucion = []
    for caso in CASOS["casos_resolucion"]:
        base = [f for f in CATALOGO if not es_stub(f)] if caso["excluir_stubs"] else CATALOGO
        r = resolver_identidad(construir_identidad(caso["tipo"], caso["numero"], caso["anio"]), base)
        resolucion.append({
            "nivel": r.nivel,
            "key": r.norma["document_key"] if r.norma else None,
            "confianza": r.confianza,
            "candidatas": sorted(c["document_key"] for c in r.candidatas),
        })

    return {
        "tipo": [normalizar_tipo_norma(c["entrada"]) for c in CASOS["casos_tipo"]],
        "identidad": [
            {"tipo": i.tipo, "numero": i.numero, "anio": i.anio, "sector": i.sector}
            for i in (
                construir_identidad(c["tipo"], c["numero"], c["anio"])
                for c in CASOS["casos_identidad"]
            )
        ],
        "resolucion": resolucion,
        "dedupe": [
            clave_dedupe(
                c["origen"], c["tipo_relacion"],
                construir_identidad(c["tipo"], c["numero"], c["anio"]),
                c["articulos"], c["descripcion"],
            )
            for c in CASOS["casos_dedupe"]
        ],
    }


def runtime_js() -> list[str] | None:
    runner = str(RAIZ / "tests" / "paridad_identidad_ts.ts")
    if shutil.which("deno"):
        return ["deno", "run", "--allow-read", runner]
    if shutil.which("node"):
        return ["node", "--experimental-strip-types", "--no-warnings", runner]
    return None


class TestSpecSincronizada(unittest.TestCase):
    def test_el_ts_generado_coincide_con_el_json(self):
        """Si alguien edita el .ts a mano, deja de haber una sola fuente."""
        r = subprocess.run(
            [sys.executable, str(RAIZ / "scripts" / "generar_spec_identidad_ts.py"), "--check"],
            capture_output=True, text=True,
        )
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


class TestParidadConTypeScript(unittest.TestCase):
    def test_los_dos_motores_dan_exactamente_lo_mismo(self):
        cmd = runtime_js()
        if not cmd:
            self.skipTest("PARIDAD NO VERIFICADA: no hay deno ni node en este entorno")
        proceso = subprocess.run(cmd, capture_output=True, text=True, cwd=str(RAIZ))
        self.assertEqual(proceso.returncode, 0, proceso.stderr)
        ts = json.loads(proceso.stdout)
        py = salida_python()

        for seccion in ("tipo", "identidad", "resolucion", "dedupe"):
            self.assertEqual(len(py[seccion]), len(ts[seccion]), seccion)
            for i, (a, b) in enumerate(zip(py[seccion], ts[seccion])):
                self.assertEqual(a, b, f"divergen en {seccion}[{i}]")


if __name__ == "__main__":
    unittest.main(verbosity=2)
