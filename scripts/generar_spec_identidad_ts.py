"""Genera el modulo TypeScript de la spec de identidad desde el JSON canonico.

El bot corre en Deno dentro de una Edge Function y no puede leer archivos del
repositorio en tiempo de ejecucion, asi que la spec se materializa como .ts
generado y VERSIONADO. tests/test_paridad_identidad.py falla si el .ts generado
no coincide con el JSON, de modo que sigue habiendo una sola fuente de verdad:
si alguien edita el .ts a mano, la suite lo detecta.

Uso:  python scripts/generar_spec_identidad_ts.py [--check]
"""

import argparse
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
SPEC = RAIZ / "config" / "identidad_normativa.spec.json"
DESTINO = RAIZ / "supabase" / "functions" / "telegram-bot" / "identidad_spec.generated.ts"

CABECERA = """// ARCHIVO GENERADO — NO EDITAR A MANO.
// Fuente: config/identidad_normativa.spec.json
// Regenerar: python scripts/generar_spec_identidad_ts.py
// La paridad con el motor Python la verifica tests/test_paridad_identidad.py.
"""


def render() -> str:
    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    tipos = "\n".join(
        f"  {json.dumps(k, ensure_ascii=False)}: {json.dumps(v, ensure_ascii=False)},"
        for k, v in spec["tipos_canonicos"].items()
    )
    return (
        f"{CABECERA}\n"
        f"export const SPEC_VERSION = {spec['version']};\n"
        f"export const ANIO_PIVOTE_DOS_DIGITOS = {spec['anio_pivote_dos_digitos']};\n"
        f"export const ANIO_MINIMO = {spec['anio_minimo']};\n"
        f"export const ANIO_MAXIMO = {spec['anio_maximo']};\n\n"
        f"export const TIPOS_CANONICOS: Record<string, string> = {{\n{tipos}\n}};\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Solo verifica que este sincronizado")
    args = parser.parse_args()

    contenido = render()
    actual = DESTINO.read_text(encoding="utf-8") if DESTINO.exists() else None

    if args.check:
        if actual != contenido:
            print("DESINCRONIZADO: regenera con python scripts/generar_spec_identidad_ts.py")
            return 1
        print("spec TypeScript sincronizada con el JSON canonico")
        return 0

    DESTINO.write_text(contenido, encoding="utf-8")
    print(f"Escrito {DESTINO.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
