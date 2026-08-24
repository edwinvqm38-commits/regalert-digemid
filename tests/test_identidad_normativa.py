"""Tests de la capa canonica de identidad normativa (H-05, H-06, H-07).

Incluye los casos de regresion A-F exigidos y las variantes de tipo_norma
REALMENTE presentes en produccion.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from identidad_normativa import (  # noqa: E402
    AMBIGUA,
    DATOS_INSUFICIENTES,
    NIVEL_EXACTA,
    NIVEL_NUMERO_ANIO,
    NIVEL_TIPO_NUMERO,
    NIVEL_TIPO_NUMERO_ANIO,
    NO_ENCONTRADA,
    clave_dedupe,
    construir_identidad,
    identidad_de_norma,
    normalizar_articulos,
    normalizar_numero,
    normalizar_tipo_norma,
    resolver_identidad,
)


def norma(id_, key, tipo, numero, anio):
    return {
        "id": id_, "document_key": key,
        "tipo_norma": tipo, "numero": numero, "anio": anio,
    }


# Catalogo con filas calcadas de produccion, incluyendo las variantes de
# tipo_norma en forma larga y los numeros con sufijo de sector.
CATALOGO = [
    norma("n1", "DS-014-2011-SA", "Decreto Supremo", "014-2011-SA", 2011),
    norma("n2", "DS-008-2025-SA", "Decreto Supremo", "008-2025-SA", 2025),
    norma("n3", "RM-554-2022-MINSA", "Resolución Ministerial", "554-2022-MINSA", 2022),
    norma("n4", "LEY-29459", "Ley", "29459", 2009),
    norma("n5", "RM-615-2024", "RM", "615", 2024),
    norma("n6", "RM-737-2010", "RM", "737", 2010),
    norma("n7", "RD-354-99-DG-DIGEMID-1999", "RD", "354-99-DG-DIGEMID", 1999),
    norma("n8", "DS-14-2002", "DS", "14", 2002),
]


class TestNormalizacionTipo(unittest.TestCase):
    """H-05 · TODAS las variantes reales halladas en produccion."""

    def test_variantes_reales_de_produccion(self):
        esperado = {
            "RM": "RM", "DS": "DS", "RD": "RD", "LEY": "LEY", "DU": "DU", "RS": "RS",
            "Decreto Supremo": "DS", "Resolución Ministerial": "RM", "Ley": "LEY",
        }
        for entrada, salida in esperado.items():
            self.assertEqual(normalizar_tipo_norma(entrada), salida, f"falla con {entrada!r}")

    def test_tolera_tildes_mayusculas_espacios_y_puntos(self):
        for v in ["resolucion ministerial", "RESOLUCIÓN  MINISTERIAL", " Resolucion Ministerial ",
                  "r.m.", "R.M.", "rm"]:
            self.assertEqual(normalizar_tipo_norma(v), "RM", f"falla con {v!r}")

    def test_no_confunde_tipos_juridicamente_distintos(self):
        self.assertNotEqual(normalizar_tipo_norma("Resolución Ministerial"),
                            normalizar_tipo_norma("Resolución Directoral"))
        self.assertNotEqual(normalizar_tipo_norma("Decreto Supremo"),
                            normalizar_tipo_norma("Decreto Legislativo"))
        self.assertEqual(normalizar_tipo_norma("Decreto Legislativo"), "DL")

    def test_desconocido_devuelve_none_sin_inventar(self):
        for v in [None, "", "   ", "Circular Interna", "XYZ"]:
            self.assertIsNone(normalizar_tipo_norma(v))


class TestParseoDeNumero(unittest.TestCase):
    def test_caso_f_ceros_a_la_izquierda(self):
        """Caso F: DS 014-2011 y DS 14-2011 son la misma identidad."""
        a = construir_identidad("DS", "014-2011-SA")
        b = construir_identidad("Decreto Supremo", "14", 2011)
        self.assertEqual(a.numero, b.numero)
        self.assertEqual(a.anio, b.anio)
        self.assertEqual(normalizar_numero("014"), normalizar_numero("14"))

    def test_extrae_anio_y_sector_embebidos(self):
        ident = construir_identidad("Decreto Supremo", "014-2011-SA")
        self.assertEqual((ident.tipo, ident.numero, ident.anio, ident.sector),
                         ("DS", "14", 2011, "SA"))

    def test_sector_compuesto_y_anio_de_dos_digitos(self):
        ident = construir_identidad("RD", "354-99-DG-DIGEMID")
        self.assertEqual(ident.numero, "354")
        self.assertEqual(ident.anio, 1999)
        self.assertEqual(ident.sector, "DG-DIGEMID")

    def test_numero_simple_sin_anio(self):
        ident = construir_identidad("LEY", "29459")
        self.assertEqual((ident.numero, ident.anio, ident.sector), ("29459", None, None))


class TestResolucionJerarquica(unittest.TestCase):
    def test_caso_a_ds_008_2025_resuelve_ds_014_2011(self):
        """Caso A."""
        r = resolver_identidad(construir_identidad("DS", "014", 2011, "SA"), CATALOGO)
        self.assertTrue(r.resuelta)
        self.assertEqual(r.norma["document_key"], "DS-014-2011-SA")
        self.assertEqual(r.nivel, NIVEL_EXACTA)

    def test_caso_b_ds_015_2025_resuelve_sin_sector(self):
        """Caso B: la cita no trae sector; debe resolver por tipo+numero+año."""
        r = resolver_identidad(construir_identidad("DS", "014", 2011), CATALOGO)
        self.assertTrue(r.resuelta)
        self.assertEqual(r.norma["document_key"], "DS-014-2011-SA")
        self.assertEqual(r.nivel, NIVEL_TIPO_NUMERO_ANIO)

    def test_caso_d_ley_29459_sin_anio_resuelve_la_ley_real(self):
        """Caso D: "Ley 29459" sin año debe encontrar la LEY-29459 real."""
        r = resolver_identidad(construir_identidad("LEY", "29459"), CATALOGO)
        self.assertTrue(r.resuelta, "no debe quedar sin resolver ni crear stub")
        self.assertEqual(r.norma["document_key"], "LEY-29459")
        self.assertEqual(r.nivel, NIVEL_TIPO_NUMERO)

    def test_caso_e_mismo_numero_y_anio_distinto_tipo_es_ambiguo(self):
        """Caso E: RM 150-2025 y RD 150-2025 coexisten; sin tipo -> AMBIGUA."""
        catalogo = CATALOGO + [
            norma("x1", "RM-150-2025", "RM", "150", 2025),
            norma("x2", "RD-150-2025", "RD", "150", 2025),
        ]
        r = resolver_identidad(construir_identidad(None, "150", 2025), catalogo)
        self.assertEqual(r.nivel, AMBIGUA)
        self.assertIsNone(r.norma, "nunca debe elegir la primera candidata")
        self.assertEqual(len(r.candidatas), 2)

    def test_caso_e_con_tipo_si_desambigua(self):
        catalogo = CATALOGO + [
            norma("x1", "RM-150-2025", "RM", "150", 2025),
            norma("x2", "RD-150-2025", "RD", "150", 2025),
        ]
        r = resolver_identidad(construir_identidad("RD", "150", 2025), catalogo)
        self.assertTrue(r.resuelta)
        self.assertEqual(r.norma["document_key"], "RD-150-2025")

    def test_numero_sin_tipo_pero_unico_resuelve(self):
        r = resolver_identidad(construir_identidad(None, "615", 2024), CATALOGO)
        self.assertTrue(r.resuelta)
        self.assertEqual(r.nivel, NIVEL_NUMERO_ANIO)

    def test_multiples_leyes_mismo_numero_es_ambiguo(self):
        catalogo = CATALOGO + [norma("z", "LEY-29459-BIS", "LEY", "29459", 2015)]
        r = resolver_identidad(construir_identidad("LEY", "29459"), catalogo)
        self.assertEqual(r.nivel, AMBIGUA)
        self.assertIsNone(r.norma)

    def test_inexistente_no_encontrada(self):
        r = resolver_identidad(construir_identidad("RM", "99999", 2030), CATALOGO)
        self.assertEqual(r.nivel, NO_ENCONTRADA)

    def test_sin_numero_datos_insuficientes(self):
        r = resolver_identidad(construir_identidad("RM", None, 2020), CATALOGO)
        self.assertEqual(r.nivel, DATOS_INSUFICIENTES)

    def test_identidad_de_norma_usa_campos_no_document_key(self):
        ident = identidad_de_norma(CATALOGO[0])
        self.assertEqual((ident.tipo, ident.numero, ident.anio), ("DS", "14", 2011))


class TestDeduplicacion(unittest.TestCase):
    """H-07 · idempotencia."""

    def _clave(self, articulos, tipo="modifica"):
        return clave_dedupe("origen-1", tipo, construir_identidad("LEY", "29459"), articulos)

    def test_misma_relacion_redactada_distinto_es_la_misma_clave(self):
        self.assertEqual(self._clave("artículos 10 y 11"), self._clave("arts. 10 y 11"))
        self.assertEqual(self._clave("artículos 10 y 11"), self._clave("10, 11"))
        self.assertEqual(self._clave("artículos 11 y 10"), self._clave("10 y 11"))

    def test_articulos_distintos_no_se_fusionan(self):
        self.assertNotEqual(self._clave("artículo 10"), self._clave("artículo 12"))

    def test_tipo_relacion_distinto_no_se_fusiona(self):
        self.assertNotEqual(self._clave("art. 5", "modifica"), self._clave("art. 5", "deroga"))

    def test_normas_afectadas_distintas_no_se_fusionan(self):
        a = clave_dedupe("o", "deroga", construir_identidad("RM", "339", 2023))
        b = clave_dedupe("o", "deroga", construir_identidad("RD", "339", 2023))
        self.assertNotEqual(a, b, "mismo numero+año pero distinto tipo: son normas distintas")

    def test_ceros_a_la_izquierda_no_duplican(self):
        a = clave_dedupe("o", "modifica", construir_identidad("DS", "014-2011-SA"))
        b = clave_dedupe("o", "modifica", construir_identidad("Decreto Supremo", "14", 2011, "SA"))
        self.assertEqual(a, b)

    def test_el_fragmento_no_participa_en_la_clave(self):
        """La evidencia no es identidad: cambiar la cita no crea un duplicado."""
        ident = construir_identidad("RM", "339", 2023)
        self.assertEqual(
            clave_dedupe("o", "deroga", ident, "art. 3", "Resolución Ministerial N° 339-2023"),
            clave_dedupe("o", "deroga", ident, "art. 3", "RM 339-2023/MINSA"),
        )

    def test_sin_identidad_cae_a_descripcion_normalizada(self):
        vacia = construir_identidad(None, None)
        a = clave_dedupe("o", "deroga", vacia, None, "Las disposiciones que se opongan")
        b = clave_dedupe("o", "deroga", vacia, None, "las  disposiciones que se opongan")
        self.assertEqual(a, b)

    def test_normalizar_articulos_conserva_numerales_compuestos(self):
        self.assertEqual(normalizar_articulos("sub numeral 5.1.4 del numeral 5.1"), "5.1,5.1.4")
        self.assertEqual(normalizar_articulos(None), "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
