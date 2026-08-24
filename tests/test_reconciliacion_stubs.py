"""Reglas de reconciliacion de stubs (H-08).

Se prueban contra scripts/reconciliar_stubs_normativos.py con datos calcados de
produccion. Ningun test toca la base: el script se ejecuta en modo DRY-RUN sobre
estructuras en memoria.
"""

import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts"))

import reconciliar_stubs_normativos as R  # noqa: E402

LEY_29459_REAL = {"id": "ley29459", "document_key": "LEY-29459", "tipo_norma": "Ley",
                  "numero": "29459", "anio": 2009, "titulo": "Ley de los productos farmacéuticos",
                  "estado_vigencia": "vigente", "process_status": "drive_structured"}
STUB_29459 = {"id": "stub29459", "document_key": "NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT",
              "tipo_norma": "LEY", "numero": "29459", "anio": None,
              "titulo": "Ley 29459, Ley de los productos farmacéuticos, dispositivos médicos",
              "estado_vigencia": "vigente", "process_status": "stub_derogada"}
LEY_29698_REAL = {"id": "ley29698", "document_key": "LEY-29698", "tipo_norma": "LEY",
                  "numero": "29698", "anio": None, "titulo": "Disponen la publicación del proyecto",
                  "estado_vigencia": "vigente", "process_status": "text_extracted"}
STUB_ART9 = {"id": "stubart9", "document_key": "LEY-29698-ART9", "tipo_norma": "LEY",
             "numero": "29698", "anio": None,
             "titulo": "artículo 9 de la Ley 29698 incorporado en la Ley 31738",
             "estado_vigencia": "derogada", "process_status": "stub_derogada"}
STUB_DS_004 = {"id": "stubds4", "document_key": "DS-004-2016", "tipo_norma": "DS", "numero": "004",
               "anio": 2016, "titulo": "Decreto Supremo N° 004-2016-SA",
               "estado_vigencia": "derogada", "process_status": "stub_derogada"}
DS_4_2023 = {"id": "ds42023", "document_key": "DS-4-2023", "tipo_norma": "DS", "numero": "4",
             "anio": 2023, "titulo": "DS 004-2023-SA", "estado_vigencia": "vigente",
             "process_status": "text_extracted"}


def rel(id_, origen, tipo, estado, afectada, tipo_n, numero, anio, arts=None, alcance=None, desc="x"):
    return {"id": id_, "norma_origen_id": origen, "norma_origen_document_key": "ORIGEN",
            "tipo_relacion": tipo, "estado": estado, "norma_afectada_id": afectada,
            "tipo_norma_afectada": tipo_n, "numero_afectada": numero, "anio_afectada": anio,
            "articulos_afectados": arts, "alcance": alcance, "fragmento_verificado": True,
            "descripcion_afectada": desc}


class TestClasificacion(unittest.TestCase):
    def _clasificar(self, stub, catalogo, relaciones=()):
        return R.clasificar(stub, list(relaciones), catalogo)

    def test_stub_con_norma_real_unica_es_reconciliable(self):
        """A · Ley 32319 -> Ley 29459: el stub duplica una norma real."""
        clasif, res, _ = self._clasificar(STUB_29459, [LEY_29459_REAL, STUB_29459])
        self.assertEqual(clasif, R.MATCH_EXACTO_UNICO)
        self.assertEqual(res.norma["document_key"], "LEY-29459")

    def test_para_una_ley_tipo_mas_numero_ya_es_identidad_completa(self):
        """Las leyes no reinician numeracion cada año: 29459 es unica."""
        _, res, _ = self._clasificar(STUB_29459, [LEY_29459_REAL, STUB_29459])
        self.assertEqual(res.nivel, R.NIVEL_TIPO_NUMERO)

    def test_un_ds_sin_anio_no_seria_identidad_completa(self):
        """Contraste: para DS el numero SI se repite cada año."""
        stub = dict(STUB_DS_004, anio=None, document_key="DS-4", titulo="Decreto Supremo N° 004")
        clasif, _, _ = self._clasificar(stub, [stub, DS_4_2023,
                                               dict(DS_4_2023, id="otro", document_key="DS-4-2021", anio=2021)])
        self.assertEqual(clasif, R.IDENTIDAD_AMBIGUA)

    def test_stub_que_es_un_articulo_no_se_fusiona(self):
        """C · LEY-29698-ART9 modela el articulo 9, no la ley."""
        clasif, res, motivo = self._clasificar(STUB_ART9, [LEY_29698_REAL, STUB_ART9])
        self.assertEqual(clasif, R.STUB_UNIDAD_PARCIAL)
        self.assertIn("LEY-29698", motivo)

    def test_relacion_parcial_no_convierte_al_stub_en_unidad(self):
        """La exoneracion de los arts. 10 y 11 es parcial, pero el stub sigue
        representando la Ley 29459 entera: identidad y alcance son cosas
        distintas."""
        relaciones = [rel("r1", "o1", "exonera", "confirmada", "stub29459",
                          "LEY", "29459", None, "10 y 11", "parcial")]
        clasif, _, _ = self._clasificar(STUB_29459, [LEY_29459_REAL, STUB_29459], relaciones)
        self.assertEqual(clasif, R.MATCH_EXACTO_UNICO)

    def test_sin_norma_real_el_stub_es_legitimo(self):
        """F · DS-004-2016 no existe en el corpus: el stub es la unica constancia."""
        clasif, _, _ = self._clasificar(STUB_DS_004, [STUB_DS_004, DS_4_2023])
        self.assertEqual(clasif, R.SIN_NORMA_REAL)

    def test_un_stub_nunca_se_reconcilia_contra_otro_stub(self):
        otro_stub = dict(STUB_29459, id="stub2", document_key="NORM-LEY-29459-BIS")
        clasif, _, _ = self._clasificar(STUB_29459, [STUB_29459, otro_stub])
        self.assertEqual(clasif, R.SIN_NORMA_REAL)

    def test_dos_normas_reales_candidatas_es_ambiguo(self):
        gemela = dict(LEY_29459_REAL, id="otra", document_key="LEY-29459-BIS", anio=2015)
        clasif, res, _ = self._clasificar(STUB_29459, [LEY_29459_REAL, gemela, STUB_29459])
        self.assertEqual(clasif, R.IDENTIDAD_AMBIGUA)
        self.assertIsNone(res.norma)


class TestVigencia(unittest.TestCase):
    """Nunca se traslada la vigencia del stub a la norma real."""

    def test_afectacion_parcial_no_toca_vigencia(self):
        accion, motivo = R.accion_sobre_vigencia(
            rel("r", "o", "deroga", "confirmada", None, "LEY", "29698", None, "9", "parcial"),
            STUB_ART9, LEY_29698_REAL,
        )
        self.assertEqual(accion, "no_tocar")
        self.assertIn("parcial", motivo)

    def test_exonera_no_altera_vigencia_aunque_sea_total(self):
        accion, _ = R.accion_sobre_vigencia(
            rel("r", "o", "exonera", "confirmada", None, "LEY", "29459", None),
            STUB_29459, LEY_29459_REAL,
        )
        self.assertEqual(accion, "no_tocar")

    def test_derogacion_total_sobre_norma_vigente_requiere_humano(self):
        accion, motivo = R.accion_sobre_vigencia(
            rel("r", "o", "deroga", "confirmada", None, "LEY", "29698", None, None, "total"),
            STUB_ART9, LEY_29698_REAL,
        )
        self.assertEqual(accion, "requiere_humano")
        self.assertIn("decision juridica", motivo)

    def test_nunca_propone_copiar_la_vigencia_del_stub(self):
        """El stub esta 'derogada' y la ley real 'vigente': jamas se copia."""
        _, filas = None, R.analizar([STUB_ART9, LEY_29698_REAL],
                                    [rel("r", "o", "deroga", "confirmada", "stubart9",
                                         "LEY", "29698", None, "9", "parcial")])[0]
        self.assertTrue(filas)
        for f in filas:
            self.assertNotEqual(f["accion_sobre_vigencia"], "copiar_del_stub")
            self.assertEqual(f["estado_vigencia_stub"], "derogada")
            self.assertEqual(f["estado_vigencia_real"], "vigente")


class TestHistorialYIdempotencia(unittest.TestCase):
    def test_una_relacion_rechazada_no_se_reconcilia(self):
        filas, _ = R.analizar(
            [STUB_29459, LEY_29459_REAL],
            [rel("r", "o", "exonera", "rechazada", "stub29459", "LEY", "29459", None)],
        )
        self.assertEqual(filas[0]["estado_relacion"], "rechazada")
        self.assertIsNone(filas[0]["nuevo_norma_afectada_id"], "el historial se preserva intacto")

    def test_la_propuesta_es_identica_al_repetirla(self):
        normas = [STUB_29459, LEY_29459_REAL]
        relaciones = [rel("r", "o", "exonera", "confirmada", "stub29459",
                          "LEY", "29459", None, "10 y 11", "parcial")]
        primera, _ = R.analizar(normas, relaciones)
        segunda, _ = R.analizar(normas, relaciones)
        self.assertEqual(primera, segunda)

    def test_una_relacion_ya_apuntada_a_la_norma_real_no_aparece(self):
        """Idempotencia: despues de reconciliar, no queda nada que hacer."""
        filas, _ = R.analizar(
            [STUB_29459, LEY_29459_REAL],
            [rel("r", "o", "exonera", "confirmada", "ley29459", "LEY", "29459", None)],
        )
        self.assertTrue(all(f["relacion_id"] is None for f in filas),
                        "el stub queda sin relaciones colgando")


class TestColisionesDedupe(unittest.TestCase):
    def test_dos_afectaciones_distintas_a_la_misma_norma_no_colisionan(self):
        """Caso real DS-15-2025: articulo 43 vs infraccion 30 del Anexo 01."""
        normas = [LEY_29459_REAL]
        relaciones = [
            rel("a", "ds15", "modifica", "confirmada", "ley29459", "LEY", "29459", None,
                None, None, "Reglamento aprobado por Decreto Supremo 014-2011-SA"),
            rel("b", "ds15", "modifica", "confirmada", "ley29459", "LEY", "29459", None,
                None, None, "Anexo 01 - Escala por Infracciones y Sanciones"),
        ]
        self.assertEqual(R.colisiones_dedupe([], normas, relaciones), [])

    def test_un_duplicado_real_si_se_detecta(self):
        normas = [LEY_29459_REAL]
        relaciones = [
            rel("a", "o1", "exonera", "confirmada", "ley29459", "LEY", "29459", None,
                "artículos 10 y 11", "parcial", "Ley 29459"),
            rel("b", "o1", "exonera", "pendiente", None, "LEY", "29459", None,
                "arts. 10 y 11", "parcial", "Ley N° 29459"),
        ]
        colisiones = R.colisiones_dedupe(
            [{"relacion_id": "b", "nuevo_norma_afectada_id": "ley29459"}], normas, relaciones
        )
        self.assertEqual(len(colisiones), 1)
        self.assertEqual(colisiones[0]["articulos_distintos"], "no")
        self.assertIn("duplicado real", colisiones[0]["veredicto"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
