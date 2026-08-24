"""Tests de regresion del detector de relaciones normativas.

Cada test corresponde a un error REAL detectado en produccion. Se cargan las
funciones puras del script sin importar el modulo entero, para no exigir
credenciales de Supabase ni tocar la API de DeepSeek.

Ejecutar:  python tests/test_detector_relaciones.py
           (o `pytest tests/` si pytest esta disponible)
"""

import json
import re
import sys
import types
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
SCRIPT = RAIZ / "scripts" / "detectar_derogaciones_normativa.py"


def cargar_detector():
    """Importa el script con los modulos externos apagados (stubs), de modo que
    los tests corran sin red ni credenciales."""
    for nombre, attrs in {
        "dotenv": {"load_dotenv": lambda *a, **k: None},
        "supabase": {"create_client": lambda *a, **k: None},
    }.items():
        if nombre not in sys.modules:
            mod = types.ModuleType(nombre)
            for k, v in attrs.items():
                setattr(mod, k, v)
            sys.modules[nombre] = mod

    if "requests" not in sys.modules:
        requests = types.ModuleType("requests")
        exc = types.ModuleType("requests.exceptions")

        class RequestException(Exception):
            pass

        class Timeout(RequestException):
            pass

        exc.RequestException, exc.Timeout = RequestException, Timeout
        requests.exceptions = exc
        requests.post = lambda *a, **k: None
        sys.modules["requests"] = requests
        sys.modules["requests.exceptions"] = exc

    import importlib.util

    spec = importlib.util.spec_from_file_location("detector_bajo_test", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


D = cargar_detector()

sys.path.insert(0, str(RAIZ / "scripts"))
from identidad_normativa import AMBIGUA, NIVEL_TIPO_NUMERO  # noqa: E402


class RespuestaFalsa:
    """Doble de `requests.Response` para simular respuestas de DeepSeek."""

    def __init__(self, status_code=200, payload=None, texto=None, finish_reason="stop"):
        self.status_code = status_code
        if payload is not None:
            self._payload = payload
        else:
            self._payload = {
                "choices": [
                    {"message": {"content": texto or ""}, "finish_reason": finish_reason}
                ]
            }

    def json(self):
        return self._payload


class TestClausulaGenerica(unittest.TestCase):
    """DS-14-2002: 'Deroguese las disposiciones que se opongan' no identifica
    ninguna norma concreta y no puede generar una relacion."""

    def test_clausula_generica_se_descarta(self):
        self.assertTrue(
            D.es_clausula_generica(
                None, None, None, "las disposiciones que se opongan al presente Decreto Supremo"
            )
        )

    def test_norma_identificada_no_es_clausula_generica(self):
        self.assertFalse(
            D.es_clausula_generica("DS", "014", 2011, "Decreto Supremo N° 014-2011-SA")
        )


class TestLinajeVsObjeto(unittest.TestCase):
    """H-02 (regresion del PR #64) y caso RM-899-2025.

    'aprobado por X' identifica el instrumento que CONTIENE el objeto -es un
    destino legitimo-. Solo 'modificado por Y' denota una enmienda previa.
    """

    def test_aprobado_por_es_objeto_legitimo_ds_15_2025(self):
        frag = (
            "Modificar el artículo 43 del Reglamento de Establecimientos Farmacéuticos, "
            "aprobado por Decreto Supremo Nº 014-2011-SA"
        )
        self.assertFalse(D.es_cita_de_linaje(frag, "014", 2011))

    def test_aprobado_por_es_objeto_legitimo_ds_008_2025(self):
        frag = (
            "Modificar los artículos 23, 94, 110 del Reglamento que regula, "
            "aprobado por Decreto Supremo N° 014-2011-SA"
        )
        self.assertFalse(D.es_cita_de_linaje(frag, "014", 2011))

    def test_aprobada_por_rm_737_2010_es_objeto_legitimo(self):
        frag = (
            "Incorporar el sub numeral 5.1.4 de la Directiva N° 165, "
            "aprobada por Resolución Ministerial N° 737-2010/MINSA"
        )
        self.assertFalse(D.es_cita_de_linaje(frag, "737", 2010))

    def test_modificado_por_si_es_linaje_rm_899_2025(self):
        frag = (
            "Modificar el segundo párrafo del numeral 2 del Anexo 02 de la Directiva N° 165, "
            "aprobada por Resolución Ministerial N° 737-2010/MINSA, "
            "modificado por Resolución Ministerial N° 615-2024/MINSA"
        )
        self.assertTrue(D.es_cita_de_linaje(frag, "615", 2024))


class TestProyectosYContaminacion(unittest.TestCase):
    """RM-419-2025, RM-727-2025 y RM-883-2024."""

    def test_proyecto_anexado_se_recorta(self):
        texto = (
            "Artículo 1.- Disponer la publicación del proyecto.\n"
            "Regístrese y comuníquese.\n\nPROYECTO PARA PUBLICACIÓN\n"
            "Artículo 2.- Derogar la Resolución Directoral N° 006-2015-DIGEMID-DG-MINSA."
        )
        recortado = D.recortar_antes_de_proyecto_anexado(texto, "RM-419-2025")
        self.assertNotIn("Derogar", recortado)

    def test_decreto_anexado_en_resolucion_se_recorta(self):
        texto = (
            "SE RESUELVE:\nArtículo 1.- Disponer la publicación del proyecto.\n\n"
            "DECRETA:\nArtículo 1.- Modificar los artículos 3, 6, 10 del Reglamento."
        )
        recortado = D.recortar_decreto_anexado(texto, "RM", "RM-727-2025")
        self.assertIn("SE RESUELVE", recortado)
        self.assertNotIn("Modificar los artículos 3, 6, 10", recortado)

    def test_decreto_real_no_se_recorta_a_si_mismo(self):
        texto = "DECRETA:\nArtículo 1.- Modificar el artículo 195 del Reglamento."
        self.assertEqual(D.recortar_decreto_anexado(texto, "DS", "DS-059-2024-RE"), texto)

    def test_norma_ajena_previa_se_descarta_rm_883_2024(self):
        texto = (
            "DECRETA:\nArtículo 1.- El presente Decreto Supremo modifica el artículo 195 "
            "del Reglamento de la Ley N° 28091.\n2355977-2\nSALUD\n"
            "RESOLUCIÓN MINISTERIAL\nNº 883-2024/MINSA\nLima, 17 de diciembre del 2024\nVisto..."
        )
        recortado = D.recortar_antes_del_encabezado_propio(texto, "RM", "883", 2024, "RM-883-2024")
        self.assertTrue(recortado.lstrip().startswith("RESOLUCIÓN MINISTERIAL"))
        self.assertNotIn("28091", recortado)


class TestVentanaEstructural(unittest.TestCase):
    """H-01: la parte dispositiva va al final y no puede quedar amputada.

    Reproduce la forma real de RM-894-2024: considerandos largos y el articulo
    derogatorio pasado el caracter 15.000.
    """

    def _norma_larga(self):
        considerandos = "Que, " + ("x" * 60 + "\n") * 400  # ~24k chars
        dispositiva = (
            "SE RESUELVE:\n"
            "Artículo 1.- Modificar el rubro Autorización de Uso del Anexo.\n"
            "Artículo 2.- Aprobar la NTS N° 221-MINSA/DGIESP-2024.\n"
            "Artículo 3.- Derogar la Resolución Ministerial N° 339-2023/MINSA.\n"
        )
        return "RESOLUCIÓN MINISTERIAL\nNº 894-2024/MINSA\n" + considerandos + dispositiva

    def test_articulo_derogatorio_final_sobrevive(self):
        texto = self._norma_larga()
        self.assertGreater(len(texto), D.MAX_CHARS_TEXTO)
        # El truncado viejo lo perdia:
        self.assertNotIn("Derogar la Resolución Ministerial N° 339-2023", texto[: D.MAX_CHARS_TEXTO])
        # La segmentacion estructural lo conserva:
        seleccion = D.seleccionar_texto_relevante(texto, "RM-894-2024")
        self.assertIn("Derogar la Resolución Ministerial N° 339-2023", seleccion)
        self.assertLessEqual(len(seleccion), D.MAX_CHARS_TEXTO + 200)

    def test_sin_marcador_dispositivo_conserva_el_final(self):
        texto = "y" * 40000 + "\nArtículo 5.- Derogar la Resolución Ministerial N° 615-2024."
        seleccion = D.seleccionar_texto_relevante(texto, "SIN-MARCADOR")
        self.assertIn("Derogar la Resolución Ministerial N° 615-2024", seleccion)

    def test_texto_corto_pasa_intacto(self):
        texto = "SE RESUELVE:\nArtículo 1.- Derogar la RM N° 1-2020."
        self.assertEqual(D.seleccionar_texto_relevante(texto, "CORTA"), texto)


class TestEstadosDeepSeek(unittest.TestCase):
    """H-03: un error del modelo nunca puede equivaler a 'cero relaciones'."""

    def _con_respuesta(self, respuesta, veces=10):
        llamadas = {"n": 0}

        def post(*_a, **_k):
            llamadas["n"] += 1
            if isinstance(respuesta, Exception):
                raise respuesta
            return respuesta

        original, D.requests.post = D.requests.post, post
        original_sleep, D.time.sleep = D.time.sleep, lambda *_: None
        try:
            return D.call_deepseek("k", "x" * 500), llamadas["n"]
        finally:
            D.requests.post, D.time.sleep = original, original_sleep

    def test_json_invalido_no_es_cero_relaciones(self):
        (estado, _), _ = self._con_respuesta(RespuestaFalsa(texto="esto no es json"))
        self.assertEqual(estado, D.ESTADO_ERROR_JSON)
        self.assertNotEqual(estado, D.ESTADO_OK)

    def test_timeout_no_es_cero_relaciones(self):
        (estado, _), intentos = self._con_respuesta(D.requests.exceptions.Timeout())
        self.assertEqual(estado, D.ESTADO_TIMEOUT)
        self.assertGreater(intentos, 1, "un timeout debe reintentarse")

    def test_http_500_es_error_api(self):
        (estado, _), _ = self._con_respuesta(RespuestaFalsa(status_code=500))
        self.assertEqual(estado, D.ESTADO_ERROR_API)

    def test_respuesta_truncada_por_tokens_no_es_ok(self):
        (estado, _), _ = self._con_respuesta(
            RespuestaFalsa(texto='{"relaciones": [{"tipo_rela', finish_reason="length")
        )
        self.assertEqual(estado, D.ESTADO_RESPUESTA_INCOMPLETA)

    def test_texto_insuficiente_no_llama_al_modelo(self):
        estado, _ = D.call_deepseek("k", "corto")
        self.assertEqual(estado, D.ESTADO_TEXTO_INSUFICIENTE)

    def test_respuesta_valida_es_ok(self):
        payload = json.dumps({"relaciones": [{"tipo_relacion": "deroga"}]})
        (estado, data), _ = self._con_respuesta(RespuestaFalsa(texto=payload))
        self.assertEqual(estado, D.ESTADO_OK)
        self.assertEqual(len(data["relaciones"]), 1)

    def test_cero_relaciones_legitimo_sigue_siendo_ok(self):
        (estado, data), _ = self._con_respuesta(RespuestaFalsa(texto='{"relaciones": []}'))
        self.assertEqual(estado, D.ESTADO_OK)
        self.assertEqual(data["relaciones"], [])


class TestIdentidadYNumeros(unittest.TestCase):
    def test_numero_sucio_se_normaliza(self):
        self.assertEqual(D.normalizar_numero("014-2011-SA"), "14")
        self.assertEqual(D.normalizar_numero("014"), "14")
        self.assertEqual(D.normalizar_numero("14"), "14")

    def test_identidad_citada_usa_numero_normalizado(self):
        """Sustituye a la construccion de document_key: la identidad ya no se
        deduce de una cadena armada a mano, sino de la capa canonica."""
        ident = D.construir_identidad("DS", "014", 2011)
        self.assertEqual((ident.tipo, ident.numero, ident.anio), ("DS", "14", 2011))

    def test_identidad_para_dedupe_prefiere_la_norma_real(self):
        citada = D.construir_identidad("LEY", "29459")
        resuelta = D.resolver_identidad(
            citada,
            [{"id": "n", "document_key": "LEY-29459", "tipo_norma": "Ley",
              "numero": "29459", "anio": 2009}],
        )
        self.assertTrue(resuelta.resuelta)
        self.assertEqual(D.identidad_para_dedupe(resuelta, citada).anio, 2009)

    def test_identidad_para_dedupe_cae_a_la_cita_si_no_resuelve(self):
        citada = D.construir_identidad("RM", "99999", 2030)
        sin_resolver = D.resolver_identidad(citada, [])
        self.assertIs(D.identidad_para_dedupe(sin_resolver, citada), citada)


class TestVersionadoAnalizador(unittest.TestCase):
    """H-04: al subir la version, las normas viejas vuelven a la cola."""

    def test_version_es_entera_y_positiva(self):
        self.assertIsInstance(D.ANALYZER_VERSION, int)
        self.assertGreaterEqual(D.ANALYZER_VERSION, 2)

    def test_norma_de_version_anterior_se_reencola(self):
        filas = [
            {"id": "1", "document_key": "A", "derogacion_analizada": True,
             "relaciones_analyzer_version": D.ANALYZER_VERSION - 1},
            {"id": "2", "document_key": "B", "derogacion_analizada": True,
             "relaciones_analyzer_version": D.ANALYZER_VERSION},
            {"id": "3", "document_key": "C", "derogacion_analizada": False,
             "relaciones_analyzer_version": None},
        ]
        claves = {
            f["document_key"]
            for f in filas
            if not f.get("derogacion_analizada")
            or (f.get("relaciones_analyzer_version") or 0) < D.ANALYZER_VERSION
        }
        self.assertEqual(claves, {"A", "C"})


# ---------------------------------------------------------------------------
# H-07 · Idempotencia real de procesar_norma (reanalisis con --force)
# ---------------------------------------------------------------------------
class SupabaseFalso:
    """Doble de Supabase en memoria. Solo soporta las operaciones que usa el
    detector; cualquier otra explota a proposito, para que el test no pase por
    accidente sobre una consulta que no se esta emulando."""

    def __init__(self, paginas, relaciones=None):
        self.paginas = paginas
        self.relaciones = list(relaciones or [])
        self.updates = []

    def table(self, nombre):
        return _ConsultaFalsa(self, nombre)


class _Respuesta:
    def __init__(self, data):
        self.data = data


class _ConsultaFalsa:
    def __init__(self, db, tabla):
        self.db, self.tabla = db, tabla
        self._filtros, self._insert, self._update = {}, None, None

    def select(self, *_a, **_k):
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def eq(self, campo, valor):
        self._filtros[campo] = valor
        return self

    def insert(self, fila):
        self._insert = fila
        return self

    def update(self, fila):
        self._update = fila
        return self

    def execute(self):
        if self._insert is not None:
            fila = dict(self._insert)
            fila["id"] = f"rel-{len(self.db.relaciones) + 1}"
            # Emula el indice unico parcial de la migracion.
            clave = fila.get("clave_dedupe")
            if clave and any(r.get("clave_dedupe") == clave for r in self.db.relaciones):
                raise AssertionError(f"UNIQUE violado en clave_dedupe: {clave}")
            self.db.relaciones.append(fila)
            return _Respuesta([fila])
        if self._update is not None:
            self.db.updates.append((self.tabla, self._update, dict(self._filtros)))
            return _Respuesta([])
        if self.tabla == "digemid_norma_paginas":
            return _Respuesta(self.db.paginas)
        if self.tabla == "digemid_norma_relaciones":
            origen = self._filtros.get("norma_origen_id")
            return _Respuesta(
                [r for r in self.db.relaciones if r.get("norma_origen_id") == origen]
            )
        raise AssertionError(f"consulta no emulada sobre {self.tabla}")


TEXTO_NORMA = (
    "RESOLUCION MINISTERIAL N° 500-2025/MINSA\n\n"
    "SE RESUELVE:\n\n"
    "Articulo 1.- Derogar el articulo 10 de la Ley N° 29459.\n"
)

CATALOGO_FALSO = [
    {"id": "ley-real", "document_key": "LEY-29459", "tipo_norma": "Ley",
     "numero": "29459", "anio": 2009},
]


class TestIdempotenciaConForce(unittest.TestCase):
    """La misma norma reanalizada N veces produce las MISMAS relaciones."""

    def _respuesta_ia(self, redaccion):
        """Dos corridas del modelo que describen LA MISMA relacion juridica con
        redaccion distinta: es exactamente lo que producia duplicados (H-07)."""
        return {
            "relaciones": [
                {
                    "tipo_relacion": "deroga",
                    "tipo_norma": "LEY",
                    "numero": "29459",
                    "anio": None,
                    "articulos_afectados": redaccion["articulos"],
                    "alcance": "parcial",
                    "descripcion": redaccion["descripcion"],
                    "fragmento": redaccion["fragmento"],
                }
            ]
        }

    def _correr(self, db, redaccion):
        original = D.call_deepseek
        D.call_deepseek = lambda *a, **k: (D.ESTADO_OK, self._respuesta_ia(redaccion))
        try:
            return D.procesar_norma(
                db,
                {"id": "origen-1", "document_key": "RM-500-2025",
                 "tipo_norma": "RM", "numero": "500", "anio": 2025},
                "clave-falsa",
                CATALOGO_FALSO,
            )
        finally:
            D.call_deepseek = original

    def setUp(self):
        self.db = SupabaseFalso(
            paginas=[{"page_number": 1, "text_normalized": TEXTO_NORMA, "text_raw": None}]
        )

    def test_tres_corridas_producen_una_sola_relacion(self):
        redacciones = [
            {"articulos": "articulo 10", "descripcion": "Deroga el articulo 10 de la Ley 29459",
             "fragmento": "Derogar el articulo 10 de la Ley N° 29459."},
            {"articulos": "art. 10", "descripcion": "Derogacion del art. 10 de la Ley N° 29459",
             "fragmento": "Articulo 1.- Derogar el articulo 10 de la Ley N° 29459."},
            {"articulos": "10", "descripcion": "Se deroga el 10 de la Ley 29459",
             "fragmento": "Derogar el articulo 10"},
        ]
        insertadas = [self._correr(self.db, r) for r in redacciones]
        self.assertEqual(insertadas, [1, 0, 0], "el reanalisis no debe volver a insertar")
        self.assertEqual(len(self.db.relaciones), 1)

    def test_la_relacion_queda_vinculada_a_la_ley_real_sin_anio(self):
        """Caso D: la cita dice "Ley 29459" sin año y aun asi se resuelve."""
        self._correr(self.db, {"articulos": "articulo 10",
                               "descripcion": "Deroga el articulo 10 de la Ley 29459",
                               "fragmento": "Derogar el articulo 10 de la Ley N° 29459."})
        fila = self.db.relaciones[0]
        self.assertEqual(fila["norma_afectada_id"], "ley-real")
        self.assertEqual(fila["identidad_nivel"], NIVEL_TIPO_NUMERO)
        self.assertIsNone(fila["identidad_candidatas"])

    def test_identidad_ambigua_no_se_vincula_a_ninguna_candidata(self):
        catalogo = CATALOGO_FALSO + [
            {"id": "stub", "document_key": "NORM-LEY-29459-STUB", "tipo_norma": "LEY",
             "numero": "29459", "anio": None},
        ]
        original = D.call_deepseek
        D.call_deepseek = lambda *a, **k: (D.ESTADO_OK, self._respuesta_ia(
            {"articulos": "articulo 10", "descripcion": "Deroga el art. 10 de la Ley 29459",
             "fragmento": "Derogar el articulo 10 de la Ley N° 29459."}))
        try:
            D.procesar_norma(
                self.db,
                {"id": "origen-1", "document_key": "RM-500-2025",
                 "tipo_norma": "RM", "numero": "500", "anio": 2025},
                "clave-falsa",
                catalogo,
            )
        finally:
            D.call_deepseek = original
        fila = self.db.relaciones[0]
        self.assertIsNone(fila["norma_afectada_id"], "nunca elegir una de dos candidatas")
        self.assertEqual(fila["identidad_nivel"], AMBIGUA)
        self.assertIn("NORM-LEY-29459-STUB", fila["identidad_candidatas"])

    def test_relaciones_distintas_del_mismo_origen_no_se_fusionan(self):
        self._correr(self.db, {"articulos": "articulo 10", "descripcion": "deroga art 10",
                               "fragmento": "Derogar el articulo 10"})
        self._correr(self.db, {"articulos": "articulo 12", "descripcion": "deroga art 12",
                               "fragmento": "Derogar el articulo 12"})
        self.assertEqual(len(self.db.relaciones), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
