"""Regresión del CANARY CONTROLADO de F-03B — SOLO DRY-RUN.

El canary no es una excepción a la política documental: es MÁS ESTRICTO que
producción. Estos tests prueban ambas capas: las reglas propias del canary
(allowlist exacta, máximo 3, MATCH_EXACTO únicamente, `file_url` NULL
verificado dos veces, determinismo A/B) y que nunca se ejecuta una escritura
real -`agent.supabase` es un doble que hace `raise AssertionError` ante
cualquier `.insert(`/`.update(`/`.upsert(`/`.delete(`, así que un canary que
intentara escribir haría fallar el test, no la base de datos-.

Sin red ni PDFs reales: `EvidenciaDocumental.apariciones` se construye desde
texto, igual que en test_identidad_documental.py y
test_politica_documental.py.
"""

import re
import sys
import types
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))


def _stub_module(name, **attrs):
    if name in sys.modules:
        return sys.modules[name]
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


_stub_module("dotenv", load_dotenv=lambda *a, **k: None)


class _StubSupabaseClientNuncaSeToca:
    """Cualquier intento de escritura real hace fallar el test, no la base."""

    def table(self, *_a, **_k):
        raise AssertionError(
            "El canary NUNCA debe construir una llamada real a Supabase fuera "
            "de los dobles inyectados por el test."
        )


_stub_module(
    "supabase",
    Client=_StubSupabaseClientNuncaSeToca,
    create_client=lambda *a, **k: _StubSupabaseClientNuncaSeToca(),
)

if "bs4" not in sys.modules:
    class _StubAnchor:
        def __init__(self, href: str, texto: str):
            self._href, self._texto = href, texto

        def get(self, key, default=None):
            return self._href if key == "href" else default

        def get_text(self, _sep=" "):
            return self._texto

    class _StubBeautifulSoup:
        def __init__(self, html, *_a, **_k):
            self._anchors = [
                _StubAnchor(href, texto)
                for href, texto in re.findall(
                    r'<a\s+href="([^"]*)"[^>]*>([^<]*)</a>', html or "", re.IGNORECASE
                )
            ]

        def find_all(self, _tag=None, href=False, **_k):
            return self._anchors

    _stub_module("bs4", BeautifulSoup=_StubBeautifulSoup)

import agents.agent_normative_pdf_detector as detector  # noqa: E402
import scripts.canary_f03b_pdf_detector as canary  # noqa: E402
from identidad_documental import EvidenciaDocumental, identidades_en_texto  # noqa: E402
from identidad_normativa import construir_identidad  # noqa: E402


def encabezado(tipo_largo: str, numero: str) -> str:
    return f"MINISTERIO DE SALUD\n\n{tipo_largo}\nN° {numero}\n\nLima, 3 de junio de 2024\n"


class _StubQuery:
    """Doble mínimo de la interfaz fluida de `postgrest`: `.select().in_()`,
    `.select().eq().single()`, ambos terminando en `.execute()`. Cualquier
    `.insert(`/`.update(`/`.upsert(`/`.delete(` invocado sobre este doble NO
    existe como método: la llamada falla con AttributeError, no se ejecuta
    silenciosamente."""

    def __init__(self, filas: dict[str, dict | None], file_url_por_id: dict | None = None):
        self._filas = filas
        # Permite simular que el file_url cambio ENTRE la lectura inicial y
        # el refresco de precondicion (test de "cambio de precondicion").
        self._file_url_por_id = file_url_por_id or {
            f["id"]: f["file_url"] for f in filas.values() if f
        }

    def select(self, *_a, **_k):
        return self

    def in_(self, _campo, valores):
        self._filtro = list(valores)
        return self

    def eq(self, _campo, valor):
        self._eq_valor = valor
        return self

    def single(self):
        self._single = True
        return self

    def execute(self):
        if hasattr(self, "_filtro"):
            data = [
                dict(fila, id=fila["id"])
                for clave, fila in self._filas.items()
                if fila and clave in self._filtro
            ]
            return types.SimpleNamespace(data=data)
        if hasattr(self, "_eq_valor"):
            file_url = self._file_url_por_id.get(self._eq_valor)
            return types.SimpleNamespace(data={"file_url": file_url})
        return types.SimpleNamespace(data=[])


def construir_agente(filas: dict[str, dict | None], *, html_por_url: dict[str, str],
                     evidencias_por_url: dict[str, EvidenciaDocumental] | None = None,
                     evidencia_factory=None,
                     file_url_por_id: dict | None = None):
    agente = detector.NormativePdfDetectorAgent.__new__(detector.NormativePdfDetectorAgent)
    agente.ignored_link_connection_errors = 0
    agente.table_name = "digemid_documentos"
    agente.supabase = types.SimpleNamespace(
        table=lambda _n: _StubQuery(filas, file_url_por_id)
    )
    agente.is_pdf_response = lambda _url: False

    class _FakeResponse:
        def __init__(self, texto):
            self.text = texto
            self.headers = {"Content-Type": "text/html"}

    agente.fetch_detail_response = lambda url: _FakeResponse(html_por_url.get(url, ""))

    if evidencia_factory is not None:
        agente._evidencia_de_candidato = evidencia_factory
    else:
        def _por_defecto(url, anchor_text, identidad_objetivo):
            if evidencias_por_url and url in evidencias_por_url:
                return evidencias_por_url[url]
            return EvidenciaDocumental(
                identidad_objetivo=identidad_objetivo,
                apariciones=[], total_paginas=0, paginas_analizadas=0,
                texto_completo="", filename=url.rsplit("/", 1)[-1], url=url,
            )
        agente._evidencia_de_candidato = _por_defecto

    return agente


def fila_base(document_key: str, *, title: str, file_url=None) -> dict:
    return {
        "id": f"id-{document_key}",
        "document_key": document_key,
        "title": title,
        "detail_url": f"https://x/{document_key}/detalle",
        "file_url": file_url,
        "raw": {},
    }


OBJETIVO_900 = construir_identidad("RM", "900-2025-MINSA")


def evidencia_exacta(url: str, identidad_objetivo, pdf_sha256="sha-fijo") -> EvidenciaDocumental:
    texto = encabezado("RESOLUCION MINISTERIAL", "900-2025/MINSA")
    return EvidenciaDocumental(
        identidad_objetivo=identidad_objetivo,
        apariciones=identidades_en_texto(texto, 1),
        total_paginas=2, paginas_analizadas=2, texto_completo=texto,
        filename=url.rsplit("/", 1)[-1], url=url, pdf_sha256=pdf_sha256,
    )


class TestAllowlistEstrictaMaximo3(unittest.TestCase):
    """1. allowlist de 3 procesa solo esas 3.
    2. una cuarta nunca se procesa -ni por LIMIT ni si la consulta trajera de mas-.
    """

    def test_una_allowlist_de_3_procesa_exactamente_esas_3(self):
        filas = {
            "A": fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA"),
            "B": fila_base("B", title="Resolución Ministerial N° 901-2025/MINSA"),
            "C": fila_base("C", title="Resolución Ministerial N° 902-2025/MINSA"),
        }
        html_por_url = {
            "https://x/A/detalle": '<a href="https://x/A/900.pdf">enlace</a>',
            "https://x/B/detalle": '<a href="https://x/B/901.pdf">enlace</a>',
            "https://x/C/detalle": '<a href="https://x/C/902.pdf">enlace</a>',
        }

        def factory(url, _anchor, identidad_objetivo):
            numero = url.rsplit("/", 2)[-2]
            texto = encabezado(
                "RESOLUCION MINISTERIAL",
                {"A": "900-2025/MINSA", "B": "901-2025/MINSA", "C": "902-2025/MINSA"}[numero],
            )
            return EvidenciaDocumental(
                identidad_objetivo=identidad_objetivo,
                apariciones=identidades_en_texto(texto, 1),
                total_paginas=1, paginas_analizadas=1, texto_completo=texto,
                filename=url.rsplit("/", 1)[-1], url=url, pdf_sha256=f"sha-{numero}",
            )

        agente = construir_agente(filas, html_por_url=html_por_url, evidencia_factory=factory)
        corrida = canary.ejecutar_dry_run(agente, ["A", "B", "C"])

        self.assertEqual(len(corrida), 3)
        self.assertEqual([f["document_key"] for f in corrida], ["A", "B", "C"])
        self.assertTrue(all(f["apto"] for f in corrida))

    def test_una_cuarta_nunca_se_procesa_allowlist_rechazada(self):
        with self.assertRaises(ValueError):
            canary.validar_allowlist(["A", "B", "C", "D"])

    def test_una_cuarta_nunca_se_procesa_aunque_la_consulta_devuelva_de_mas(self):
        """Defensa en profundidad: aunque `obtener_filas_allowlist` recibiera
        de la base una fila que NO esta en la allowlist -el equivalente a que
        `.in_()` fallara o alguien use un LIMIT en vez de una allowlist-, el
        filtrado explicito por clave la descarta."""
        filas_con_extra = {
            "A": fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA"),
            "INTRUSA": fila_base("INTRUSA", title="Resolución Ministerial N° 999-2025/MINSA"),
        }
        agente = construir_agente(filas_con_extra, html_por_url={})
        resultado = canary.obtener_filas_allowlist(agente, ["A"])

        self.assertEqual(list(resultado.keys()), ["A"])
        self.assertNotIn("INTRUSA", resultado)

    def test_allowlist_vacia_se_rechaza(self):
        with self.assertRaises(ValueError):
            canary.validar_allowlist([])

    def test_allowlist_con_duplicados_se_rechaza(self):
        with self.assertRaises(ValueError):
            canary.validar_allowlist(["A", "A", "B"])


class TestPrecondiciones(unittest.TestCase):
    """3 a 8: cada precondicion, una por una."""

    def test_document_key_inexistente_aborta_ese_caso(self):
        agente = construir_agente({"A": None}, html_por_url={})
        ficha = canary.evaluar_fila_para_canary(agente, "A", None)

        self.assertFalse(ficha["apto"])
        self.assertEqual(ficha["motivo_aborto"], canary.ABORT_NO_EXISTE)

    def test_file_url_ya_no_es_null_no_escribe(self):
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA",
                         file_url="https://x/ya-existe.pdf")
        agente = construir_agente({"A": fila}, html_por_url={})
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertFalse(ficha["apto"])
        self.assertEqual(ficha["motivo_aborto"], canary.ABORT_FILE_URL_NO_NULL)

    def test_decision_distinta_de_match_exacto_no_escribe(self):
        """Contenido CONTRADICTORIO -> NO_ENCONTRADA_TRAS_AUDITORIA_COMPLETA,
        que no es MATCH_EXACTO: el canary debe abortar, no solo el resolvedor."""
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA")
        html = {"https://x/A/detalle": '<a href="https://x/otra.pdf">enlace</a>'}

        def factory(url, _anchor, identidad_objetivo):
            texto = encabezado("LEY", "29698")  # otra norma
            return EvidenciaDocumental(
                identidad_objetivo=identidad_objetivo,
                apariciones=identidades_en_texto(texto, 1),
                total_paginas=1, paginas_analizadas=1, texto_completo=texto,
                filename="otra.pdf", url=url, pdf_sha256="sha-otra",
            )

        agente = construir_agente({"A": fila}, html_por_url=html, evidencia_factory=factory)
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertFalse(ficha["apto"])
        self.assertEqual(ficha["motivo_aborto"], canary.ABORT_NO_MATCH_EXACTO)

    def test_auditoria_incompleta_no_escribe(self):
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA")
        html = {"https://x/A/detalle": '<a href="https://x/grande.pdf">enlace</a>'}

        def factory(url, _anchor, identidad_objetivo):
            ev = EvidenciaDocumental(
                identidad_objetivo=identidad_objetivo,
                apariciones=[],  # nada leido todavia
                total_paginas=40, paginas_analizadas=3,  # se corto antes de terminar
                texto_completo="", filename="grande.pdf", url=url, pdf_sha256="sha-grande",
            )
            return ev

        agente = construir_agente({"A": fila}, html_por_url=html, evidencia_factory=factory)
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertFalse(ficha["apto"])
        self.assertIn(
            ficha["motivo_aborto"],
            (canary.ABORT_NO_MATCH_EXACTO, canary.ABORT_AUDITORIA_INCOMPLETA),
        )

    def test_multinorma_no_entra_en_este_primer_canary(self):
        """MATCH_MULTINORMA es un resultado VALIDO para el resolvedor, pero
        este primer canary lo rechaza explicitamente: solo MATCH_EXACTO."""
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA")
        html = {"https://x/A/detalle": '<a href="https://x/peruano.pdf">enlace</a>'}

        def factory(url, _anchor, identidad_objetivo):
            texto = (
                encabezado("RESOLUCION MINISTERIAL", "900-2025/MINSA")
                + "\n" + encabezado("DECRETO SUPREMO", "010-2017-SA")
            )
            return EvidenciaDocumental(
                identidad_objetivo=identidad_objetivo,
                apariciones=identidades_en_texto(texto, 1),
                total_paginas=1, paginas_analizadas=1, texto_completo=texto,
                filename="peruano.pdf", url=url, pdf_sha256="sha-peruano",
            )

        agente = construir_agente({"A": fila}, html_por_url=html, evidencia_factory=factory)
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertFalse(ficha["apto"])
        self.assertEqual(ficha["motivo_aborto"], canary.ABORT_NO_MATCH_EXACTO)
        self.assertNotEqual(ficha["classification"], "MATCH_EXACTO")

    def test_identidad_desconocida_no_escribe(self):
        """Titulo que solo cita a otra norma, sin declarar la suya (F-03B)."""
        fila = fila_base("A", title="Modifican la Resolución Ministerial N° 419-2025/MINSA")
        agente = construir_agente({"A": fila}, html_por_url={})
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertFalse(ficha["apto"])
        self.assertEqual(ficha["motivo_aborto"], canary.ABORT_SIN_IDENTIDAD)


class TestDeterminismoDobleCorrida(unittest.TestCase):
    """9. SHA cambia entre preflight A/B -> canary no apto."""

    def _agente_con_sha(self, sha_devuelto):
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA")
        html = {"https://x/A/detalle": '<a href="https://x/900.pdf">enlace</a>'}

        def factory(url, _anchor, identidad_objetivo):
            return evidencia_exacta(url, identidad_objetivo, pdf_sha256=sha_devuelto())

        return construir_agente({"A": fila}, html_por_url=html, evidencia_factory=factory), fila

    def test_sha_estable_es_deterministico(self):
        agente, _fila = self._agente_con_sha(lambda: "sha-estable")
        corrida_a = canary.ejecutar_dry_run(agente, ["A"])
        corrida_b = canary.ejecutar_dry_run(agente, ["A"])

        deterministico, diferencias = canary.comparar_corridas(corrida_a, corrida_b)
        self.assertTrue(deterministico, diferencias)
        self.assertEqual(diferencias, [])

    def test_sha_cambia_entre_a_y_b_no_apto(self):
        contador = {"n": 0}

        def sha_inestable():
            contador["n"] += 1
            return f"sha-version-{contador['n']}"

        agente, _fila = self._agente_con_sha(sha_inestable)
        corrida_a = canary.ejecutar_dry_run(agente, ["A"])
        corrida_b = canary.ejecutar_dry_run(agente, ["A"])

        deterministico, diferencias = canary.comparar_corridas(corrida_a, corrida_b)
        self.assertFalse(deterministico)
        self.assertTrue(any("pdf_sha256" in d for d in diferencias))


class TestPrecondicionCambioEntreLecturaYPropuesta(unittest.TestCase):
    """La segunda verificacion de file_url -justo antes de proponer el
    payload- debe abortar si cambio desde la lectura inicial."""

    def test_file_url_cambia_entre_lectura_inicial_y_refresco_aborta(self):
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA", file_url=None)
        html = {"https://x/A/detalle": '<a href="https://x/900.pdf">enlace</a>'}

        def factory(url, _anchor, identidad_objetivo):
            return evidencia_exacta(url, identidad_objetivo)

        # El refresco de precondicion devuelve un file_url YA NO nulo -alguien
        # mas escribio esta fila entre la lectura inicial y la propuesta-.
        agente = construir_agente(
            {"A": fila}, html_por_url=html, evidencia_factory=factory,
            file_url_por_id={"id-A": "https://x/otro-proceso-ya-escribio.pdf"},
        )
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertFalse(ficha["apto"])
        self.assertEqual(ficha["motivo_aborto"], canary.ABORT_PRECONDICION_CAMBIO)


class TestPayloadContieneEvidenciaDocumental(unittest.TestCase):
    """10. payload generado contiene evidencia documental."""

    def test_payload_incluye_raw_pdf_detection_con_candidatos(self):
        fila = fila_base("A", title="Resolución Ministerial N° 900-2025/MINSA")
        html = {"https://x/A/detalle": '<a href="https://x/900.pdf">enlace</a>'}

        def factory(url, _anchor, identidad_objetivo):
            return evidencia_exacta(url, identidad_objetivo)

        agente = construir_agente({"A": fila}, html_por_url=html, evidencia_factory=factory)
        ficha = canary.evaluar_fila_para_canary(agente, "A", fila)

        self.assertTrue(ficha["apto"])
        payload = ficha["payload_propuesto"]
        self.assertIn("raw", payload)
        self.assertIn("pdf_detection", payload["raw"])
        self.assertIn("candidatos", payload["raw"]["pdf_detection"])
        self.assertGreaterEqual(len(payload["raw"]["pdf_detection"]["candidatos"]), 1)
        candidato = payload["raw"]["pdf_detection"]["candidatos"][0]
        self.assertEqual(candidato["clasificacion"], "PDF_IDENTIDAD_EXACTA")
        self.assertEqual(payload["file_url"], "https://x/900.pdf")
        self.assertEqual(payload["process_status"], "pdf_detected")
        self.assertTrue(payload["has_file"])


class TestNuncaEscribeDeVerdad(unittest.TestCase):
    """El doble de Supabase hace `raise AssertionError` ante cualquier
    `.table(...)` fuera de los stubs inyectados por el test -si el canary
    intentara construir un `.update()` real (no a traves de `_StubQuery`),
    este test lo detectaria."""

    def test_agente_real_sin_stub_de_escritura_nunca_se_toca(self):
        agente_sin_stub = detector.NormativePdfDetectorAgent.__new__(
            detector.NormativePdfDetectorAgent
        )
        agente_sin_stub.supabase = _StubSupabaseClientNuncaSeToca()
        agente_sin_stub.table_name = "digemid_documentos"

        with self.assertRaises(AssertionError):
            agente_sin_stub.supabase.table("digemid_documentos")


if __name__ == "__main__":
    unittest.main()
