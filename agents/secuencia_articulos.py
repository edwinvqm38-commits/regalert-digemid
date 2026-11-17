"""Verifica que la numeracion de articulos de una transcripcion sea coherente.

Un OCR puede comerse una pagina o leer "Articulo 13" donde dice "Articulo 18"
con aspecto de texto perfecto. Si los articulos saltan, retroceden o se
repiten, algo se perdio o se leyo mal. Es una SENAL para revision humana, no
un veredicto: en normas modificatorias (que citan articulos de otras normas
entre comillas) la numeracion puede ser legitimamente irregular.

Solo cuenta encabezados al inicio de linea ("Articulo 12°.-"), no las
referencias dentro de una frase ("conforme al articulo 5").
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Numero con letras que el OCR suele confundir con digitos: O/o->0, l/I->1.
_ENCABEZADO = re.compile(
    r"^[ \t]*(?:ART[ÍI]CULO|ARTICULO|ART\.)\s*([0-9OolI]{1,4})\s*(?:[°º.]|o\b)?\s*(?:[-–—]\s*([A-Z])\b)?\s*(?:[.\-–—:]|$)",
    re.I | re.M,
)
_CONFUSION = str.maketrans({"O": "0", "o": "0", "l": "1", "I": "1"})
MIN_ENCABEZADOS = 5
# Las normas que modifican otras citan articulos ajenos entre comillas: su
# numeracion no es evaluable con este metodo.
_VERBOS_MODIFICATORIOS = re.compile(
    r"modif[ií]quese|incorp[oó]rese|incorp[oó]rase|sustit[uú]yese|sustit[uú]yase|der[oó]guese|"
    r"modificar|incorporar|sustituir|derogar",
    re.I,
)
_PAGINAS_PARA_DETECTAR_MODIFICATORIA = 3
# Un salto muy grande suele ser un numero mal leido o una cita de otra norma.
SALTO_ATIPICO = 50


@dataclass
class Senal:
    tipo: str  # salto | retroceso | repetido | numero_dudoso
    pagina: int
    detalle: str


@dataclass
class ResultadoSecuencia:
    encabezados: int = 0
    senales: list[Senal] = field(default_factory=list)
    evaluable: bool = True
    motivo_no_evaluable: str = ""

    @property
    def requiere_revision(self) -> bool:
        return bool(self.senales)


def analizar_secuencia(paginas: list[str]) -> ResultadoSecuencia:
    vistos: list[tuple[int, int, str]] = []  # (pagina, numero, sufijo)
    res = ResultadoSecuencia()
    for pag, texto in enumerate(paginas, 1):
        for m in _ENCABEZADO.finditer(texto):
            crudo = m.group(1)
            if crudo != crudo.translate(_CONFUSION) or not crudo.isdigit():
                res.senales.append(Senal("numero_dudoso", pag, f"'{crudo}' parece un número leído con letras"))
            try:
                numero = int(crudo.translate(_CONFUSION))
            except ValueError:
                continue
            vistos.append((pag, numero, (m.group(2) or "").upper()))
    res.encabezados = len(vistos)
    cabecera = "\n".join(paginas[:_PAGINAS_PARA_DETECTAR_MODIFICATORIA])
    if _VERBOS_MODIFICATORIOS.search(cabecera):
        res.evaluable = False
        res.motivo_no_evaluable = "norma modificatoria: cita artículos de otras normas"
        res.senales = [s for s in res.senales if s.tipo == "numero_dudoso"]
        return res
    if len(vistos) < MIN_ENCABEZADOS:
        res.evaluable = False
        res.motivo_no_evaluable = "pocos artículos para evaluar"
        res.senales = [s for s in res.senales if s.tipo == "numero_dudoso"]
        return res

    previo_pag, previo_num, previo_suf = vistos[0]
    for pag, num, suf in vistos[1:]:
        if suf:  # 5-A, 5-B: bis/ter, no altera la secuencia principal
            continue
        if num == previo_num and not previo_suf:
            res.senales.append(Senal("repetido", pag, f"Artículo {num} aparece de nuevo"))
        elif num <= 2 and num < previo_num:
            pass  # reinicio legitimo: anexo, indice o nueva seccion que vuelve a empezar en 1
        elif num < previo_num:
            res.senales.append(Senal("retroceso", pag, f"Artículo {num} después del {previo_num} (pág. {previo_pag})"))
        elif num > previo_num + SALTO_ATIPICO:
            res.senales.append(Senal("numero_dudoso", pag, f"Artículo {num} tras el {previo_num}: ¿número mal leído o cita de otra norma?"))
            continue  # no mueve la referencia: el siguiente articulo normal no debe verse como retroceso
        elif num > previo_num + 1:
            faltan = ", ".join(str(n) for n in range(previo_num + 1, min(num, previo_num + 4)))
            res.senales.append(Senal("salto", pag, f"Del {previo_num} (pág. {previo_pag}) salta al {num}; faltan {faltan}"))
        previo_pag, previo_num, previo_suf = pag, num, ""
    return res
