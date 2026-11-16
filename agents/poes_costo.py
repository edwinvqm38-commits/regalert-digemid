"""Cotizacion de una revision de POES con IA, ANTES de gastar nada.

Calcula tokens estimados y un precio al usuario a partir de una tarifa que el
dueño del producto define (no hay precios fijos aqui: los costos de los
proveedores cambian). Asi el usuario ve "tu POES tiene N paginas, la revision
cuesta S/ X" y confirma antes de que se llame a la IA.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

CHARS_POR_TOKEN = 3.2  # español: conservador (sobreestima un poco)
TOKENS_CONTEXTO_NORMAS_POR_PROCEDIMIENTO = 3000
TOKENS_PROMPT_BASE = 600
TOKENS_SALIDA_POR_PROCEDIMIENTO = 700
TOKENS_SALIDA_FALTANTES = 1500


@dataclass(frozen=True)
class Tarifa:
    usd_por_millon_entrada: float
    usd_por_millon_salida: float
    usd_a_pen: float
    multiplicador_margen: float  # precio = costo * multiplicador (cubre OCR, fallos, soporte)
    minimo_pen: float
    redondeo_pen: float = 0.5


@dataclass(frozen=True)
class Cotizacion:
    paginas: int
    procedimientos: int
    tokens_entrada: int
    tokens_salida: int
    costo_usd: float
    precio_pen: float


def estimar_tokens(texto: str) -> int:
    return math.ceil(len(texto) / CHARS_POR_TOKEN)


def cotizar(paginas: list[str], procedimientos: int, tarifa: Tarifa) -> Cotizacion:
    n = max(procedimientos, 1)
    entrada = (
        sum(estimar_tokens(p) for p in paginas)
        + n * (TOKENS_CONTEXTO_NORMAS_POR_PROCEDIMIENTO + TOKENS_PROMPT_BASE)
        + TOKENS_CONTEXTO_NORMAS_POR_PROCEDIMIENTO  # paso de "que falta" a nivel documento
    )
    salida = n * TOKENS_SALIDA_POR_PROCEDIMIENTO + TOKENS_SALIDA_FALTANTES
    costo = entrada / 1e6 * tarifa.usd_por_millon_entrada + salida / 1e6 * tarifa.usd_por_millon_salida
    bruto = max(costo * tarifa.usd_a_pen * tarifa.multiplicador_margen, tarifa.minimo_pen)
    precio = math.ceil(bruto / tarifa.redondeo_pen) * tarifa.redondeo_pen
    return Cotizacion(len(paginas), n, entrada, salida, round(costo, 4), round(precio, 2))
