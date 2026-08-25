"""F-04-B.1: lógica pura del paquete visual autocontenido para revisión humana.

Sin Supabase, sin red, sin filesystem: dado el Manifest V2 ya congelado y la
comparación de motores ya generada (ambos read-only, de corridas anteriores),
decide DETERMINÍSTICAMENTE qué imagen le corresponde a cada una de las 50
páginas y valida que nada se haya perdido, duplicado o colado por error antes
de que `scripts/f04b_generar_paquete_golden.py` descargue un solo PDF.

Regla de oro (igual que en F-04-A/F-04-A.2): ante una discrepancia -un SHA256
que no coincide con el manifest congelado, una página fuera de la muestra,
una imagen que no se puede enlazar de vuelta a su fila- el resultado se
EXCLUYE y se marca explícitamente. Nunca se sustituye en silencio.
"""

import re

GOLDEN_PDF_SHA_MISMATCH = "GOLDEN_PDF_SHA_MISMATCH"
PAGINA_FUERA_DEL_PDF = "PAGINA_FUERA_DEL_PDF"
RENDERIZADO_OK = "RENDERIZADO_OK"
ERROR_RENDER = "ERROR_RENDER"

_NOMBRE_ARCHIVO_RE = re.compile(r"^(\d{3})_(.+)_p(\d+)\.png$")


def clave_pagina(document_key: str, page_number: int) -> tuple[str, int]:
    return (document_key, int(page_number))


def _clave_segura_para_archivo(document_key: str) -> str:
    """Sustituye cualquier caracter no seguro para nombre de archivo. Nunca
    debe producir una colisión entre dos document_key distintos que ya eran
    distintos como texto -solo normaliza separadores de sistema de archivos,
    no reduce el alfabeto de forma agresiva-."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", document_key)


def nombre_archivo_imagen(indice_1based: int, document_key: str, page_number: int) -> str:
    """Nombre determinista: `001_RM-250-2019_p1.png`. El índice de 3 dígitos
    es puramente para que el listado de archivos quede ordenado igual que la
    muestra; la identidad real de la página es (document_key, page_number),
    nunca el índice por sí solo."""
    return f"{indice_1based:03d}_{_clave_segura_para_archivo(document_key)}_p{int(page_number)}.png"


def orden_paginas(filas: list[dict]) -> list[dict]:
    """Orden determinista y estable para numerar, independiente del orden en
    que haya llegado el manifest (que está ordenado por riesgo, no por
    documento)."""
    return sorted(filas, key=lambda f: (f["document_key"], int(f["page_number"])))


def sha_coincide(sha_esperado: str | None, sha_calculado: str | None) -> bool:
    if not sha_esperado or not sha_calculado:
        return False
    return sha_esperado.strip().lower() == sha_calculado.strip().lower()


def construir_plan_render(filas_manifest: list[dict]) -> list[dict]:
    """Para cada fila del Manifest V2 congelado, en orden determinista, arma
    la entrada del plan de render: índice, nombre de archivo esperado,
    document_key, page_number, SHA256 esperado (el del manifest, nunca uno
    recalculado), y de dónde descargar el PDF."""
    ordenadas = orden_paginas(filas_manifest)
    plan = []
    for indice, fila in enumerate(ordenadas, start=1):
        document_key = fila["document_key"]
        page_number = int(fila["page_number"])
        plan.append({
            "indice": indice,
            "document_key": document_key,
            "page_number": page_number,
            "pdf_sha256_esperado": fila.get("pdf_sha256"),
            "storage_path": fila.get("storage_path"),
            "archivo_imagen": "paginas/" + nombre_archivo_imagen(indice, document_key, page_number),
        })
    return plan


def verificar_cobertura_paginas(filas_manifest: list[dict], filas_comparacion: list[dict]) -> dict:
    """Cruza el Manifest V2 contra la comparación de motores YA generada:
    deben ser exactamente las mismas 50 (document_key, page_number), sin
    duplicados de ningún lado y sin que ninguna traiga páginas que la otra no
    tiene -si algo no cuadra, NO es un detalle menor: significa que estamos a
    punto de renderizar o de mostrar algo que no es la muestra congelada-."""
    claves_manifest = [clave_pagina(f["document_key"], f["page_number"]) for f in filas_manifest]
    claves_comparacion = [clave_pagina(f["document_key"], f["page_number"]) for f in filas_comparacion]
    set_manifest, set_comparacion = set(claves_manifest), set(claves_comparacion)
    return {
        "total_manifest": len(claves_manifest),
        "total_comparacion": len(claves_comparacion),
        "duplicados_en_manifest": len(claves_manifest) - len(set_manifest),
        "duplicados_en_comparacion": len(claves_comparacion) - len(set_comparacion),
        "solo_en_manifest": sorted(set_manifest - set_comparacion),
        "solo_en_comparacion": sorted(set_comparacion - set_manifest),
        "coincide_exactamente": (
            set_manifest == set_comparacion
            and len(claves_manifest) == len(set_manifest)
            and len(claves_comparacion) == len(set_comparacion)
        ),
    }


def verificar_presencia(plan: list[dict], requeridas: list[tuple[str, int]]) -> dict[str, bool]:
    """Confirma que ciertas (document_key, page_number) puntuales -pedidas
    explícitamente como control obligatorio- están en el plan de render."""
    presentes = {clave_pagina(f["document_key"], f["page_number"]) for f in plan}
    resultado = {}
    for document_key, page_number in requeridas:
        etiqueta = f"{document_key}_p{page_number}"
        resultado[etiqueta] = clave_pagina(document_key, page_number) in presentes
    return resultado


def contar_ocr(filas_comparacion: list[dict]) -> int:
    """Páginas cuyo texto histórico ya venía de Tesseract (same_engine_as_stored),
    tal como las identificó F-04-A.2 -no se recalcula aquí, se cuenta lo que
    la comparación ya decidió-."""
    return sum(1 for f in filas_comparacion if f.get("same_engine_as_stored"))


def verificar_enlace_render(entrada_resultado: dict) -> bool:
    """Confirma que el nombre de archivo de una entrada YA renderizada decodifica
    de vuelta exactamente a su (document_key, page_number) -que la imagen no
    se cruzó con la de otra fila por un error de índice o de orden-."""
    archivo = entrada_resultado.get("archivo_imagen", "")
    nombre = archivo.rsplit("/", 1)[-1]
    m = _NOMBRE_ARCHIVO_RE.match(nombre)
    if not m:
        return False
    _indice, clave_archivo, pagina_archivo = m.groups()
    return (
        clave_archivo == _clave_segura_para_archivo(entrada_resultado["document_key"])
        and int(pagina_archivo) == int(entrada_resultado["page_number"])
    )


def resumen_control_calidad(
    plan: list[dict],
    resultados_render: list[dict],
    filas_comparacion: list[dict],
    requeridas: list[tuple[str, int]],
    ocr_esperadas: int,
) -> dict:
    """Arma el control obligatorio completo (F-04-B.1) en un solo dict, listo
    para imprimir e incluir en el paquete. `todos_los_controles_pasan` es la
    única señal que decide si el paquete puede declararse listo."""
    cobertura = verificar_cobertura_paginas(
        [{"document_key": f["document_key"], "page_number": f["page_number"]} for f in plan],
        filas_comparacion,
    )
    renderizadas = [r for r in resultados_render if r.get("estado") == RENDERIZADO_OK]
    mismatches = [r for r in resultados_render if r.get("estado") == GOLDEN_PDF_SHA_MISMATCH]
    archivos = [r["archivo_imagen"] for r in renderizadas]
    duplicados_render = len(archivos) - len(set(archivos))
    enlaces_incorrectos = [r for r in renderizadas if not verificar_enlace_render(r)]
    presencia = verificar_presencia(plan, requeridas)
    ocr_reales = contar_ocr(filas_comparacion)

    # Por construccion, resultados_render solo puede contener filas que
    # vinieron de `plan` (que a su vez viene del manifest): esto NUNCA
    # deberia encontrar nada. Se calcula igual, en vez de asumirlo, para que
    # un cambio futuro que rompa esa garantia no pase desapercibido.
    claves_manifest = {clave_pagina(f["document_key"], f["page_number"]) for f in plan}
    fuera_de_manifest = sum(
        1 for r in renderizadas
        if clave_pagina(r["document_key"], r["page_number"]) not in claves_manifest
    )

    todos_los_controles_pasan = (
        cobertura["coincide_exactamente"]
        and cobertura["total_manifest"] == 50
        and len(renderizadas) == 50
        and duplicados_render == 0
        and fuera_de_manifest == 0
        and len(mismatches) == 0
        and len(enlaces_incorrectos) == 0
        and all(presencia.values())
        and ocr_reales == ocr_esperadas
    )

    return {
        "total_pares_manifest": cobertura["total_manifest"],
        "total_pares_comparacion": cobertura["total_comparacion"],
        "cobertura_coincide_exactamente": cobertura["coincide_exactamente"],
        "solo_en_manifest": cobertura["solo_en_manifest"],
        "solo_en_comparacion": cobertura["solo_en_comparacion"],
        "total_renders_generados": len(renderizadas),
        "renders_duplicados": duplicados_render,
        "renders_fuera_de_manifest": fuera_de_manifest,
        "sha_coincide_count": len(renderizadas),
        "sha_mismatch_count": len(mismatches),
        "paginas_sha_mismatch": [
            {"document_key": r["document_key"], "page_number": r["page_number"]} for r in mismatches
        ],
        "porcentaje_sha_coincide": round(100.0 * len(renderizadas) / len(plan), 2) if plan else 0.0,
        "documentos_requeridos_presentes": presencia,
        "paginas_ocr_identificadas": ocr_reales,
        "paginas_ocr_esperadas": ocr_esperadas,
        "enlace_render_correcto_count": len(renderizadas) - len(enlaces_incorrectos),
        "enlace_render_incorrecto_count": len(enlaces_incorrectos),
        "todos_los_controles_pasan": todos_los_controles_pasan,
    }


__all__ = [
    "GOLDEN_PDF_SHA_MISMATCH",
    "PAGINA_FUERA_DEL_PDF",
    "RENDERIZADO_OK",
    "ERROR_RENDER",
    "clave_pagina",
    "nombre_archivo_imagen",
    "orden_paginas",
    "sha_coincide",
    "construir_plan_render",
    "verificar_cobertura_paginas",
    "verificar_presencia",
    "contar_ocr",
    "verificar_enlace_render",
    "resumen_control_calidad",
]
