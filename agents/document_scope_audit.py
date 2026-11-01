"""Auditoría conservadora y read-only de pertenencia documental por página.

No usa ``quality_score`` ni decide qué texto debe persistirse. Consume bloques y
tablas con coordenadas, conserva la evidencia literal y produce únicamente un
diagnóstico serializable. Un resultado ``scope_verified`` requiere una señal
positiva de identidad; la ausencia de señales nunca se interpreta como prueba.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


SCOPE_VERIFIED = "scope_verified"
MIXED_PENDING_REVIEW = "mixed_pending_review"
SCOPE_UNKNOWN = "scope_unknown"
AUDIT_VERSION = "f04c1_scope_v2"


@dataclass(frozen=True)
class ScopeBlock:
    text: str
    bbox: tuple[float, float, float, float]
    kind: str = "text"


@dataclass(frozen=True)
class ScopeTable:
    bbox: tuple[float, float, float, float]
    rows: list[list[str | None]] = field(default_factory=list)
    text: str = ""


@dataclass(frozen=True)
class ScopePage:
    page_number: int
    width: float
    height: float
    blocks: list[ScopeBlock]
    tables: list[ScopeTable] = field(default_factory=list)
    printed_page: str | None = None


@dataclass(frozen=True)
class NormIdentity:
    type_code: str
    number: int
    year: int
    sector: str | None = None


@dataclass(frozen=True)
class NormReference:
    raw: str
    identity: NormIdentity
    heading: bool


@dataclass(frozen=True)
class HeadingOccurrence:
    reference: NormReference
    blocks: tuple[ScopeBlock, ...]
    reading_order: int


NORM_TYPE_PATTERN = (
    r"(Decreto\s+Supremo|Resoluci[oóÓO�]n\s+Ministerial|Ley|"
    r"Decreto\s+Legislativo|Resoluci[oóÓO�]n\s+Suprema)"
)
NUMBER_MARK_PATTERN = r"(?:N(?:ro)?\.?\s*[°ºoO�]?\s*)?"
NORM_PATTERNS = (
    re.compile(
        rf"\b{NORM_TYPE_PATTERN}\s*{NUMBER_MARK_PATTERN}"
        r"0*(\d{1,6})\s*[-/]\s*(\d{4})(?:\s*[-/]\s*([A-Z]{2,12}))?",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b{NORM_TYPE_PATTERN}\s*{NUMBER_MARK_PATTERN}"
        r"0*(\d{1,6})\s*[-/]\s*([A-Z]{2,12})\s*[-/]\s*(\d{4})",
        re.IGNORECASE,
    ),
)
NORM_TYPE_ONLY_PATTERN = re.compile(rf"^\s*{NORM_TYPE_PATTERN}\s*$", re.IGNORECASE)
NORM_ID_PATTERNS = (
    re.compile(
        rf"^\s*{NUMBER_MARK_PATTERN}0*(\d{{1,6}})\s*[-/]\s*(\d{{4}})"
        r"(?:\s*[-/]\s*([A-Z]{2,12}))?\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        rf"^\s*{NUMBER_MARK_PATTERN}0*(\d{{1,6}})\s*[-/]\s*([A-Z]{{2,12}})"
        r"\s*[-/]\s*(\d{4})\s*$",
        re.IGNORECASE,
    ),
)
TYPE_CODES = {
    "DECRETO SUPREMO": "DS",
    "RESOLUCION MINISTERIAL": "RM",
    "LEY": "LEY",
    "DECRETO LEGISLATIVO": "DL",
    "RESOLUCION SUPREMA": "RS",
}
KEY_PATTERN = re.compile(
    r"^(DS|RM|LEY|DL|RS)-0*(\d{1,6})-(\d{4})(?:-([A-Z]{2,12}))?$",
    re.IGNORECASE,
)
MINISTRY_PATTERN = re.compile(
    r"\bMINISTERIO\s+DE\s+([A-ZÁÉÍÓÚÑ ]{3,60})",
    re.IGNORECASE,
)
SECTOR_MINISTRIES = {
    "SA": ("SALUD",),
    "MINSA": ("SALUD",),
    "RE": ("RELACIONES EXTERIORES",),
}
CITATION_PREFIX = re.compile(
    r"\b(?:conforme|seg[uú]n|de\s+conformidad|modifica|reglamenta|al\s+amparo|"
    r"previsto|establecido|cita|menciona)\b",
    re.IGNORECASE,
)
CITATION_LINK_END = re.compile(
    r"\b(?:APROBADO|MODIFICADO|REGULADO|SUSTITUIDO|INCORPORADO)\s+POR\s*$",
    re.IGNORECASE,
)
CONTINUATION_PATTERN = re.compile(
    r"^\s*(?:Art[ií]culo|ANEXO|DISPOSICI[ÓO]N|CAP[IÍ]TULO|T[IÍ]TULO)\b",
    re.IGNORECASE,
)
CLOSING_PATTERN = re.compile(
    r"^\s*(?:Reg[ií�]strese|Comun[ií�]quese|Publ[ií�]quese|Dado\s+en|POR\s+TANTO)\b",
    re.IGNORECASE | re.MULTILINE,
)
START_PATTERN = re.compile(
    r"^\s*(?:DECRETA|SE\s+RESUELVE|CONSIDERANDO|ANEXO\s+N[°ºoO�]?)\b",
    re.IGNORECASE | re.MULTILINE,
)
ANNEX_HEADING_PATTERN = re.compile(
    r"^\s*[¿?«\"]?ANEXO(?:\s+N[°ºoO�]?\s*\d+)?\b",
    re.IGNORECASE,
)


def normalize(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value or "")
    return " ".join(
        "".join(char for char in decomposed if unicodedata.category(char) != "Mn")
        .upper()
        .split()
    )


def parse_document_key(document_key: str) -> NormIdentity:
    match = KEY_PATTERN.match(normalize(document_key).replace(" ", ""))
    if not match:
        raise ValueError(f"document_key no soportado: {document_key}")
    return NormIdentity(
        type_code=match.group(1).upper(),
        number=int(match.group(2)),
        year=int(match.group(3)),
        sector=match.group(4).upper() if match.group(4) else None,
    )


def _type_code(raw_type: str) -> str:
    normalized = normalize(raw_type).replace("�", "O")
    return TYPE_CODES[normalized]


def _same_identity(left: NormIdentity, right: NormIdentity) -> bool:
    return (
        left.type_code == right.type_code
        and left.number == right.number
        and left.year == right.year
        and (left.sector is None or right.sector is None or left.sector == right.sector)
    )


def _heading_like_line(line: str, match_start: int) -> bool:
    prefix = line[:match_start].strip(" :.-–—\t")
    if prefix:
        return False
    compact = line.strip()
    return len(compact) <= 220


def extract_norm_references(text: str) -> list[NormReference]:
    references: list[NormReference] = []
    previous_text = ""
    seen: set[tuple[str, int, int, str | None]] = set()
    for line in (text or "").splitlines() or [text or ""]:
        for pattern_index, pattern in enumerate(NORM_PATTERNS):
            for match in pattern.finditer(line):
                before = line[: match.start()]
                heading = not previous_text.strip() and _heading_like_line(
                    line, match.start()
                )
                if CITATION_PREFIX.search((previous_text + " " + before)[-120:]):
                    heading = False
                if pattern_index == 0:
                    number, year, sector = (
                        int(match.group(2)),
                        int(match.group(3)),
                        normalize(match.group(4)) if match.group(4) else None,
                    )
                else:
                    number, year, sector = (
                        int(match.group(2)),
                        int(match.group(4)),
                        normalize(match.group(3)),
                    )
                identity = NormIdentity(
                    type_code=_type_code(match.group(1)),
                    number=number,
                    year=year,
                    sector=sector,
                )
                identity_key = (
                    identity.type_code,
                    identity.number,
                    identity.year,
                    identity.sector,
                )
                if identity_key in seen:
                    continue
                seen.add(identity_key)
                references.append(
                    NormReference(
                        raw=match.group(0),
                        identity=identity,
                        heading=heading,
                    )
                )
        previous_text += " " + line
    return references


def _normalized_bbox(
    bbox: tuple[float, float, float, float], width: float, height: float
) -> dict[str, float]:
    x0, y0, x1, y1 = bbox
    return {
        "x0": round(x0 / max(width, 1), 4),
        "y0": round(y0 / max(height, 1), 4),
        "x1": round(x1 / max(width, 1), 4),
        "y1": round(y1 / max(height, 1), 4),
    }


def _signal(
    code: str,
    evidence: str,
    confidence: str,
    bbox: dict[str, float] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "code": code,
        "confidence": confidence,
        "evidence": evidence[:500],
    }
    if bbox is not None:
        value["bbox"] = bbox
    return value


def _column_layout(page: ScopePage) -> dict[str, Any]:
    bands: set[str] = set()
    for block in page.blocks:
        x0, _, x1, _ = block.bbox
        width_ratio = (x1 - x0) / max(page.width, 1)
        center = ((x0 + x1) / 2) / max(page.width, 1)
        if width_ratio > 0.72:
            bands.add("full_width")
        elif center < 0.47:
            bands.add("left")
        elif center > 0.53:
            bands.add("right")
        else:
            bands.add("center")
    return {
        "detected_columns": 2 if {"left", "right"}.issubset(bands) else 1,
        "bands": sorted(bands),
    }


def _column_index(
    bbox: tuple[float, float, float, float], page: ScopePage, detected_columns: int
) -> int:
    if detected_columns != 2:
        return 0
    x0, _, x1, _ = bbox
    center = ((x0 + x1) / 2) / max(page.width, 1)
    return 0 if center < 0.5 else 1


def _reading_position(
    bbox: tuple[float, float, float, float], page: ScopePage, detected_columns: int
) -> tuple[int, float, float]:
    return (
        _column_index(bbox, page, detected_columns),
        bbox[1],
        bbox[0],
    )


def _ordered_blocks(page: ScopePage, detected_columns: int) -> list[ScopeBlock]:
    return sorted(
        page.blocks,
        key=lambda block: (
            _column_index(block.bbox, page, detected_columns),
            block.bbox[1],
            block.bbox[0],
        ),
    )


def _union_bbox(blocks: Iterable[ScopeBlock]) -> tuple[float, float, float, float]:
    values = [block.bbox for block in blocks]
    return (
        min(value[0] for value in values),
        min(value[1] for value in values),
        max(value[2] for value in values),
        max(value[3] for value in values),
    )


def _split_heading_reference(
    type_text: str, identifier_text: str
) -> NormReference | None:
    type_match = NORM_TYPE_ONLY_PATTERN.match(type_text)
    if not type_match:
        return None
    for pattern_index, pattern in enumerate(NORM_ID_PATTERNS):
        identifier_match = pattern.match(identifier_text)
        if not identifier_match:
            continue
        if pattern_index == 0:
            number, year, sector = (
                int(identifier_match.group(1)),
                int(identifier_match.group(2)),
                normalize(identifier_match.group(3))
                if identifier_match.group(3)
                else None,
            )
        else:
            number, year, sector = (
                int(identifier_match.group(1)),
                int(identifier_match.group(3)),
                normalize(identifier_match.group(2)),
            )
        return NormReference(
            raw=f"{type_text.strip()} {identifier_text.strip()}",
            identity=NormIdentity(
                type_code=_type_code(type_match.group(1)),
                number=number,
                year=year,
                sector=sector,
            ),
            heading=True,
        )
    return None


def _split_references_in_block(text: str) -> list[NormReference]:
    """Extrae pares tipo/identificador sin perder su contexto dentro del bloque."""
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    references: list[NormReference] = []
    for index in range(len(lines) - 1):
        reference = _split_heading_reference(
            lines[index], lines[index + 1].rstrip(" ,.;:")
        )
        if reference is None:
            continue
        preceding = " ".join(lines[:index])
        heading = index == 0 and not CITATION_PREFIX.search(preceding)
        references.append(
            NormReference(
                raw=f"{lines[index]} {lines[index + 1]}",
                identity=reference.identity,
                heading=heading,
            )
        )
    return references


def _explicit_expected_annex_blocks(
    page: ScopePage, expected: NormIdentity
) -> list[ScopeBlock]:
    annex_blocks: list[ScopeBlock] = []
    for block in page.blocks:
        compact = " ".join(line.strip() for line in block.text.splitlines())
        if not ANNEX_HEADING_PATTERN.match(compact):
            continue
        references = extract_norm_references(compact)
        references.extend(_split_references_in_block(block.text))
        if any(
            _same_identity(reference.identity, expected) for reference in references
        ):
            annex_blocks.append(block)
    return annex_blocks


def _citation_linked_to_previous(
    ordered: list[ScopeBlock], index: int, page: ScopePage, detected_columns: int
) -> bool:
    if index <= 0:
        return False
    current = ordered[index]
    previous = ordered[index - 1]
    if _column_index(previous.bbox, page, detected_columns) != _column_index(
        current.bbox, page, detected_columns
    ):
        return False
    vertical_gap = current.bbox[1] - previous.bbox[3]
    if vertical_gap > page.height * 0.035:
        return False
    return bool(CITATION_LINK_END.search(normalize(previous.text)))


def _collect_heading_occurrences(
    page: ScopePage, detected_columns: int
) -> tuple[list[HeadingOccurrence], list[str]]:
    ordered = _ordered_blocks(page, detected_columns)
    occurrences: list[HeadingOccurrence] = []
    citations: list[str] = []
    consumed: set[int] = set()

    for index, block in enumerate(ordered):
        if index in consumed:
            continue
        split_reference: NormReference | None = None
        if index + 1 < len(ordered):
            following = ordered[index + 1]
            same_column = _column_index(
                block.bbox, page, detected_columns
            ) == _column_index(following.bbox, page, detected_columns)
            close_enough = following.bbox[1] - block.bbox[3] <= page.height * 0.035
            if same_column and close_enough:
                split_reference = _split_heading_reference(block.text, following.text)
        if split_reference is not None:
            if _citation_linked_to_previous(ordered, index, page, detected_columns):
                citations.append(split_reference.raw)
            else:
                occurrences.append(
                    HeadingOccurrence(
                        reference=split_reference,
                        blocks=(block, following),
                        reading_order=index,
                    )
                )
            consumed.add(index + 1)
            continue

        references = extract_norm_references(block.text)
        references.extend(_split_references_in_block(block.text))
        references.sort(key=lambda reference: reference.heading, reverse=True)
        seen_references: set[tuple[str, int, int, str | None]] = set()
        for reference in references:
            identity_key = (
                reference.identity.type_code,
                reference.identity.number,
                reference.identity.year,
                reference.identity.sector,
            )
            if identity_key in seen_references:
                continue
            seen_references.add(identity_key)
            if not reference.heading or _citation_linked_to_previous(
                ordered, index, page, detected_columns
            ):
                citations.append(reference.raw)
                continue
            occurrences.append(
                HeadingOccurrence(
                    reference=reference,
                    blocks=(block,),
                    reading_order=index,
                )
            )
    return occurrences, citations


def _table_text(table: ScopeTable) -> str:
    if table.text.strip():
        return table.text
    return "\n".join(
        " | ".join("" if cell is None else str(cell) for cell in row)
        for row in table.rows
    )


def _fragmented_cells(table: ScopeTable) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for row_index, row in enumerate(table.rows):
        for column_index in range(len(row) - 1):
            left = str(row[column_index] or "").strip()
            right = str(row[column_index + 1] or "").strip()
            if re.search(r"[A-Za-zÁÉÍÓÚÑáéíóúñ]{3,}$", left) and re.match(
                r"^[A-Za-zÁÉÍÓÚÑáéíóúñ]\s+\d", right
            ):
                issues.append(
                    {
                        "row": row_index + 1,
                        "left_column": column_index + 1,
                        "right_column": column_index + 2,
                        "left_value": left,
                        "right_value": right,
                        "reason": "possible_trailing_letter_moved_to_next_cell",
                    }
                )
    return issues


def _printed_page_from_blocks(page: ScopePage) -> str | None:
    if page.printed_page:
        return page.printed_page
    for block in page.blocks:
        text = block.text.strip()
        if re.fullmatch(r"\d{1,4}", text):
            _, y0, _, y1 = block.bbox
            if y1 / max(page.height, 1) < 0.12 or y0 / max(page.height, 1) > 0.88:
                return text
    return None


def audit_page_scope(
    page: ScopePage,
    expected: NormIdentity,
    previous_expected_evidence: bool = False,
) -> dict[str, Any]:
    signals: list[dict[str, Any]] = []
    layout = _column_layout(page)
    detected_columns = int(layout["detected_columns"])
    ordered_blocks = _ordered_blocks(page, detected_columns)
    heading_occurrences, legitimate_citations = _collect_heading_occurrences(
        page, detected_columns
    )
    expected_headings = [
        occurrence
        for occurrence in heading_occurrences
        if _same_identity(occurrence.reference.identity, expected)
    ]
    foreign_headings = [
        occurrence
        for occurrence in heading_occurrences
        if not _same_identity(occurrence.reference.identity, expected)
    ]
    foreign_ministries: list[tuple[ScopeBlock, str]] = []

    for occurrence in expected_headings:
        signals.append(
            _signal(
                "expected_norm_heading",
                occurrence.reference.raw,
                "high",
                _normalized_bbox(
                    _union_bbox(occurrence.blocks), page.width, page.height
                ),
            )
        )
    for occurrence in foreign_headings:
        signals.append(
            _signal(
                "foreign_norm_heading",
                occurrence.reference.raw,
                "high",
                _normalized_bbox(
                    _union_bbox(occurrence.blocks), page.width, page.height
                ),
            )
        )

    inferred_sector = expected.sector
    if inferred_sector is None and expected_headings:
        inferred_sector = expected_headings[0].reference.identity.sector
    expected_ministries = SECTOR_MINISTRIES.get(inferred_sector or "")

    for block in page.blocks:
        bbox = _normalized_bbox(block.bbox, page.width, page.height)
        for ministry_match in MINISTRY_PATTERN.finditer(normalize(block.text)):
            ministry = ministry_match.group(1).strip()
            heading_line = len(normalize(block.text)) <= 180 and _heading_like_line(
                normalize(block.text), ministry_match.start()
            )
            ministry_matches = expected_ministries is not None and any(
                ministry.startswith(name) for name in expected_ministries
            )
            if (
                heading_line
                and expected_ministries is not None
                and not ministry_matches
            ):
                foreign_ministries.append((block, ministry))
                signals.append(
                    _signal("foreign_ministry_heading", ministry, "medium", bbox)
                )
            elif heading_line and expected_ministries is None:
                signals.append(
                    _signal(
                        "ministry_heading_without_expected_sector",
                        ministry,
                        "low",
                        bbox,
                    )
                )

        if START_PATTERN.search(block.text):
            signals.append(
                _signal("disposition_start_marker", block.text, "medium", bbox)
            )
        if CLOSING_PATTERN.search(block.text):
            signals.append(
                _signal("disposition_close_marker", block.text, "medium", bbox)
            )

    if legitimate_citations:
        signals.append(
            _signal(
                "legal_citations_not_treated_as_foreign",
                "; ".join(legitimate_citations),
                "high",
            )
        )

    if layout["detected_columns"] == 2:
        signals.append(
            _signal(
                "two_column_layout",
                "La página contiene bloques en columnas izquierda y derecha; el orden lineal no prueba mezcla.",
                "high",
            )
        )

    expected_start_order = min(
        (occurrence.reading_order for occurrence in expected_headings), default=None
    )
    expected_start_block = (
        min(expected_headings, key=lambda occurrence: occurrence.reading_order).blocks[
            0
        ]
        if expected_headings
        else None
    )
    expected_start_position = (
        (
            _column_index(expected_start_block.bbox, page, detected_columns),
            expected_start_block.bbox[1],
        )
        if expected_start_block is not None
        else None
    )
    if expected_start_order is not None and expected_start_order > 1:
        signals.append(
            _signal(
                "expected_document_starts_late",
                "La norma esperada comienza después de contenido previo en el flujo de lectura de la página.",
                "high",
            )
        )

    suspicious_regions: list[dict[str, Any]] = []
    seen_text_regions: set[tuple[float, float, float, float]] = set()
    for occurrence in foreign_headings:
        occurrence_bbox = _union_bbox(occurrence.blocks)
        if occurrence_bbox in seen_text_regions:
            continue
        seen_text_regions.add(occurrence_bbox)
        suspicious_regions.append(
            {
                "kind": "text_block",
                "bbox": _normalized_bbox(occurrence_bbox, page.width, page.height),
                "reasons": ["foreign_norm_heading"],
                "evidence": occurrence.reference.raw,
            }
        )
    for block, ministry in foreign_ministries:
        if block.bbox in seen_text_regions:
            continue
        seen_text_regions.add(block.bbox)
        suspicious_regions.append(
            {
                "kind": "text_block",
                "bbox": _normalized_bbox(block.bbox, page.width, page.height),
                "reasons": ["foreign_ministry_heading"],
                "evidence": ministry,
            }
        )

    if expected_start_order is not None:
        for block in ordered_blocks[:expected_start_order]:
            if not CLOSING_PATTERN.search(block.text):
                continue
            if block.bbox in seen_text_regions:
                continue
            seen_text_regions.add(block.bbox)
            suspicious_regions.append(
                {
                    "kind": "text_block",
                    "bbox": _normalized_bbox(block.bbox, page.width, page.height),
                    "reasons": ["prior_disposition_closes_before_expected_start"],
                    "evidence": block.text[:500],
                }
            )
            signals.append(
                _signal(
                    "prior_disposition_closes_before_expected_start",
                    block.text,
                    "high",
                    _normalized_bbox(block.bbox, page.width, page.height),
                )
            )

    table_results: list[dict[str, Any]] = []
    for index, table in enumerate(page.tables, start=1):
        text = _table_text(table)
        bbox = _normalized_bbox(table.bbox, page.width, page.height)
        refs = extract_norm_references(text)
        has_foreign_ref = any(
            reference.heading and not _same_identity(reference.identity, expected)
            for reference in refs
        )
        table_position = _reading_position(table.bbox, page, detected_columns)
        above_expected_start = (
            expected_start_position is not None
            and table_position[:2] < expected_start_position
        )
        disposition_markers = [
            (
                _reading_position(occurrence.blocks[-1].bbox, page, detected_columns),
                _same_identity(occurrence.reference.identity, expected),
            )
            for occurrence in heading_occurrences
        ]
        disposition_markers.extend(
            (
                _reading_position(block.bbox, page, detected_columns),
                False,
            )
            for block, _ in foreign_ministries
        )
        preceding_markers = [
            marker for marker in disposition_markers if marker[0] <= table_position
        ]
        active_marker = max(
            preceding_markers, default=None, key=lambda marker: marker[0]
        )
        follows_foreign_heading = active_marker is not None and not active_marker[1]
        spans_columns = (
            detected_columns == 2
            and table.bbox[0] < page.width * 0.47
            and table.bbox[2] > page.width * 0.53
        )
        fragmented = _fragmented_cells(table)
        suspicious = (
            has_foreign_ref
            or above_expected_start
            or follows_foreign_heading
            or spans_columns
        )
        reasons: list[str] = []
        if has_foreign_ref:
            reasons.append("foreign_norm_heading_in_table")
        if above_expected_start:
            reasons.append("table_precedes_expected_document_start")
        if follows_foreign_heading:
            reasons.append("table_belongs_to_active_foreign_disposition")
        if spans_columns:
            reasons.append("table_reading_order_ambiguous_across_columns")
        if fragmented:
            reasons.append("possible_fragmented_cells")
            signals.append(
                _signal(
                    "fragmented_table_cell",
                    json.dumps(fragmented, ensure_ascii=False),
                    "medium",
                    bbox,
                )
            )
        table_result = {
            "table_index": index,
            "bbox": bbox,
            "suspicious_scope": suspicious,
            "reasons": reasons,
            "fragmented_cells": fragmented,
        }
        table_results.append(table_result)
        if suspicious:
            suspicious_regions.append(
                {
                    "kind": "table",
                    "table_index": index,
                    "bbox": bbox,
                    "reasons": reasons,
                }
            )

    has_expected = bool(expected_headings)
    has_foreign = bool(foreign_headings or foreign_ministries)
    continuation = any(CONTINUATION_PATTERN.search(block.text) for block in page.blocks)
    continuity_supported = (
        previous_expected_evidence and continuation and not has_foreign
    )
    if continuity_supported:
        signals.append(
            _signal(
                "continuity_from_previous_page",
                "La página continúa una estructura normativa y la página física inmediatamente anterior mantenía activa la norma esperada.",
                "high",
            )
        )

    explicit_expected_annex_blocks = _explicit_expected_annex_blocks(page, expected)
    explicit_expected_annex = bool(explicit_expected_annex_blocks)
    if explicit_expected_annex:
        signals.append(
            _signal(
                "annex_explicitly_bound_to_expected_norm",
                "El encabezado del anexo identifica expresamente la norma esperada.",
                "high",
                _normalized_bbox(
                    explicit_expected_annex_blocks[0].bbox, page.width, page.height
                ),
            )
        )

    if has_foreign or suspicious_regions:
        state = MIXED_PENDING_REVIEW
        justification = "Se detectó otra disposición, un cambio de ministerio o una región anterior al inicio explícito de la norma; requiere revisión humana."
    elif has_expected or explicit_expected_annex or continuity_supported:
        state = SCOPE_VERIFIED
        if continuity_supported and not (has_expected or explicit_expected_annex):
            justification = "La página continúa la norma activa de la página física anterior, contiene estructura normativa compatible y no presenta un encabezado competidor."
        else:
            justification = "La página contiene una identificación explícita de la norma esperada y no presenta un encabezado normativo competidor."
    else:
        state = SCOPE_UNKNOWN
        justification = "No existe evidencia positiva suficiente para verificar la pertenencia de toda la página."

    uncertainty: list[str] = []
    if state != SCOPE_VERIFIED:
        uncertainty.append("Requiere comparación visual con el PDF oficial.")
    if suspicious_regions:
        uncertainty.append(
            "Las regiones señaladas no deben atribuirse automáticamente a la norma."
        )
    if layout["detected_columns"] == 2:
        uncertainty.append(
            "El orden de lectura entre columnas debe confirmarse visualmente."
        )
    if any(table["fragmented_cells"] for table in table_results):
        uncertainty.append(
            "La integridad de celdas es independiente de la pertenencia documental."
        )

    last_expected_order = max(
        (occurrence.reading_order for occurrence in expected_headings), default=-1
    )
    last_foreign_order = max(
        (occurrence.reading_order for occurrence in foreign_headings), default=-1
    )
    if last_expected_order >= 0 or last_foreign_order >= 0:
        expected_active_at_end = last_expected_order > last_foreign_order
    else:
        expected_active_at_end = previous_expected_evidence and not has_foreign

    return {
        "page_number_pdf": page.page_number,
        "page_number_printed": _printed_page_from_blocks(page),
        "classification": state,
        "signals": signals,
        "layout": layout,
        "tables": table_results,
        "suspicious_regions": suspicious_regions,
        "justification": justification,
        "uncertainty": uncertainty,
        "review_pending": state != SCOPE_VERIFIED or bool(suspicious_regions),
        "has_expected_identity_evidence": (
            has_expected or explicit_expected_annex or continuity_supported
        ),
        "document_flow": {
            "expected_active_at_start": previous_expected_evidence,
            "expected_active_at_end": expected_active_at_end,
            "expected_sector": inferred_sector,
        },
    }


def audit_document_scope(
    *,
    document_key: str,
    pdf_name: str,
    pdf_sha256: str,
    source_asset_id: int | None,
    pages: Iterable[ScopePage],
    evidence_source: str = "layout_fixture",
) -> dict[str, Any]:
    expected = parse_document_key(document_key)
    page_results: list[dict[str, Any]] = []
    previous_expected_active = False
    previous_page_number: int | None = None
    for page in sorted(pages, key=lambda item: item.page_number):
        adjacent = (
            previous_page_number is not None
            and page.page_number == previous_page_number + 1
        )
        result = audit_page_scope(
            page,
            expected,
            previous_expected_active and adjacent,
        )
        page_results.append(result)
        previous_expected_active = bool(
            result["document_flow"]["expected_active_at_end"]
        )
        inferred_sector = result["document_flow"].get("expected_sector")
        if expected.sector is None and inferred_sector:
            expected = NormIdentity(
                type_code=expected.type_code,
                number=expected.number,
                year=expected.year,
                sector=str(inferred_sector),
            )
        previous_page_number = page.page_number

    counts = {
        state: sum(1 for page in page_results if page["classification"] == state)
        for state in (SCOPE_VERIFIED, MIXED_PENDING_REVIEW, SCOPE_UNKNOWN)
    }
    return {
        "audit_version": AUDIT_VERSION,
        "mode": "dry-run",
        "evidence_source": evidence_source,
        "document_key_expected": document_key,
        "pdf_name": pdf_name,
        "pdf_sha256": pdf_sha256,
        "source_asset_id": source_asset_id,
        "summary": {
            "total_pages": len(page_results),
            "classification_counts": counts,
            "writes_performed": 0,
        },
        "pages": page_results,
    }


def render_scope_summary(report: dict[str, Any]) -> str:
    lines = [
        f"Auditoría F04C.1 (dry-run): {report['document_key_expected']}",
        f"PDF: {report['pdf_name']}",
        f"SHA-256: {report['pdf_sha256']}",
        f"source_asset_id: {report['source_asset_id']}",
        "",
    ]
    for page in report["pages"]:
        printed = page.get("page_number_printed") or "?"
        lines.append(
            f"- PDF {page['page_number_pdf']} / impresa {printed}: "
            f"{page['classification']} — {page['justification']}"
        )
        for region in page["suspicious_regions"]:
            lines.append(
                f"  región sospechosa: {region['kind']} {region.get('table_index', '')} "
                f"({', '.join(region['reasons'])})"
            )
    lines.extend(["", "No se realizaron escrituras en Supabase ni en el PDF."])
    return "\n".join(lines)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scope_page_from_pdf(
    pdf_path: str | Path,
    page_index: int,
    *,
    printed_page: str | None = None,
) -> ScopePage:
    """Adapta una página real. Importa PyMuPDF/pdfplumber solo al invocarse."""
    import fitz
    import pdfplumber

    path = str(pdf_path)
    with fitz.open(path) as document:
        page = document[page_index]
        blocks = [
            ScopeBlock(
                text=str(block[4] or ""),
                bbox=(
                    float(block[0]),
                    float(block[1]),
                    float(block[2]),
                    float(block[3]),
                ),
            )
            for block in page.get_text("blocks")
            if len(block) >= 5 and str(block[4] or "").strip()
        ]
        width, height = float(page.rect.width), float(page.rect.height)

    tables: list[ScopeTable] = []
    with pdfplumber.open(path) as document:
        plumber_page = document.pages[page_index]
        for table in plumber_page.find_tables():
            rows = table.extract() or []
            tables.append(
                ScopeTable(
                    bbox=tuple(float(value) for value in table.bbox),
                    rows=rows,
                )
            )
    return ScopePage(
        page_number=page_index + 1,
        width=width,
        height=height,
        blocks=blocks,
        tables=tables,
        printed_page=printed_page,
    )


def report_to_json(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
