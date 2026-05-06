import json
import logging
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from supabase import create_client


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
REPORTS_DIR = Path("reports")
JSON_REPORT_PATH = REPORTS_DIR / "drive_audit_digemid.json"
MARKDOWN_REPORT_PATH = REPORTS_DIR / "drive_audit_digemid.md"
DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"
SUPABASE_SELECT_FIELDS = (
    "id, document_key, title, source_type, source_section, published_date, "
    "file_name, file_url, drive_file_id, drive_file_url, process_status, raw"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def normalize_for_matching(value: str | None) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.upper()
    normalized = re.sub(r"[^A-Z0-9]+", "", normalized)
    return normalized.strip()


def env_json_to_file(env_name: str, fallback_path_env: str | None = None) -> tuple[Path, bool]:
    raw_value = os.getenv(env_name)
    if raw_value:
        candidate = Path(raw_value)
        if candidate.exists():
            return candidate, False

        temp_dir = Path(".tmp_audit")
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir / f"{env_name.lower()}.json"
        temp_path.write_text(raw_value, encoding="utf-8")
        return temp_path, True

    if fallback_path_env:
        fallback = os.getenv(fallback_path_env)
        if fallback:
            candidate = Path(fallback)
            if candidate.exists():
                return candidate, False

    raise ValueError(
        f"Falta {env_name}"
        + (f" o {fallback_path_env}" if fallback_path_env else "")
    )


def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def get_drive_service():
    client_path, client_is_temp = env_json_to_file(
        "GOOGLE_OAUTH_CLIENT_JSON",
        fallback_path_env="GOOGLE_OAUTH_CLIENT_JSON_PATH",
    )
    token_path, token_is_temp = env_json_to_file(
        "GOOGLE_OAUTH_TOKEN_JSON",
        fallback_path_env="GOOGLE_OAUTH_TOKEN_PATH",
    )

    # Validamos ambos archivos sin exponer contenido; el token es el que usa la API.
    if not client_path.exists():
        raise FileNotFoundError(f"No existe el client json OAuth: {client_path}")
    if not token_path.exists():
        raise FileNotFoundError(f"No existe el token OAuth: {token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise ValueError("El token OAuth no es valido o requiere reautorizacion")

    return build("drive", "v3", credentials=creds), [path for path, is_temp in [
        (client_path, client_is_temp),
        (token_path, token_is_temp),
    ] if is_temp]


def build_parent_path(parent_path: str, name: str) -> str:
    if not parent_path:
        return name
    return f"{parent_path}/{name}"


def list_drive_children(service, folder_id: str) -> list[dict]:
    items: list[dict] = []
    page_token = None

    while True:
        response = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields=(
                    "nextPageToken, files("
                    "id, name, mimeType, parents, createdTime, modifiedTime, "
                    "webViewLink, webContentLink, size)"
                ),
                pageSize=1000,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        items.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return items


def traverse_drive_tree(service, root_folder_id: str) -> tuple[list[dict], list[dict], dict]:
    all_files: list[dict] = []
    all_folders: list[dict] = []
    path_map: dict[str, str] = {root_folder_id: "DIGEMID"}
    pending: list[tuple[str, str]] = [(root_folder_id, "DIGEMID")]

    while pending:
        folder_id, folder_path = pending.pop()
        for item in list_drive_children(service, folder_id):
            parent_ids = item.get("parents") or []
            parent_id = parent_ids[0] if parent_ids else None
            parent_path = path_map.get(parent_id, folder_path)
            item_record = {
                "file_id": item.get("id"),
                "name": item.get("name"),
                "mime_type": item.get("mimeType"),
                "parent_id": parent_id,
                "parent_path": parent_path,
                "path": build_parent_path(parent_path, item.get("name") or ""),
                "createdTime": item.get("createdTime"),
                "modifiedTime": item.get("modifiedTime"),
                "webViewLink": item.get("webViewLink"),
                "webContentLink": item.get("webContentLink"),
                "size": item.get("size"),
            }

            if item_record["mime_type"] == DRIVE_FOLDER_MIME:
                all_folders.append(item_record)
                path_map[item_record["file_id"]] = item_record["path"]
                pending.append((item_record["file_id"], item_record["path"]))
            else:
                all_files.append(item_record)

    return all_files, all_folders, path_map


def is_pdf(item: dict) -> bool:
    name = normalize_text(item.get("name")).lower()
    mime_type = item.get("mime_type") or ""
    return mime_type == "application/pdf" or name.endswith(".pdf")


def infer_alert_key(text: str) -> str | None:
    patterns = [
        r"ALERTA(?:DIGEMID)?N?\D*?(\d{1,3})[-_ ]?(\d{2,4})",
        r"\bALERTA[-_ ]?(\d{1,3})[-_ ]?(\d{2,4})\b",
    ]
    compact = re.sub(r"\s+", "", text.upper())
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if not match:
            continue
        number = match.group(1).zfill(2)
        year = match.group(2)
        if len(year) == 2:
            year = f"20{year}"
        return f"{int(number)}-{year}"
    return None


def infer_normative_key(text: str) -> str | None:
    normalized = re.sub(r"[^A-Z0-9]+", "-", normalize_for_matching_with_hyphen(text))
    normalized = re.sub(r"-+", "-", normalized).strip("-")

    patterns = [
        r"\b(DS)-?(\d{1,4})-?(\d{4})-?(SA|MINSA|PCM|JUS|EF)\b",
        r"\b(RM)-?(\d{1,4})-?(\d{4})-?(MINSA|SA)\b",
        r"\b(RD)-?(\d{1,4})-?(\d{4})-?(DIGEMID|MINSA)\b",
        r"\b(RS)-?(\d{1,4})-?(\d{4})-?(SA|MINSA|PCM)\b",
        r"\b(RJ)-?(\d{1,4})-?(\d{4})-?(MINSA|DIGEMID)\b",
        r"\b(DL)-?(\d{1,4})\b",
        r"\b(LEY)-?(\d{4,6})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if not match:
            continue
        groups = [group for group in match.groups() if group]
        return "-".join(groups)
    return None


def normalize_for_matching_with_hyphen(value: str | None) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.upper()
    normalized = re.sub(r"[^A-Z0-9]+", "-", normalized)
    return normalized.strip("-")


def infer_document_key(item: dict) -> str | None:
    haystack = " ".join(
        filter(
            None,
            [
                item.get("name"),
                item.get("path"),
                item.get("parent_path"),
            ],
        )
    )
    alert_key = infer_alert_key(haystack)
    if alert_key:
        return alert_key
    return infer_normative_key(haystack)


def looks_like_digemid_pdf(item: dict) -> bool:
    if not is_pdf(item):
        return False

    haystack = normalize_for_matching_with_hyphen(
        " ".join(filter(None, [item.get("name"), item.get("path")]))
    )
    markers = [
        "ALERTA",
        "DIGEMID",
        "DS-",
        "RM-",
        "RD-",
        "RS-",
        "LEY-",
        "DECRETO",
        "RESOLUCION",
        "NORMAS-LEGALES",
        "MINSA",
        "SA",
    ]
    return any(marker in haystack for marker in markers) or infer_document_key(item) is not None


def fetch_supabase_documents(supabase) -> list[dict]:
    rows: list[dict] = []
    start = 0
    batch_size = 1000

    while True:
        response = (
            supabase
            .table("digemid_documentos")
            .select(SUPABASE_SELECT_FIELDS)
            .range(start, start + batch_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < batch_size:
            break
        start += batch_size

    return rows


def build_drive_candidate(item: dict) -> dict:
    probable_key = infer_document_key(item)
    return {
        **item,
        "probable_document_key": probable_key,
        "normalized_name": normalize_for_matching(item.get("name")),
        "normalized_path": normalize_for_matching(item.get("path")),
        "is_probable_digemid_pdf": looks_like_digemid_pdf(item),
    }


def build_supabase_indexes(rows: list[dict]) -> dict:
    by_drive_id: dict[str, list[dict]] = defaultdict(list)
    by_document_key: dict[str, list[dict]] = defaultdict(list)
    by_file_name: dict[str, list[dict]] = defaultdict(list)
    normalized_tokens: list[tuple[str, dict]] = []

    for row in rows:
        drive_file_id = normalize_text(row.get("drive_file_id"))
        document_key = normalize_text(row.get("document_key"))
        file_name = normalize_text(row.get("file_name"))
        title = normalize_text(row.get("title"))

        if drive_file_id:
            by_drive_id[drive_file_id].append(row)
        if document_key:
            by_document_key[normalize_for_matching(document_key)].append(row)
        if file_name:
            by_file_name[normalize_for_matching(file_name)].append(row)

        combined = normalize_for_matching(" ".join(filter(None, [document_key, file_name, title])))
        if combined:
            normalized_tokens.append((combined, row))

    return {
        "by_drive_id": by_drive_id,
        "by_document_key": by_document_key,
        "by_file_name": by_file_name,
        "normalized_tokens": normalized_tokens,
    }


def score_match(drive_item: dict, row: dict) -> int:
    score = 0
    probable_key = normalize_for_matching(drive_item.get("probable_document_key"))
    row_key = normalize_for_matching(row.get("document_key"))
    normalized_name = drive_item.get("normalized_name") or ""
    row_file = normalize_for_matching(row.get("file_name"))
    title = normalize_for_matching(row.get("title"))

    if drive_item.get("file_id") and drive_item.get("file_id") == row.get("drive_file_id"):
        score += 100
    if probable_key and row_key and probable_key == row_key:
        score += 60
    if normalized_name and row_file and normalized_name == row_file:
        score += 40
    if probable_key and title and probable_key in title:
        score += 20
    if normalized_name and row_file and (
        normalized_name in row_file or row_file in normalized_name
    ):
        score += 10

    return score


def find_supabase_matches(drive_item: dict, indexes: dict) -> list[dict]:
    candidates: dict[str, tuple[int, dict]] = {}

    for row in indexes["by_drive_id"].get(drive_item.get("file_id"), []):
        candidates[row["id"]] = (score_match(drive_item, row), row)

    probable_key = normalize_for_matching(drive_item.get("probable_document_key"))
    for row in indexes["by_document_key"].get(probable_key, []):
        candidates[row["id"]] = (score_match(drive_item, row), row)

    for row in indexes["by_file_name"].get(drive_item.get("normalized_name"), []):
        candidates[row["id"]] = (score_match(drive_item, row), row)

    for token, row in indexes["normalized_tokens"]:
        if probable_key and probable_key in token:
            candidates[row["id"]] = (score_match(drive_item, row), row)
        elif drive_item.get("normalized_name") and drive_item["normalized_name"] in token:
            candidates[row["id"]] = (score_match(drive_item, row), row)

    ranked = sorted(candidates.values(), key=lambda item: item[0], reverse=True)
    return [row for score, row in ranked if score >= 20]


def summarize_supabase_row(row: dict) -> dict:
    return {
        "id": row.get("id"),
        "document_key": row.get("document_key"),
        "title": row.get("title"),
        "source_type": row.get("source_type"),
        "source_section": row.get("source_section"),
        "published_date": row.get("published_date"),
        "file_name": row.get("file_name"),
        "drive_file_id": row.get("drive_file_id"),
        "process_status": row.get("process_status"),
    }


def classify_audit(drive_files: list[dict], supabase_rows: list[dict]) -> dict:
    indexes = build_supabase_indexes(supabase_rows)
    matched_supabase: list[dict] = []
    drive_only: list[dict] = []
    needs_manual_review: list[dict] = []
    non_digemid_files: list[dict] = []
    matched_row_ids: set[str] = set()
    drive_by_key: dict[str, list[dict]] = defaultdict(list)

    for item in drive_files:
        candidate = build_drive_candidate(item)
        probable_key = candidate.get("probable_document_key")
        if probable_key:
            drive_by_key[normalize_for_matching(probable_key)].append(candidate)

        if not candidate["is_probable_digemid_pdf"]:
            non_digemid_files.append(candidate)
            continue

        matches = find_supabase_matches(candidate, indexes)
        if not matches:
            drive_only.append(candidate)
            continue

        if len(matches) == 1:
            row = matches[0]
            matched_supabase.append({
                "drive": candidate,
                "supabase": summarize_supabase_row(row),
                "match_reason": infer_match_reason(candidate, row),
            })
            matched_row_ids.add(str(row.get("id")))
            continue

        best_score = score_match(candidate, matches[0])
        second_score = score_match(candidate, matches[1])
        if best_score >= second_score + 20:
            row = matches[0]
            matched_supabase.append({
                "drive": candidate,
                "supabase": summarize_supabase_row(row),
                "match_reason": infer_match_reason(candidate, row),
            })
            matched_row_ids.add(str(row.get("id")))
        else:
            needs_manual_review.append({
                "drive": candidate,
                "supabase_candidates": [
                    summarize_supabase_row(row)
                    for row in matches[:5]
                ],
                "reason": "Coincidencia ambigua entre multiples registros Supabase",
            })

    supabase_only: list[dict] = []
    for row in supabase_rows:
        row_id = str(row.get("id"))
        if row_id in matched_row_ids:
            continue

        has_drive_reference = bool(normalize_text(row.get("drive_file_id")) or normalize_text(row.get("file_name")))
        if not has_drive_reference:
            continue

        row_key = normalize_for_matching(row.get("document_key"))
        row_file = normalize_for_matching(row.get("file_name"))
        found_in_drive = any(
            (
                normalize_text(item.get("file_id")) == normalize_text(row.get("drive_file_id"))
                or (row_key and row_key == normalize_for_matching(item.get("probable_document_key")))
                or (row_file and row_file == normalize_for_matching(item.get("name")))
            )
            for item in drive_files
        )
        if not found_in_drive:
            supabase_only.append(summarize_supabase_row(row))

    possible_duplicates = []
    for key, items in drive_by_key.items():
        probable_items = [item for item in items if item["is_probable_digemid_pdf"]]
        if len(probable_items) > 1:
            possible_duplicates.append({
                "document_key": probable_items[0].get("probable_document_key"),
                "files": probable_items,
            })

    return {
        "matched_supabase": matched_supabase,
        "drive_only": drive_only,
        "supabase_only": supabase_only,
        "possible_duplicates": possible_duplicates,
        "needs_manual_review": needs_manual_review,
        "non_digemid_files": non_digemid_files,
    }


def infer_match_reason(drive_item: dict, row: dict) -> str:
    if drive_item.get("file_id") and drive_item.get("file_id") == row.get("drive_file_id"):
        return "drive_file_id exacto"
    probable_key = normalize_for_matching(drive_item.get("probable_document_key"))
    row_key = normalize_for_matching(row.get("document_key"))
    if probable_key and probable_key == row_key:
        return "document_key probable"
    if drive_item.get("normalized_name") == normalize_for_matching(row.get("file_name")):
        return "file_name exacto"
    return "coincidencia parcial normalizada"


def make_json_report(root_folder_id: str, all_files: list[dict], all_folders: list[dict], classifications: dict) -> dict:
    probable_pdfs = [build_drive_candidate(item) for item in all_files if looks_like_digemid_pdf(item)]
    return {
        "root_folder_id": root_folder_id,
        "summary": {
            "total_drive_files": len(all_files),
            "total_drive_folders": len(all_folders),
            "total_detected_pdfs": len(probable_pdfs),
            "matched_supabase": len(classifications["matched_supabase"]),
            "drive_only": len(classifications["drive_only"]),
            "supabase_only": len(classifications["supabase_only"]),
            "possible_duplicates": len(classifications["possible_duplicates"]),
            "needs_manual_review": len(classifications["needs_manual_review"]),
            "non_digemid_files": len(classifications["non_digemid_files"]),
        },
        "drive_inventory": {
            "files": all_files,
            "folders": all_folders,
        },
        "classifications": classifications,
    }


def preview_lines(items: list[dict], formatter, limit: int = 10) -> list[str]:
    return [formatter(item) for item in items[:limit]]


def render_markdown_report(report: dict) -> str:
    summary = report["summary"]
    classifications = report["classifications"]

    lines = [
        "# Auditoria Drive DIGEMID",
        "",
        "## Resumen ejecutivo",
        "",
        f"- Archivos Drive auditados: {summary['total_drive_files']}",
        f"- Carpetas Drive auditadas: {summary['total_drive_folders']}",
        f"- PDFs DIGEMID probables: {summary['total_detected_pdfs']}",
        f"- Coincidencias con Supabase: {summary['matched_supabase']}",
        f"- Drive only: {summary['drive_only']}",
        f"- Supabase only: {summary['supabase_only']}",
        f"- Posibles duplicados: {summary['possible_duplicates']}",
        f"- Revision manual: {summary['needs_manual_review']}",
        f"- No DIGEMID: {summary['non_digemid_files']}",
        "",
        "## Total archivos Drive",
        "",
        f"{summary['total_drive_files']}",
        "",
        "## Total carpetas Drive",
        "",
        f"{summary['total_drive_folders']}",
        "",
        "## Total PDFs detectados",
        "",
        f"{summary['total_detected_pdfs']}",
        "",
        "## Coincidencias con Supabase",
        "",
    ]

    matched_lines = preview_lines(
        classifications["matched_supabase"],
        lambda item: (
            f"- `{item['drive'].get('name')}` -> `{item['supabase'].get('document_key')}` "
            f"({item.get('match_reason')})"
        ),
    )
    lines.extend(matched_lines or ["- Sin coincidencias detectadas"])
    lines.extend(["", "## Drive only", ""])

    drive_only_lines = preview_lines(
        classifications["drive_only"],
        lambda item: (
            f"- `{item.get('name')}` | key probable: `{item.get('probable_document_key') or 'N/A'}` | "
            f"ruta: `{item.get('path')}`"
        ),
    )
    lines.extend(drive_only_lines or ["- Sin archivos solo en Drive"])
    lines.extend(["", "## Supabase only", ""])

    supabase_only_lines = preview_lines(
        classifications["supabase_only"],
        lambda item: (
            f"- `{item.get('document_key') or 'SIN-KEY'}` | file_name: `{item.get('file_name') or 'N/A'}` | "
            f"drive_file_id: `{item.get('drive_file_id') or 'N/A'}`"
        ),
    )
    lines.extend(supabase_only_lines or ["- Sin registros solo en Supabase"])
    lines.extend(["", "## Posibles duplicados", ""])

    duplicate_lines = preview_lines(
        classifications["possible_duplicates"],
        lambda item: (
            f"- `{item.get('document_key')}` | archivos: "
            + ", ".join(f"`{drive_item.get('name')}`" for drive_item in item.get("files", [])[:5])
        ),
    )
    lines.extend(duplicate_lines or ["- Sin duplicados probables"])
    lines.extend(["", "## Revision manual", ""])

    manual_lines = preview_lines(
        classifications["needs_manual_review"],
        lambda item: (
            f"- `{item['drive'].get('name')}` | razon: {item.get('reason')}"
        ),
    )
    lines.extend(manual_lines or ["- Sin casos ambiguos detectados"])
    lines.extend(["", "## Recomendaciones de siguiente fase", ""])
    lines.extend([
        "- Consolidar una tabla de decision por `document_key` antes de mover cualquier archivo.",
        "- Resolver primero `possible_duplicates` y `needs_manual_review` para evitar migraciones incorrectas.",
        "- Definir una estrategia de manifiestos por carpeta destino antes de copiar o reubicar archivos.",
        "- Ejecutar esta auditoria de nuevo inmediatamente antes de la migracion para trabajar con un inventario actualizado.",
    ])
    lines.append("")
    return "\n".join(lines)


def write_reports(report: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    JSON_REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    MARKDOWN_REPORT_PATH.write_text(
        render_markdown_report(report),
        encoding="utf-8",
    )


def main():
    load_env()

    root_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not root_folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID")

    logger.info("Iniciando auditoria de Drive en modo solo lectura")
    temp_credential_paths: list[Path] = []
    try:
        drive_service, temp_credential_paths = get_drive_service()
        supabase = get_supabase()

        all_files, all_folders, _ = traverse_drive_tree(drive_service, root_folder_id)
        logger.info("Drive auditado | archivos: %s | carpetas: %s", len(all_files), len(all_folders))

        supabase_rows = fetch_supabase_documents(supabase)
        logger.info("Supabase auditado | documentos: %s", len(supabase_rows))

        classifications = classify_audit(all_files, supabase_rows)
        report = make_json_report(root_folder_id, all_files, all_folders, classifications)
        write_reports(report)

        logger.info(
            "Reportes generados | json: %s | markdown: %s",
            JSON_REPORT_PATH,
            MARKDOWN_REPORT_PATH,
        )
    finally:
        for temp_path in temp_credential_paths:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                logger.warning("No se pudo limpiar archivo temporal OAuth: %s", temp_path)


if __name__ == "__main__":
    main()
