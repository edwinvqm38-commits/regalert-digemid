import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/drive"]


DEFAULT_SAFE_PREFIXES = (
    "regalert_drive_test",
    "regalert_drive_oauth_test",
    "DIGEMID_ALERTA_",
    "ALERTA_",
)


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_credentials():
    client_json_path = os.getenv("GOOGLE_OAUTH_CLIENT_JSON_PATH")
    token_path = os.getenv("GOOGLE_OAUTH_TOKEN_PATH")

    if not client_json_path:
        raise ValueError("Falta GOOGLE_OAUTH_CLIENT_JSON_PATH en .env")

    if not token_path:
        raise ValueError("Falta GOOGLE_OAUTH_TOKEN_PATH en .env")

    client_json_path = Path(client_json_path)
    token_path = Path(token_path)

    if not client_json_path.exists():
        raise FileNotFoundError(f"No existe OAuth client JSON: {client_json_path}")

    creds = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise ValueError(
                "No hay token OAuth válido. Primero ejecuta test_drive_oauth_upload.py"
            )

    return creds


def get_drive_service():
    return build("drive", "v3", credentials=get_credentials())


def list_files(service, folder_id: str) -> list[dict]:
    query = f"'{folder_id}' in parents and trashed = false"

    files = []
    page_token = None

    while True:
        response = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, webViewLink, createdTime)",
                pageSize=100,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return files


def should_trash(file_name: str, all_files: bool) -> bool:
    if all_files:
        return True

    return file_name.startswith(DEFAULT_SAFE_PREFIXES)


def trash_file(service, file_id: str):
    return (
        service.files()
        .update(
            fileId=file_id,
            body={"trashed": True},
            fields="id, name, trashed",
            supportsAllDrives=True,
        )
        .execute()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Mover archivos a papelera")
    parser.add_argument("--all", action="store_true", help="Incluir todos los archivos de la carpeta")
    args = parser.parse_args()

    load_env()

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID en .env")

    service = get_drive_service()
    files = list_files(service, folder_id)

    print("")
    print("📁 Carpeta Drive:", folder_id)
    print("📄 Archivos encontrados:", len(files))
    print("")

    if not files:
        print("No hay archivos para revisar.")
        return

    candidates = []

    for file in files:
        name = file.get("name", "")
        file_id = file.get("id", "")
        link = file.get("webViewLink", "")

        mark = "LIMPIAR" if should_trash(name, args.all) else "OMITIR"

        print(f"[{mark}] {name}")
        print(f"       ID: {file_id}")
        print(f"       Link: {link}")

        if should_trash(name, args.all):
            candidates.append(file)

    print("")
    print("🧹 Candidatos a mover a papelera:", len(candidates))

    if not args.execute:
        print("")
        print("✅ Modo simulación. No se movió nada a papelera.")
        print("Para ejecutar limpieza real:")
        print("python scripts/clean_drive_folder.py --execute")
        print("")
        print("Si quieres limpiar absolutamente todo lo que hay en esa carpeta:")
        print("python scripts/clean_drive_folder.py --execute --all")
        return

    print("")
    print("⚠️ Ejecutando limpieza real...")

    moved = 0

    for file in candidates:
        file_id = file["id"]
        name = file["name"]

        trash_file(service, file_id)
        moved += 1

        print(f"🗑️ Movido a papelera: {name}")

    print("")
    print("✅ Limpieza finalizada.")
    print("Archivos movidos a papelera:", moved)


if __name__ == "__main__":
    main()
