import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


def load_env():
    load_dotenv()
    load_dotenv(Path.cwd().parent / ".env", override=False)


def get_drive_service():
    json_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")

    if not json_path:
        raise ValueError("Falta GOOGLE_SERVICE_ACCOUNT_JSON_PATH en .env")

    if not Path(json_path).exists():
        raise FileNotFoundError(f"No existe el JSON: {json_path}")

    scopes = ["https://www.googleapis.com/auth/drive.file"]

    credentials = service_account.Credentials.from_service_account_file(
        json_path,
        scopes=scopes,
    )

    return build("drive", "v3", credentials=credentials)


def upload_test_file():
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID en .env")

    service = get_drive_service()

    temp_dir = Path("tmp")
    temp_dir.mkdir(exist_ok=True)

    file_path = temp_dir / "regalert_drive_test.txt"
    file_path.write_text(
        f"Prueba de subida RegAlert DIGEMID - {datetime.now().isoformat()}",
        encoding="utf-8",
    )

    metadata = {
        "name": "regalert_drive_test.txt",
        "parents": [folder_id],
    }

    media = MediaFileUpload(
        str(file_path),
        mimetype="text/plain",
        resumable=False,
    )

    uploaded = (
        service.files()
        .create(
            body=metadata,
            media_body=media,
            fields="id, name, webViewLink, webContentLink",
        )
        .execute()
    )

    print("✅ Archivo subido correctamente")
    print("ID:", uploaded.get("id"))
    print("Nombre:", uploaded.get("name"))
    print("Ver en Drive:", uploaded.get("webViewLink"))
    print("Descarga:", uploaded.get("webContentLink"))


if __name__ == "__main__":
    load_env()
    upload_test_file()