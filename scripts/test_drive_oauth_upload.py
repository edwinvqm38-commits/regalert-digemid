import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = ["https://www.googleapis.com/auth/drive"]


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
            flow = InstalledAppFlow.from_client_secrets_file(
                str(client_json_path),
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return creds


def get_drive_service():
    creds = get_credentials()
    return build("drive", "v3", credentials=creds)


def upload_test_file():
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not folder_id:
        raise ValueError("Falta GOOGLE_DRIVE_FOLDER_ID en .env")

    service = get_drive_service()

    temp_dir = Path("tmp")
    temp_dir.mkdir(exist_ok=True)

    file_path = temp_dir / "regalert_drive_oauth_test.txt"
    file_path.write_text(
        f"Prueba OAuth RegAlert DIGEMID - {datetime.now().isoformat()}",
        encoding="utf-8",
    )

    metadata = {
        "name": "regalert_drive_oauth_test.txt",
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

    print("✅ Archivo subido correctamente con OAuth")
    print("ID:", uploaded.get("id"))
    print("Nombre:", uploaded.get("name"))
    print("Ver en Drive:", uploaded.get("webViewLink"))
    print("Descarga:", uploaded.get("webContentLink"))


if __name__ == "__main__":
    load_env()
    upload_test_file()

