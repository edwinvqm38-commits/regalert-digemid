from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]

BASE_DIR = Path(__file__).resolve().parents[1]
CLIENT_PATH = BASE_DIR / ".secrets" / "google_oauth_client.json"
TOKEN_PATH = BASE_DIR / ".secrets" / "google_oauth_token.json"

if not CLIENT_PATH.exists():
    raise FileNotFoundError(f"No existe client OAuth: {CLIENT_PATH}")

flow = InstalledAppFlow.from_client_secrets_file(
    str(CLIENT_PATH),
    scopes=SCOPES,
)

creds = flow.run_local_server(
    port=0,
    access_type="offline",
    prompt="consent",
    include_granted_scopes="false",
)

TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

print("Token OAuth generado correctamente en:")
print(TOKEN_PATH)
print("Scopes:")
for scope in creds.scopes or []:
    print("-", scope)
