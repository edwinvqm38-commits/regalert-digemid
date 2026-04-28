# RegAlert DIGEMID - Monitor Python

Sistema ligero para monitorear alertas DIGEMID usando Python, Supabase, Telegram y GitHub Actions.

## Fase 1

La Fase 1 realiza:

1. Consulta la página de alertas/modificaciones de DIGEMID.
2. Detecta documentos publicados.
3. Registra documentos nuevos en Supabase usando document_key.
4. Envía notificación por Telegram solo si existen novedades.
5. Se ejecuta localmente o por horario mediante GitHub Actions.

## Variables requeridas

Para ejecución local, crear un archivo .env con:

SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

En GitHub, estas variables deben configurarse como Repository Secrets.

## Ejecución local

python -m pip install -r requirements.txt
python main.py

## Seguridad

No subir el archivo .env al repositorio.
