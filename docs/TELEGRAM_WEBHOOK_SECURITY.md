# Autenticación y despliegue del webhook de Telegram

La Edge Function `telegram-bot` conserva `--no-verify-jwt` porque Telegram no
envía un JWT de Supabase. Cada `POST` exige en su lugar el secreto compartido
que Telegram entrega en `X-Telegram-Bot-Api-Secret-Token`.

## Fuente de verdad

El repository secret de GitHub `TELEGRAM_WEBHOOK_SECRET` es la única fuente
operativa. Debe tener entre 1 y 256 caracteres y usar solamente `A-Z`, `a-z`,
`0-9`, `_` y `-`. Por ejemplo, `openssl rand -hex 32` genera un valor válido.

El workflow `Prepare Telegram Webhook` sincroniza ese valor en Supabase y usa
la misma variable para llamar a `setWebhook`. Nunca se debe guardar el valor en
el repositorio, imprimirlo ni mantener una segunda copia manual divergente.

## Secuencia coordinada

`Deploy Supabase Functions` aplica este orden obligatorio:

```text
TELEGRAM_WEBHOOK_SECRET de GitHub
              │
              ▼
Sincronizar secreto en Supabase
              │
              ▼
Capturar allowed_updates, max_connections y errores previos
              │
              ▼
setWebhook(url, secret_token, parámetros conservados)
              │
              ▼
Verificar respuesta, URL, parámetros y errores nuevos
              │
              ▼
Desplegar telegram-bot --no-verify-jwt
```

El job de despliegue declara `needs: prepare-telegram`. Si falla la validación,
la sincronización, `setWebhook` o `getWebhookInfo`, `telegram-bot` no se
despliega. `whatsapp-bot` se despliega en un job independiente y no queda
bloqueado por la preparación de Telegram.

El payload nunca incluye `drop_pending_updates`, por lo que no solicita
descartar entregas. Conserva `allowed_updates` y `max_connections` cuando
Telegram los reporta antes de la operación.

## Primera activación

1. Crear estos repository secrets en GitHub:
   `SUPABASE_ACCESS_TOKEN`, `SUPABASE_PROJECT_REF`, `TELEGRAM_BOT_TOKEN` y
   `TELEGRAM_WEBHOOK_SECRET`.
2. Integrar el código únicamente cuando los cuatro secrets estén disponibles.
3. Ejecutar `Deploy Supabase Functions`, o permitir su ejecución desde `main`.
4. El workflow sincroniza primero el secreto. No es necesario configurarlo
   además a mano en Supabase.
5. La versión anterior sigue atendiendo mientras se prepara el webhook: el
   encabezado nuevo es compatible con una versión que todavía no lo valida.
6. Solo después de verificar Telegram se despliega la versión protegida.
7. Enviar un mensaje real al bot y confirmar que se procesa correctamente.
   Esta entrega es la verificación definitiva del secreto compartido, porque
   `getWebhookInfo` no revela `secret_token`.

No ejecutar primero un despliegue manual de `telegram-bot`: omitiría la barrera
de preparación y podría dejar todas las entregas en HTTP 401 o 503.

## Actualizaciones futuras

Las actualizaciones normales reutilizan el mismo repository secret y recorren
la secuencia completa antes de cada despliegue. Si la preparación o el deploy
fallan, la versión anterior continúa usando el mismo secreto.

La rotación del valor es una operación distinta: Supabase y Telegram no ofrecen
un cambio atómico entre ambos servicios. Debe programarse como mantenimiento
controlado y no mezclarse con una actualización ordinaria. Hasta implementar
aceptación transitoria de dos secretos, puede existir una ventana breve de 401
durante una rotación.

## Verificación de Telegram

Antes de `setWebhook`, el workflow guarda:

- URL y parámetros actuales.
- `last_error_date` histórico.
- Número de updates pendientes, solo con fines diagnósticos.

Después comprueba `ok=true`, la URL exacta, la conservación de
`allowed_updates` y `max_connections`, y que no aparezca un error fechado desde
el inicio de la operación. Un error histórico sin fecha nueva no bloquea. El
número de pendientes puede bajar por entregas normales y no demuestra descarte;
la garantía relevante es que el payload no contiene `drop_pending_updates`.

## Respuestas de la función

- `POST` autenticado: continúa al procesamiento normal.
- Header ausente o incorrecto: HTTP 401.
- Secreto ausente o inválido en Supabase: HTTP 503.
- `GET`: health check público.
- Otros métodos, incluido `OPTIONS`: HTTP 405. El endpoint no tenía CORS y no
  se agregan encabezados CORS.

Nunca registrar el secreto configurado ni el encabezado recibido.
