#!/usr/bin/env bash
# Prepara una COPIA de la funcion telegram-bot llamada telegram-bot-ensayo para
# probar cambios en el mismo proyecto de Supabase, sin tocar la funcion real.
#
# Los secrets de Supabase son del PROYECTO, no de cada funcion. Por eso la copia
# lee variables con sufijo _ENSAYO (token del bot, secreto del webhook y claves
# de IA). Si no defines las claves de IA _ENSAYO, la copia no llama a ningun
# modelo y no genera gasto.
#
# Uso (desde la raiz del repositorio):
#   bash scripts/preparar_funcion_ensayo.sh
# Luego sigue las instrucciones que imprime.

set -euo pipefail

ORIGEN="supabase/functions/telegram-bot"
DESTINO="supabase/functions/telegram-bot-ensayo"

if [ ! -d "$ORIGEN" ]; then
  echo "No encuentro $ORIGEN. Ejecuta el script desde la raiz del repositorio." >&2
  exit 1
fi

rm -rf "$DESTINO"
cp -R "$ORIGEN" "$DESTINO"

VARIABLES="TELEGRAM_BOT_TOKEN TELEGRAM_WEBHOOK_SECRET DEEPSEEK_API_KEY GEMINI_API_KEY"
for variable in $VARIABLES; do
  for archivo in "$DESTINO"/*.ts; do
    sed -i "s/Deno.env.get(\"${variable}\")/Deno.env.get(\"${variable}_ENSAYO\")/g" "$archivo"
  done
done

echo "Copia creada en $DESTINO"
echo "Variables renombradas: $(for v in $VARIABLES; do printf '%s_ENSAYO ' "$v"; done)"
cat <<'MENSAJE'

Siguientes pasos (los ejecutas tu, con tus propias cuentas):

1. En el panel de Supabase (proyecto DIGEMID_BOT) > Edge Functions > Secrets,
   agrega: TELEGRAM_BOT_TOKEN_ENSAYO (token del bot de prueba de BotFather) y
   TELEGRAM_WEBHOOK_SECRET_ENSAYO (una clave inventada por ti, solo letras,
   numeros, guion y guion bajo). NO agregues DEEPSEEK_API_KEY_ENSAYO ni
   GEMINI_API_KEY_ENSAYO si no quieres gasto de IA.

2. Despliega la copia:
   supabase functions deploy telegram-bot-ensayo --project-ref <ref-de-DIGEMID_BOT> --use-api --no-verify-jwt

3. Registra el webhook del bot de prueba (reemplaza los valores entre < >):
   curl -X POST "https://api.telegram.org/bot<TOKEN_DE_ENSAYO>/setWebhook" \
     -H "Content-Type: application/json" \
     --data '{"url":"https://<ref-de-DIGEMID_BOT>.supabase.co/functions/v1/telegram-bot-ensayo","secret_token":"<SECRETO_ENSAYO>"}'

4. Escribele /start al bot de prueba y sigue docs/PRUEBA_FUNCION_ENSAYO.md.

OJO: la base de datos es la misma que la real. Los usuarios que crees al probar
quedaran en digemid_bot_usuarios y, con la regla nueva, recibiran las alertas
reales. Al terminar, borra tus usuarios de prueba (ver la guia).
MENSAJE
