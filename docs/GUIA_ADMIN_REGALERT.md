# RegAlert DIGEMID — Guía de administrador

Esta guía es solo para ti. La guía que ven los usuarios está en
`GUIA_USUARIOS_REGALERT.md`.

---

## Comandos de administrador

| Comando | Qué hace |
|---|---|
| `/activar chat_id [nivel dias metodo_pago]` | Activa un plan a mano. Sin nivel/días, muestra botones rápidos. Ej: `/activar 123456789 basico 30 yape`. |
| `/desactivar chat_id` | Cancela la suscripción de alguien. |
| `/usuarios` | Resumen: total de usuarios, por estado/nivel, pendientes de pago. |
| `/membresias` | Lista completa de suscripciones con fecha de inicio y fin. |
| `/directorio` | Lista a todos por estado (plan activo, prueba activa, sin continuar, nunca empezó) con botón "📣 Recordar" para cada uno que no continuó. |
| `/ingresos` | Ingresos del mes actual, desglosados por plan. |
| `/invitar telefono [nombre]` | Genera un enlace de invitación (WhatsApp + Telegram) para alguien nuevo. |
| `/renombrar chat_id nombre` | Cambia el nombre mostrado de un usuario. |

Estos comandos solo funcionan si tu chat_id está en el secret
`ADMIN_CHAT_IDS` del Edge Function.

---

## Cómo llega un usuario nuevo

### 1. Invitación directa — `/invitar telefono nombre`

Genera un link de WhatsApp (con el mensaje ya armado) y el link directo de
Telegram. Cuando la persona toca "Iniciar", el bot la registra sola y te
avisa con su chat_id.

### 2. Landing page → prueba gratuita → pago

Los botones de la landing abren `t.me/<bot>?start=plan_basico` (o
`plan_consultoria`, `plan_empresarial`). Eso activa el flujo de prueba
gratuita (14 días o 3 alertas) descrito en la guía de usuarios. Al terminar
la prueba, o si el usuario elige pagar directo, se dispara el flujo de pago.

---

## Flujo de pago por Yape — cómo funciona el antifraude

1. El usuario elige un plan (`/suscribirme` o botón `plan:nivel`). El bot le
   da tu número de Yape (secrets `YAPE_NUMERO` / `YAPE_TITULAR`) y le pide
   reportar el pago con `/pague codigo_de_operacion`.
2. Ese código se guarda en la tabla `digemid_pagos_yape` con una
   restricción **única** en `codigo_operacion`. Si alguien reusa o comparte
   un código ya registrado, el bot lo rechaza solo, sin que tengas que
   revisarlo, y te avisa del intento.
3. A ti te llega el pago reportado con botones "✅ Confirmar pago" /
   "❌ Rechazar". **Antes de confirmar, verifica en tu propio Yape** que el
   código y el monto coincidan con una transacción real — eso es lo único
   que la base de datos no puede verificar por sí sola.
4. Al confirmar, se activa la suscripción automáticamente (sin escribir
   `/activar` a mano).

Si los secrets `YAPE_NUMERO`/`YAPE_TITULAR` no están configurados, el bot
cae al flujo anterior: te avisa y activas tú mismo con `/activar`.

---

## Seguimiento de usuarios y recordatorios — `/directorio`

`/directorio` agrupa a todos los que han usado el bot en:

- ✅ Con plan activo
- 🎁 En prueba gratuita activa
- ⏳ Sin continuar (prueba terminada sin suscribirse, plan vencido/cancelado,
  o pidieron un plan pero no completaron el pago) — estos traen un botón
  **"📣 Recordar a &lt;nombre&gt;"**
- 💤 Nunca empezaron prueba ni plan

Al tocar "Recordar", el bot le manda directo a esa persona un mensaje
amable invitándola a seguir, con los botones de planes (el mismo flujo de
pago por Yape de arriba). No hace falta escribir nada más.

---

## Cómo se identifica a cada usuario — `/registrarme` y `/miperfil`

Por defecto el nombre que ves es el nombre de Telegram de la persona (puede
cambiar cuando quiera). Cualquier usuario puede fijar su propio nombre de
cuenta con `/registrarme Su Nombre` — ese nombre queda guardado y es el que
debería usar cuando te escriba por temas de su cuenta o membresía. Puede
consultarlo en cualquier momento con `/miperfil`, junto con el estado de su
prueba o plan. Tú también puedes fijarlo por él con `/renombrar chat_id
nombre`.

---

## Prueba gratuita: no se puede reiniciar desde la misma cuenta

Una vez que alguien usa su prueba gratuita (activa o ya finalizada), tocar
"Empezar prueba gratuita" de nuevo ya no la reinicia — el bot le muestra los
planes en su lugar. Esto evita que la misma cuenta de Telegram reclame la
prueba dos veces.

Ojo con el límite real: Telegram no le da al bot el número de teléfono, IP
ni ningún dato del dispositivo de nadie (por diseño, es una restricción de
la plataforma, no algo que se pueda evitar desde el código). Alguien
decidido podría crear una cuenta de Telegram nueva —lo cual sí exige un
número de teléfono nuevo y funcional, no es gratis ni instantáneo— y
reclamar otra prueba gratuita con esa cuenta distinta. No hay forma de
bloquear eso desde el bot. Para la mayoría de usuarios reales el costo de
conseguir un número nuevo ya es suficiente freno; si en el futuro se vuelve
un problema real, la única solución confiable sería pedir verificación por
teléfono (compartir contacto) antes de activar la prueba, lo cual añade
fricción al registro.

---

## Dónde viven los secrets

- **Supabase → Edge Functions → Secrets**: `TELEGRAM_BOT_TOKEN`,
  `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `ADMIN_CHAT_IDS`,
  `YAPE_NUMERO`, `YAPE_TITULAR`, `DEEPSEEK_API_KEY`/`ANTHROPIC_API_KEY`.
- **GitHub → Settings → Secrets and variables → Actions**:
  `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (grupo), `TELEGRAM_ADMIN_CHAT_ID`
  (tu chat personal), `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`.

Nunca se piden ni se pegan estos valores en el chat de Claude — siempre se
configuran directo en Supabase/GitHub.

---

## Workflows de GitHub Actions (todos en `.github/workflows/`)

| Workflow | Qué hace | Cuándo corre |
|---|---|---|
| `digemid-monitor.yml` | Revisa DIGEMID y registra alertas nuevas, las manda al grupo y por DM a suscriptores/pruebas activas. | 3 veces al día, días hábiles |
| `digemid-normativa-text-simple.yml` | Extrae texto de normas con alta fidelidad (OCR incluido) y respalda el PDF. | Diario |
| `digemid-finalizar-pruebas.yml` | Cierra pruebas gratuitas vencidas por tiempo (14 días). | Diario |
| `digemid-recordatorio-planes.yml` | Recuerda planes a invitados que no se han suscrito. | Programado |
| `deploy-supabase-functions.yml` | Despliega el bot (Edge Function) a Supabase. | Manual, después de cada cambio en `supabase/functions/` |
| `set-telegram-webhook.yml` | Registra el webhook de Telegram apuntando al Edge Function. | Manual, una vez por bot/token |
| `set-telegram-bot-profile.yml` | Configura descripción y menú de comandos nativo del bot. | Manual, una vez por bot |
| `send-telegram-document.yml` | Envía un archivo del repo (ej. estas guías) a tu Telegram. | Manual |

---

## Nota sobre el nombre del bot

BotFather no permite cambiar el `@username` de un bot ya creado (solo su
nombre para mostrar). Si el username actual no transmite confianza, la
única forma de cambiarlo es crear un bot nuevo con `/newbot` y migrar
`TELEGRAM_BOT_TOKEN` + el webhook. El chat_id de cada usuario no cambia
entre bots (es su ID de Telegram), así que las suscripciones/pruebas ya
registradas siguen siendo válidas apenas el usuario le escriba al bot nuevo.
