# Canal WhatsApp Business — MVP inbound-only

Segundo canal de consulta de DIGEMID RegAlert, sobre la **WhatsApp Business
Platform (Cloud API) de Meta**. Telegram sigue siendo el canal principal y no
se modifica: WhatsApp corre en paralelo y aislado.

---

## 1. Arquitectura

```
Usuario (WhatsApp)
      │  escribe un mensaje
      ▼
Meta Cloud API ──POST──► supabase/functions/whatsapp-bot   (Edge Function, Deno)
                             │
                             │ 1. valida firma X-Hub-Signature-256
                             │ 2. descarta statuses / eventos no consultables
                             │ 3. idempotencia por message_id
                             │ 4. registra usuario y parsea el comando
                             ▼
                    supabase/functions/_shared/     ← misma base que Telegram
                      ├── digemid-datos.ts   (alertas y normativa)
                      ├── consulta-ia.ts     (RAG: buscar_paginas_texto → DeepSeek/Gemini)
                      └── fechas-lima.ts
                             │
                             ▼
                    respuesta ──► sendWhatsAppServiceMessage() ──► Meta ──► Usuario
```

### Archivos

| Archivo | Rol |
|---|---|
| `supabase/functions/whatsapp-bot/index.ts` | Webhook (GET/POST) y orquestación |
| `supabase/functions/whatsapp-bot/webhook.ts` | Verificación GET, firma HMAC, parseo de eventos |
| `supabase/functions/whatsapp-bot/whatsapp-api.ts` | Cliente Cloud API con la guardia inbound-only |
| `supabase/functions/whatsapp-bot/comandos.ts` | Normalización de comandos (con/sin `/`, opciones 1-7) |
| `supabase/functions/whatsapp-bot/formato.ts` | Adaptador de formato WhatsApp (`*negrita*`, no HTML) |
| `supabase/functions/whatsapp-bot/persistencia.ts` | Idempotencia, usuarios y log |
| `supabase/functions/_shared/*` | Datos, RAG y fechas — independientes del canal |
| `supabase/sql/2026_09_12_create_whatsapp_mvp.sql` | Tablas del canal |

### Por qué tablas propias y no `digemid_bot_usuarios`

`agents/agent_notify.py` (`_usuarios_elegibles_dm`) y
`scripts/enviar_recordatorio_planes.py` recorren `digemid_bot_usuarios` y
`digemid_suscripciones` y **envían mensajes por Telegram a cada
`telegram_chat_id` que encuentran**, sin filtrar por canal.

Registrar ahí a un usuario de WhatsApp lo habría convertido en destinatario
de envíos proactivos — justo lo que este canal no debe hacer. Con
`digemid_whatsapp_usuarios` separada, el aislamiento es **estructural**:
ningún proceso proactivo existente conoce esas filas.

El log **sí** se comparte: `digemid_bot_consultas` con
`telegram_chat_id = 'wa:<wa_id>'`. Esa tabla solo registra y cuenta (nadie
envía mensajes a partir de ella), y compartirla mantiene la trazabilidad y
los límites diarios en un solo lugar.

---

## 2. Variables requeridas

Se configuran como **secrets de la Edge Function** en Supabase
(*Project Settings → Edge Functions → Secrets*). Nunca se commitean valores
reales: `.env.example` solo lleva placeholders.

| Variable | De dónde sale |
|---|---|
| `WHATSAPP_ACCESS_TOKEN` | Meta → System User token permanente (ver §5) |
| `WHATSAPP_PHONE_NUMBER_ID` | Meta → WhatsApp → API Setup → *Phone number ID* |
| `WHATSAPP_WABA_ID` | Meta → WhatsApp → API Setup → *WhatsApp Business Account ID* |
| `WHATSAPP_VERIFY_TOKEN` | Cadena que inventas tú; la misma que pongas en Meta |
| `WHATSAPP_APP_SECRET` | Meta → App Settings → Basic → *App Secret* |
| `WHATSAPP_GRAPH_API_VERSION` | Opcional; por defecto `v21.0` |
| `WHATSAPP_OUTBOUND_MODE` | Opcional; `inbound_only` (valor asumido si falta) |

Además reutiliza las que ya usa el proyecto: `SUPABASE_URL`,
`SUPABASE_SERVICE_ROLE_KEY`, `DEEPSEEK_API_KEY`, `GEMINI_API_KEY`.

---

## 3. Flujo inbound-only

**Regla:** usuario escribe → RegAlert procesa → RegAlert responde.
El sistema **nunca** inicia una conversación.

Esto no depende solo de una variable de entorno; está garantizado por el
diseño del código:

1. `sendWhatsAppServiceMessage()` **no recibe un número de destino**. Recibe
   un `ContextoInbound` y responde al `wa_id` que viene dentro.
2. Un `ContextoInbound` solo se crea con `crearContextoInbound()`, que exige
   un mensaje entrante con `id` y `from` reales. El tipo lleva una marca
   (`unique symbol`) que impide fabricarlo con un objeto literal.
3. No existe ninguna función que acepte una lista de destinatarios, ni
   soporte de *message templates*. Un cron o workflow que quisiera hacer un
   broadcast no tiene por dónde.
4. `WHATSAPP_OUTBOUND_MODE` actúa como cerrojo adicional: cualquier valor
   distinto de `inbound_only` hace fallar el envío.
5. Los usuarios de WhatsApp viven en una tabla que los procesos proactivos
   de Telegram no leen.

Esto importa por costo y por política de Meta: los mensajes iniciados por el
negocio requieren plantillas aprobadas y se cobran por conversación; las
respuestas dentro de la ventana de servicio de 24 h, no.

---

## 4. Cómo verificar el webhook

Meta llama una sola vez con `GET`:

```
GET /whatsapp-bot?hub.mode=subscribe&hub.verify_token=<tu token>&hub.challenge=<número>
```

La función compara contra `WHATSAPP_VERIFY_TOKEN` y devuelve el `challenge`
en texto plano si coincide, o `403` si no. El token nunca aparece en logs.

Cada `POST` posterior se valida con `X-Hub-Signature-256` (HMAC SHA-256 del
cuerpo crudo con `WHATSAPP_APP_SECRET`). Firma inválida → `401`, sin
procesar el mensaje.

---

## 5. Configuración pendiente en Meta

> Requiere una cuenta de Meta Business. La app de WhatsApp Business del
> celular **no** sirve por sí sola: un número registrado en la Cloud API deja
> de poder usarse en esa app, así que lo habitual es usar un número nuevo
> para el bot.

1. **Meta Business Manager** (<https://business.facebook.com>) → crear/usar
   el negocio y completar la **verificación de negocio**.
2. **Meta for Developers** (<https://developers.facebook.com>) → crear una
   app tipo *Business* → agregar el producto **WhatsApp**.
3. En *WhatsApp → API Setup*: registrar el **número de teléfono** y anotar
   `Phone number ID` y `WhatsApp Business Account ID` (WABA).
4. **Token permanente**: *Business Settings → Users → System Users* → crear
   un system user, asignarle la app y el WABA, y generar un token con los
   permisos `whatsapp_business_messaging` y `whatsapp_business_management`.
   (El token de prueba de la pantalla de API Setup **caduca en 24 h**: sirve
   para probar, no para producción.)
5. **App Secret**: *App Settings → Basic → App Secret*.
6. **Webhook**: *WhatsApp → Configuration → Webhook → Edit*:
   - Callback URL: `https://<PROJECT_REF>.supabase.co/functions/v1/whatsapp-bot`
   - Verify token: el mismo valor de `WHATSAPP_VERIFY_TOKEN`
   - Suscribirse al campo **`messages`** (y solo a ese: el MVP no necesita
     `message_template_status_update` ni otros).

---

## 6. URL esperada de la Edge Function

```
https://<SUPABASE_PROJECT_REF>.supabase.co/functions/v1/whatsapp-bot
```

Debe desplegarse con `--no-verify-jwt`: Meta llama sin JWT de Supabase. La
autenticación real del webhook es la firma HMAC, no el JWT.

---

## 7. Cómo desplegar

Automático al hacer merge a `main` de cambios bajo `supabase/functions/**`
(workflow *Deploy Supabase Functions*), que ahora despliega ambos canales en
pasos separados.

Manual:

```bash
supabase functions deploy whatsapp-bot --project-ref "$SUPABASE_PROJECT_REF" --use-api --no-verify-jwt
```

Antes del primer despliegue hay que aplicar la migración
`supabase/sql/2026_09_12_create_whatsapp_mvp.sql` (SQL Editor de Supabase).

---

## 8. Cómo probar

```bash
# Suite del canal (no llama a Meta: fetch está mockeado)
deno test --allow-net supabase/functions/whatsapp-bot/ supabase/functions/_shared/

# Tipos y estilo
deno lint supabase/functions/whatsapp-bot/ supabase/functions/_shared/
```

Prueba de humo end-to-end, una vez configurado Meta: escribirle `hola` al
número del bot desde otro teléfono. Debe responder el menú. Después
`ultimas`, `buscar paracetamol` y `consulta qué establece la Ley 29459`.

Comandos aceptados (con `/` o sin él, y por número de opción 1-7): `menu`,
`ayuda`, `ultimas`, `hoy`, `semana`, `mes`, `recientes`, `buscar`, `normas`,
`consulta`, `detalle`, `miperfil`.

---

## 9. Cómo hacer rollback

El canal es **aditivo**: quitarlo no afecta a Telegram.

1. **Inmediato, sin desplegar nada:** en Meta → *WhatsApp → Configuration →
   Webhook*, desuscribir el campo `messages`. Dejan de llegar eventos y el
   bot deja de responder.
2. **Desactivar la función:** borrarla desde el dashboard de Supabase
   (*Edge Functions → whatsapp-bot → Delete*), o revertir el commit y quitar
   el paso del workflow.
3. Las tablas `digemid_whatsapp_*` pueden quedarse sin efecto alguno sobre
   el resto del sistema (nadie más las lee).

En ningún caso hay que tocar `telegram-bot`.

---

## 10. Limitaciones del MVP

- Solo mensajes de **texto**. Audio, imágenes, ubicación y documentos reciben
  una respuesta explicando que por ahora solo se lee texto.
- Menú de **texto numerado**, no mensajes interactivos (listas/botones) de
  WhatsApp.
- Sin nota de voz (Telegram sí la transcribe).
- Sin comandos administrativos (revisión documental, membresías, reportes,
  activaciones): esos siguen solo en Telegram.
- Sin alta de planes por WhatsApp: todo usuario nuevo entra como `gratis`
  (5 consultas IA/día, igual que en Telegram). Cambiar de nivel hoy requiere
  editar `digemid_whatsapp_usuarios.nivel` a mano.
- La identidad **no está vinculada** entre canales: si la misma persona usa
  Telegram y WhatsApp, son dos usuarios distintos con cuotas separadas.

---

## 11. Qué NO hace todavía (por diseño, no por falta de tiempo)

- No envía alertas DIGEMID automáticas ni notificaciones de nuevas normas.
- No envía recordatorios de pago, vencimiento de membresía ni renovaciones.
- No hace campañas, broadcasts ni marketing.
- No usa message templates (ni existe la función para hacerlo).
- No se dispara desde GitHub Actions ni desde Supabase Cron.

Cualquiera de estas funciones exigiría plantillas aprobadas por Meta y
conversaciones de pago, y es una decisión de negocio, no un pendiente
técnico.

---

## 12. Riesgos pendientes

| Riesgo | Estado / mitigación |
|---|---|
| **No probado contra Meta real.** La suite cubre firma, idempotencia, parseo y formato con dobles, pero nadie ha enviado aún un mensaje real. | Hacer la prueba de humo de §8 antes de dar el canal por operativo. |
| **Duplicación con `telegram-bot`.** Las consultas y el RAG se reescribieron en `_shared/` en vez de extraerlos del monolito de 5 482 líneas. Un cambio de lógica hay que hacerlo en dos sitios. | Decisión consciente: extraer del monolito tocaba ~40 puntos del canal en producción. `_shared/` queda listo para que `telegram-bot` lo importe en una fase posterior. |
| **Costo por conversación.** Aunque no haya mensajes proactivos, las respuestas abren ventanas de servicio que Meta puede tarificar según su política vigente. | Monitorear el consumo en Meta durante las primeras semanas. |
| **Ventana de 24 h.** Si el usuario escribe y la respuesta demora más de 24 h (p. ej. reproceso manual), el envío fallaría. | No aplica hoy: todas las respuestas son síncronas. |
| **Límite de cuota por número nuevo.** Meta limita mensajes/día a números sin verificar (tier inicial). | Verificar el negocio en Meta para subir de tier. |
| **`nivel` manual.** No hay flujo de alta de plan por WhatsApp. | Pendiente de decisión de negocio. |
| **Sin RLS explícita en las tablas nuevas.** Se accede solo con `service_role` desde la Edge Function, igual que el resto del proyecto. | Consistente con las tablas existentes; revisar si se expone un cliente público. |
