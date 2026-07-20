# RegAlert DIGEMID — Guía rápida

## Qué hace el bot

Vigila las alertas de DIGEMID (medicamentos falsificados, retiros, etc.) y las
normas/reglamentos, y te avisa por Telegram. Cualquier usuario puede preguntar
en lenguaje natural con `/consulta` y recibe una respuesta con la fuente
exacta (documento, fecha y página).

---

## Comandos para cualquier usuario

| Comando | Qué hace |
|---|---|
| `/start` | Da la bienvenida y muestra el menú. |
| `/menu` | Menú principal con botones. |
| `/ayuda` | Lista de comandos (esta guía, resumida, dentro del bot). |
| `/ultimas` | Últimas 5 alertas. |
| `/hoy` | Alertas publicadas hoy. |
| `/semana` | Alertas publicadas esta semana. |
| `/mes` | Alertas publicadas este mes. |
| `/recientes` | Alertas registradas recién en el sistema. |
| `/buscar texto` | Busca alertas por palabra clave. Ej: `/buscar retiro`. |
| `/detalle 50-2026` | Detalle de una alerta por número/código. |
| `/consulta pregunta` | Responde en lenguaje natural citando la fuente (norma/alerta + página). Ej: `/consulta que paso con el Opdivo falsificado`. |
| `/suscribirme nivel` | Pide activar un plan pagado (`basico`, `consultoria`, `empresarial`). |
| `/chatid` | Muestra su propio chat_id (útil si necesitas activarlo manualmente). |

El plan **gratis** tiene un límite diario de consultas con IA (`/consulta`).
Al llegar al límite, el bot le muestra los planes pagados con botones.

---

## Comandos solo para ti (administrador)

| Comando | Qué hace |
|---|---|
| `/activar chat_id [nivel dias metodo_pago]` | Activa un plan. Sin nivel/días, te muestra botones rápidos (Básico/Consultoría/Empresarial × 30 días). Ej: `/activar 123456789 basico 30 yape`. |
| `/desactivar chat_id` | Cancela la suscripción de alguien. |
| `/usuarios` | Resumen: total de usuarios, por estado/nivel, pendientes de pago. |
| `/membresias` | Lista completa de suscripciones con fecha de inicio y fin. |
| `/ingresos` | Ingresos del mes actual, desglosados por plan. |
| `/invitar telefono [nombre]` | Genera un enlace de invitación para alguien nuevo (ver abajo). |
| `/renombrar chat_id nombre` | Cambia el nombre mostrado de un usuario (útil si lo agregaste sin nombre). |

Precios actuales: Básico S/29, Consultoría S/79, Empresarial S/199 (por mes).

---

## Cómo se invita a un usuario nuevo — 2 caminos

### 1. Tú lo invitas directo (cuando ya tienes su teléfono)

```
/invitar +51987654321 Juan Perez
```

El bot te responde con **dos enlaces listos para copiar y pegar**:
- Un link de WhatsApp (`wa.me/...`) que ya trae armado el mensaje de
  bienvenida — solo lo envías con un clic.
- El link directo de Telegram (`t.me/RegAlertPeru_bot?start=<código>`), por
  si prefieres mandarlo por otro medio.

Cuando la persona toca "Iniciar" en Telegram, el bot **la registra
automáticamente** (usando el código de invitación) y **a ti te llega un
aviso** ("🆕 Nuevo usuario registrado...") con su chat_id, para que uses
`/activar` si ya pagó, o lo dejes en el plan gratis.

### 2. Landing page (para difundir en redes, WhatsApp, etc.)

Los botones de planes del landing page (`https://claude.ai/code/artifact/6bbb65d3-2e06-4731-8e05-db73a7de1e70`)
no son botones decorativos: cada uno abre Telegram directo con
`https://t.me/RegAlertPeru_bot?start=plan_basico` (o `plan_consultoria`,
`plan_empresarial`).

Eso hace que, en cuanto la persona toca el botón y presiona "Iniciar" en
Telegram, el bot:
1. La registra como usuario nuevo.
2. Genera automáticamente una **solicitud de suscripción** para el plan que
   eligió en la landing page.
3. **Te avisa a ti** con sus datos y el comando exacto para activarla:
   > 💳 Solicitud de suscripción
   > Nombre: ...  Teléfono: ...  chat_id: ...
   > Plan solicitado: basico (S/29/mes)
   > Usa `/activar <chat_id> basico 30` para activarlo.
4. Al usuario le confirma: "Solicitud enviada, en breve te contactamos para
   coordinar el pago."

O sea: **para el usuario es un solo clic** (botón → Telegram → "Iniciar").
Para ti, solo copias el comando `/activar` que el bot ya te arma cuando
confirmes que pagó.

---

## Flujo recomendado del día a día

1. Compartes el link del landing page (o el enlace de `/invitar`) en redes,
   grupos de químicos farmacéuticos, WhatsApp, etc.
2. La persona toca el botón/enlace y presiona "Iniciar" en Telegram.
3. A ti te llega el aviso automático (nuevo usuario o solicitud de plan).
4. Coordinas el pago (Yape/Plin/transferencia) por fuera del bot.
5. Confirmas con `/activar chat_id nivel dias metodo_pago`.
6. Si en algún momento olvidaste ponerle nombre a alguien, usa
   `/renombrar chat_id "Nombre Apellido"`.
7. Para ver cómo va el negocio: `/usuarios`, `/membresias`, `/ingresos`.
