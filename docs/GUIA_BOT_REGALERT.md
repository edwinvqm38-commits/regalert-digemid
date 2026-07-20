# RegAlert DIGEMID — Guía rápida

## Qué hace el bot

Vigila las alertas de DIGEMID (medicamentos falsificados, retiros, etc.) y las
normas/reglamentos, y te avisa por Telegram. Cualquier usuario puede preguntar
en lenguaje natural con `/consulta` y recibe una respuesta con la fuente
exacta (documento, fecha y página).

El bot real es **@Leer_correos_gmail_bot** (el nombre que se ve arriba del
chat es "RegAlertPeru_bot", pero el `@usuario` técnico —el que va en los
links `t.me/...`— no se puede cambiar una vez creado el bot en BotFather).

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
| `/suscribirme nivel` | Pide un plan pagado (`basico`, `consultoria`, `empresarial`) y te da el número de Yape para pagar. |
| `/pague codigo_de_operacion` | Reporta el código de operación de tu Yape luego de pagar un plan. |
| `/chatid` | Muestra su propio chat_id (útil si necesitas activarlo manualmente). |

El plan **gratis** tiene un límite diario de consultas con IA (`/consulta`).
Al llegar al límite, el bot le muestra los planes pagados con botones.

---

## Comandos solo para ti (administrador)

| Comando | Qué hace |
|---|---|
| `/activar chat_id [nivel dias metodo_pago]` | Activa un plan a mano. Sin nivel/días, te muestra botones rápidos. Ej: `/activar 123456789 basico 30 yape`. |
| `/desactivar chat_id` | Cancela la suscripción de alguien. |
| `/usuarios` | Resumen: total de usuarios, por estado/nivel, pendientes de pago. |
| `/membresias` | Lista completa de suscripciones con fecha de inicio y fin. |
| `/ingresos` | Ingresos del mes actual, desglosados por plan. |
| `/invitar telefono [nombre]` | Genera un enlace de invitación para alguien nuevo (ver abajo). |
| `/renombrar chat_id nombre` | Cambia el nombre mostrado de un usuario (útil si lo agregaste sin nombre). |

Precios actuales: Básico S/29, Consultoría S/79, Empresarial S/199 (por mes).

---

## Cómo llega un usuario nuevo — 2 caminos

### 1. Tú lo invitas directo (cuando ya tienes su teléfono)

```
/invitar +51987654321 Juan Perez
```

El bot te responde con **dos enlaces listos para copiar y pegar**: uno de
WhatsApp con el mensaje ya armado, y el link directo de Telegram. Cuando la
persona toca "Iniciar", el bot la registra automáticamente y **te avisa a
ti** con su chat_id.

### 2. Landing page → prueba gratis → suscripción con Yape

Los botones de planes del landing page
(`https://claude.ai/code/artifact/6bbb65d3-2e06-4731-8e05-db73a7de1e70`)
abren Telegram con `https://t.me/Leer_correos_gmail_bot?start=plan_basico`
(o `plan_consultoria`, `plan_empresarial`). Al tocar "Iniciar":

1. El bot **no** pide suscripción de inmediato — ofrece una **prueba
   gratuita** de hasta 14 días o 3 alertas (lo que llegue primero), con un
   botón "🎁 Empezar prueba gratuita" (y otro por si ya quiere pagar ya).
2. Durante la prueba, el usuario recibe las alertas nuevas **directo a su
   chat** (antes esto solo se mandaba al grupo) y tiene sus 5 consultas/día
   de IA, igual que el plan gratis normal.
3. Al recibir la 3ra alerta (o a los 14 días, por un job diario) el bot le
   dice que la prueba terminó y le muestra los 3 botones de plan.
4. Al elegir un plan, el bot le da tu número de Yape y le pide reportar el
   pago con `/pague codigo_de_operacion`.
5. Ese código se guarda con una restricción **única** en la base de datos:
   si alguien reusa o comparte un código ya registrado, el bot lo rechaza
   solo, sin que tengas que revisarlo.
6. A ti (admin) te llega el pago reportado con botones "✅ Confirmar" /
   "❌ Rechazar". Al confirmar, se activa el plan automáticamente — sin
   escribir `/activar` a mano.

**Requisito**: los secrets `YAPE_NUMERO` y `YAPE_TITULAR` deben estar
configurados en el Edge Function de Supabase; si faltan, el bot cae al
flujo anterior (te avisa y activas tú a mano con `/activar`).

---

## Flujo recomendado del día a día

1. Compartes el link del landing page (o el enlace de `/invitar`) en redes,
   grupos de químicos farmacéuticos, WhatsApp, etc.
2. La persona prueba gratis y, cuando se le acaba la prueba, elige un plan.
3. Te llega el pago reportado con el código de operación — lo verificas en
   tu propio Yape y tocas "✅ Confirmar".
4. Si en algún momento olvidaste ponerle nombre a alguien, usa
   `/renombrar chat_id "Nombre Apellido"`.
5. Para ver cómo va el negocio: `/usuarios`, `/membresias`, `/ingresos`.
