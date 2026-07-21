# RegAlert DIGEMID — Guía para usuarios

## Qué hace el bot

Vigila las alertas sanitarias y la normativa de DIGEMID (Perú) y te avisa por
Telegram. Puedes preguntar en lenguaje natural con `/consulta` y el bot
responde citando la fuente exacta (documento, fecha y página).

---

## Comandos disponibles

| Comando | Qué hace |
|---|---|
| `/start` | Inicia el bot y muestra el menú. |
| `/menu` | Menú principal con botones. |
| `/ayuda` | Lista de comandos dentro del bot. |
| `/ultimas` | Últimas 5 alertas. |
| `/hoy` | Alertas publicadas hoy. |
| `/semana` | Alertas publicadas esta semana. |
| `/mes` | Alertas publicadas este mes. |
| `/recientes` | Alertas registradas recién en el sistema. |
| `/buscar texto` | Busca alertas por palabra clave. Ej: `/buscar retiro`. |
| `/detalle 50-2026` | Detalle de una alerta por número o código. |
| `/consulta pregunta` | Responde citando la fuente. Ej: `/consulta que paso con el Opdivo falsificado`. |
| `/suscribirme nivel` | Pide un plan pagado (`basico`, `consultoria`, `empresarial`) y te da el número de Yape. |
| `/pague codigo_de_operacion` | Reporta tu código de operación de Yape luego de pagar. |
| `/registrarme Tu Nombre` | Registra el nombre con el que quieres identificarte para temas de tu cuenta o membresía. |
| `/miperfil` | Muestra tu nombre registrado y el estado de tu prueba o plan. |
| `/chatid` | Muestra tu propio chat_id. |

---

## Cómo funciona la prueba gratuita

Si llegaste desde la landing page y elegiste un plan, el bot te ofrece antes
una **prueba gratuita**:

- Dura hasta **14 días o 3 alertas** (lo que ocurra primero).
- Durante la prueba recibes las alertas nuevas directo a tu chat y tienes
  5 consultas de IA al día, igual que el plan gratis normal.
- Al terminar la prueba, el bot te muestra los 3 planes pagados para que
  elijas si quieres continuar.
- La prueba gratuita solo se puede usar **una vez por cuenta**: si ya la
  usaste, el botón de "Empezar prueba gratuita" te muestra los planes en
  vez de reiniciarla.

Si no viniste de la landing page (por invitación directa), tu cuenta queda
en el plan **gratis** normal desde el inicio: alertas ilimitadas + 5
consultas de IA al día, sin fecha de vencimiento.

---

## Cómo suscribirte y pagar

1. Envía `/suscribirme basico` (o `consultoria` / `empresarial`).
2. El bot te da el número de Yape para pagar el monto exacto del plan.
3. Después de pagar, envía `/pague codigo_de_operacion` con el código que
   te muestra Yape al confirmar el pago.
4. En cuanto se verifique, tu plan se activa (normalmente en minutos).

Planes y precios:

| Plan | Precio | Consultas IA/día |
|---|---|---|
| Gratis | S/0 | 5 |
| Básico | S/29/mes | 30 |
| Consultoría | S/79/mes | 100 |
| Empresarial | S/199/mes | Sin límite |

---

## Preguntas frecuentes

**¿Mis consultas son privadas?**
Sí, las consultas con IA solo funcionan en tu chat privado con el bot.

**¿Qué pasa si se me acaban las consultas del día?**
Las alertas automáticas siguen llegando sin límite. Solo las consultas con
IA tienen cupo diario, y se renueva al día siguiente.

**¿La información es confiable?**
Cada respuesta cita la alerta o norma oficial de DIGEMID de donde proviene,
con enlace al documento. Si no hay información en la fuente, el bot lo dice
en vez de inventar.
