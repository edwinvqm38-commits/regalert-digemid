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

Al escribir `/start` — ya sea desde la landing page o buscando el bot
directo en Telegram — el bot te muestra una **prueba gratuita** y los 3
planes pagados para que elijas:

- La prueba dura hasta **14 días o 3 alertas** (lo que ocurra primero).
- Durante la prueba recibes las alertas nuevas directo a tu chat y tienes
  5 consultas de IA al día.
- Al terminar la prueba sin suscribirte, **las alertas automáticas y las
  consultas con IA se pausan** hasta que elijas un plan — el bot te avisa
  y te muestra las opciones apenas se cumple el límite. También te llega
  algún recordatorio ocasional (no todos los días) por si quieres retomarlo.
- La prueba gratuita solo se puede usar **una vez por cuenta**: si ya la
  usaste, el botón de "Empezar prueba gratuita" te muestra los planes en
  vez de reiniciarla.

Si ya elegiste una prueba o un plan antes, `/start` ya no te vuelve a
mostrar esta pantalla — va directo al menú principal.

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
| Prueba gratuita | S/0 | 5 (hasta 14 días o 3 alertas) |
| Básico | S/29/mes | 30 |
| Consultoría | S/79/mes | 100 |
| Empresarial | S/199/mes | Sin límite |

---

## Preguntas frecuentes

**¿Mis consultas son privadas?**
Sí, las consultas con IA solo funcionan en tu chat privado con el bot.

**¿Qué pasa si se me acaban las consultas del día?**
Solo las consultas con IA tienen cupo diario según tu plan; se renueva al
día siguiente. Las alertas automáticas no tienen límite mientras tu prueba
o plan estén activos.

**¿Qué pasa cuando termina mi prueba gratuita?**
Las alertas y consultas se pausan hasta que elijas un plan con
`/suscribirme`. No se pierde nada de lo que ya usaste — solo se detiene el
servicio hasta que te suscribas.

**¿La información es confiable?**
Cada respuesta cita la alerta o norma oficial de DIGEMID de donde proviene,
con enlace al documento. Si no hay información en la fuente, el bot lo dice
en vez de inventar.
