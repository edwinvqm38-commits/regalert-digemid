# Probar los cambios del bot sin tocar el bot real

Esta guia prueba la rama `feature/alertas-gratis-prueba-ia` usando **el mismo
proyecto de Supabase (DIGEMID_BOT)**, sin crear otro proyecto. Se despliega una
copia de la funcion llamada `telegram-bot-ensayo` con un bot de Telegram de
prueba. El bot real y su funcion `telegram-bot` no cambian.

## Que se comparte con el bot real

- **La base de datos es la misma.** Cualquier usuario que crees al probar queda
  en `digemid_bot_usuarios`. Los comandos de administrador del bot de prueba
  (`/activar`, `/desactivar`, `/invitar`) actuan sobre usuarios reales, asi que
  usalos solo con tu propia cuenta.
- **Las claves de IA NO se comparten**: la copia lee `DEEPSEEK_API_KEY_ENSAYO` y
  `GEMINI_API_KEY_ENSAYO`. Si no las defines, el bot de prueba no llama a
  ningun modelo y no genera gasto.

## Pasos

1. Crea un bot de prueba con @BotFather (`/newbot`) y guarda su token.
2. Ejecuta `bash scripts/preparar_funcion_ensayo.sh` y sigue lo que imprime
   (secrets `_ENSAYO`, despliegue y webhook).
3. Prueba con una cuenta de Telegram que **no** este registrada en el bot real.

## Que verificar

| Prueba | Resultado esperado |
|---|---|
| `/start` | Mensaje de bienvenida que dice que las alertas son gratis y la prueba de IA es de 2 consultas al dia por 5 dias |
| `/ultimas`, `/hoy`, `/buscar retiro` sin activar prueba | Responden normalmente, sin pedir prueba ni plan |
| `/consulta algo` sin prueba | Pide prueba activa o plan, y aclara que las alertas siguen gratis |
| Boton "Empezar prueba gratuita" y luego `/consulta` | Se activa la prueba: 2 consultas al dia por 5 dias. Sin claves de IA de ensayo la respuesta de IA fallara, pero el contador y el limite igual se aplican |
| Tercera `/consulta` del dia | Avisa que se alcanzo el limite diario y que las alertas siguen llegando |
| `/miperfil` | Dice que la prueba de IA esta activa y que las alertas son gratis |

## Alertas

Para comprobar que la alerta llega a todos los registrados, **no** corras el
monitor real contra la base real: enviaria alertas a todos tus usuarios. Usa el
script `scripts/enviar_alerta_ejemplo.py` solo si sabes a quien le llega, o
verifica el envio con las pruebas automaticas (`tests/test_alertas_gratis.py`).

## Limpieza al terminar

1. Borra tu usuario de prueba de `digemid_bot_usuarios`.
2. Borra la funcion de prueba: `supabase functions delete telegram-bot-ensayo`.
3. Quita el webhook del bot de prueba o borra el bot en BotFather.
4. Elimina los secrets `*_ENSAYO` de Supabase.
