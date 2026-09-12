/** Idempotencia y registro de usuarios de WhatsApp.
 *
 * Cubre los casos 7 y 17 de la lista de pruebas del MVP: un message_id
 * duplicado (reintento de Meta) no se procesa dos veces, y un usuario que
 * escribe por primera vez queda registrado. Supabase se reemplaza por un
 * doble de prueba: no hay red ni credenciales.
 */

import { assert, assertEquals } from "jsr:@std/assert@1";
import {
  identidadCanal,
  MENSAJES_TABLE,
  reservarMensaje,
  upsertUsuarioWhatsApp,
  USUARIOS_TABLE,
} from "./persistencia.ts";

type Operacion = { tabla: string; tipo: string; valores?: Record<string, unknown> };

/** Doble de Supabase: memoriza inserts/updates y simula el UNIQUE de
 * message_id devolviendo el error 23505 de Postgres. */
function fakeSupabase(opciones: {
  messageIdsExistentes?: string[];
  usuarioExistente?: Record<string, unknown> | null;
} = {}) {
  const messageIds = new Set(opciones.messageIdsExistentes ?? []);
  const operaciones: Operacion[] = [];

  const cliente = {
    from(tabla: string) {
      return {
        insert(valores: Record<string, unknown>) {
          if (tabla === MENSAJES_TABLE) {
            const id = String(valores.message_id);
            if (messageIds.has(id)) {
              return Promise.resolve({
                error: { code: "23505", message: "duplicate key value" },
              });
            }
            messageIds.add(id);
          }
          operaciones.push({ tabla, tipo: "insert", valores });
          return Promise.resolve({ error: null });
        },
        update(valores: Record<string, unknown>) {
          operaciones.push({ tabla, tipo: "update", valores });
          return { eq: () => Promise.resolve({ error: null }) };
        },
        select() {
          return {
            eq() {
              return {
                maybeSingle: () =>
                  Promise.resolve({ data: opciones.usuarioExistente ?? null, error: null }),
              };
            },
          };
        },
      };
    },
  };

  return { cliente, operaciones };
}

Deno.test("un message_id nuevo se reserva y se procesa", async () => {
  const { cliente, operaciones } = fakeSupabase();

  const esNuevo = await reservarMensaje(cliente, {
    messageId: "wamid.NUEVO",
    waId: "51987654321",
  });

  assertEquals(esNuevo, true);
  assertEquals(operaciones.filter((o) => o.tabla === MENSAJES_TABLE).length, 1);
});

Deno.test("un message_id duplicado NO se procesa de nuevo", async () => {
  // Meta reintenta el webhook si no recibe 200 a tiempo: sin esto, el
  // usuario recibiría la misma respuesta dos veces (y se le cobraría dos
  // consultas de su plan).
  const { cliente } = fakeSupabase({ messageIdsExistentes: ["wamid.REPETIDO"] });

  const esNuevo = await reservarMensaje(cliente, {
    messageId: "wamid.REPETIDO",
    waId: "51987654321",
  });

  assertEquals(esNuevo, false);
});

Deno.test("un error de base distinto de duplicado sí se propaga", async () => {
  const cliente = {
    from: () => ({
      insert: () => Promise.resolve({ error: { code: "42P01", message: "tabla inexistente" } }),
    }),
  };

  let lanzo = false;
  try {
    await reservarMensaje(cliente, { messageId: "wamid.X", waId: "51987654321" });
  } catch {
    lanzo = true;
  }

  assert(lanzo, "un error real de base no debe confundirse con un duplicado");
});

Deno.test("un usuario nuevo de WhatsApp queda registrado como gratis", async () => {
  const { cliente, operaciones } = fakeSupabase({ usuarioExistente: null });

  const nivel = await upsertUsuarioWhatsApp(cliente, {
    waId: "51987654321",
    telefono: "51987654321",
    nombrePerfil: "Edwin QF",
  });

  assertEquals(nivel, "gratis");

  const alta = operaciones.find((o) => o.tabla === USUARIOS_TABLE && o.tipo === "insert");
  assert(alta, "debió insertarse el usuario");
  assertEquals(alta?.valores?.wa_id, "51987654321");
  assertEquals(alta?.valores?.nombre_perfil, "Edwin QF");
});

Deno.test("un usuario existente conserva su nivel y suma el mensaje", async () => {
  const { cliente, operaciones } = fakeSupabase({
    usuarioExistente: { id: "uuid-1", nivel: "basico", mensajes_recibidos: 4 },
  });

  const nivel = await upsertUsuarioWhatsApp(cliente, {
    waId: "51987654321",
    telefono: "51987654321",
    nombrePerfil: "Edwin QF",
  });

  assertEquals(nivel, "basico");

  const actualizacion = operaciones.find(
    (o) => o.tabla === USUARIOS_TABLE && o.tipo === "update",
  );
  assertEquals(actualizacion?.valores?.mensajes_recibidos, 5);
});

Deno.test("los usuarios de WhatsApp NO se escriben en las tablas de Telegram", async () => {
  // digemid_bot_usuarios y digemid_suscripciones son recorridas por
  // agent_notify.py y enviar_recordatorio_planes.py para mandar DMs por
  // Telegram: escribir ahí a un usuario de WhatsApp lo volvería
  // destinatario de mensajes proactivos.
  const { cliente, operaciones } = fakeSupabase({ usuarioExistente: null });

  await upsertUsuarioWhatsApp(cliente, {
    waId: "51987654321",
    telefono: "51987654321",
    nombrePerfil: "Edwin QF",
  });

  const tablasTocadas = new Set(operaciones.map((o) => o.tabla));
  assert(!tablasTocadas.has("digemid_bot_usuarios"), "no debe tocar usuarios de Telegram");
  assert(!tablasTocadas.has("digemid_suscripciones"), "no debe tocar suscripciones de Telegram");
});

Deno.test("la identidad de canal no colisiona con un chat_id de Telegram", () => {
  const identidad = identidadCanal("51987654321");

  assertEquals(identidad, "wa:51987654321");
  // Un chat_id de Telegram es numérico: el prefijo evita que dos usuarios
  // distintos compartan cuota o historial por tener el mismo número.
  assert(Number.isNaN(Number(identidad)));
});
