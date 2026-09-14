/** Idempotencia y registro de usuarios de WhatsApp.
 *
 * Cubre los casos 7 y 17 de la lista de pruebas del MVP: un message_id
 * duplicado (reintento de Meta) no se procesa dos veces, y un usuario que
 * escribe por primera vez queda registrado. Supabase se reemplaza por un
 * doble de prueba: no hay red ni credenciales.
 */

import { assert, assertEquals, assertFalse } from "jsr:@std/assert@1";
import {
  diasRestantesPrueba,
  enPeriodoDePrueba,
  identidadCanal,
  listarUsuariosPorVencer,
  listarUsuariosRecientes,
  MENSAJES_TABLE,
  PRUEBA_LIMITE_DIAS,
  reservarMensaje,
  upsertUsuarioWhatsApp,
  USUARIOS_TABLE,
  usuarioTieneAcceso,
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

  const usuario = await upsertUsuarioWhatsApp(cliente, {
    waId: "51987654321",
    telefono: "51987654321",
    nombrePerfil: "Edwin QF",
  });

  assertEquals(usuario.nivel, "gratis");
  assert(usuario.createdAt, "debe devolver la fecha de alta para calcular la prueba");

  const alta = operaciones.find((o) => o.tabla === USUARIOS_TABLE && o.tipo === "insert");
  assert(alta, "debió insertarse el usuario");
  assertEquals(alta?.valores?.wa_id, "51987654321");
  assertEquals(alta?.valores?.nombre_perfil, "Edwin QF");
});

Deno.test("un usuario existente conserva su nivel y suma el mensaje", async () => {
  const { cliente, operaciones } = fakeSupabase({
    usuarioExistente: {
      id: "uuid-1",
      nivel: "basico",
      mensajes_recibidos: 4,
      created_at: "2026-01-01T00:00:00.000Z",
    },
  });

  const usuario = await upsertUsuarioWhatsApp(cliente, {
    waId: "51987654321",
    telefono: "51987654321",
    nombrePerfil: "Edwin QF",
  });

  assertEquals(usuario.nivel, "basico");
  assertEquals(usuario.createdAt, "2026-01-01T00:00:00.000Z");

  const actualizacion = operaciones.find(
    (o) => o.tabla === USUARIOS_TABLE && o.tipo === "update",
  );
  assertEquals(actualizacion?.valores?.mensajes_recibidos, 5);
});

Deno.test("dentro de los dias de prueba, enPeriodoDePrueba es true", () => {
  const inicio = "2026-01-01T00:00:00.000Z";
  const ahora = new Date("2026-01-05T00:00:00.000Z"); // 4 dias despues

  assert(enPeriodoDePrueba(inicio, ahora));
});

Deno.test("pasado PRUEBA_LIMITE_DIAS, enPeriodoDePrueba es false", () => {
  const inicio = "2026-01-01T00:00:00.000Z";
  const ahora = new Date(
    new Date(inicio).getTime() + (PRUEBA_LIMITE_DIAS + 1) * 24 * 60 * 60 * 1000,
  );

  assertFalse(enPeriodoDePrueba(inicio, ahora));
});

Deno.test("un usuario gratis dentro de la prueba tiene acceso", () => {
  const inicio = new Date().toISOString();
  assert(usuarioTieneAcceso({ nivel: "gratis", createdAt: inicio }));
});

Deno.test("un usuario gratis con la prueba vencida NO tiene acceso", () => {
  const inicio = new Date(
    Date.now() - (PRUEBA_LIMITE_DIAS + 1) * 24 * 60 * 60 * 1000,
  ).toISOString();
  assertFalse(usuarioTieneAcceso({ nivel: "gratis", createdAt: inicio }));
});

Deno.test("un usuario con plan pagado tiene acceso aunque la prueba haya vencido", () => {
  const inicioMuyViejo = new Date(
    Date.now() - (PRUEBA_LIMITE_DIAS + 100) * 24 * 60 * 60 * 1000,
  ).toISOString();
  assert(usuarioTieneAcceso({ nivel: "basico", createdAt: inicioMuyViejo }));
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

Deno.test("diasRestantesPrueba cuenta hacia arriba y nunca es negativo", () => {
  const inicio = "2026-01-01T00:00:00.000Z";

  assertEquals(diasRestantesPrueba(inicio, new Date("2026-01-01T00:00:00.000Z")), 7);
  // 6 dias y 1 hora despues: queda menos de 1 dia entero, redondea a 1.
  assertEquals(diasRestantesPrueba(inicio, new Date("2026-01-07T01:00:00.000Z")), 1);
  // Ya vencido: nunca negativo.
  assertEquals(diasRestantesPrueba(inicio, new Date("2026-02-01T00:00:00.000Z")), 0);
});

/** Doble minimo para las consultas de listado (select().order().limit() y
 * select().eq().order()) que usan los comandos admin. */
function fakeSupabaseListado(filas: Record<string, unknown>[]) {
  return {
    from() {
      return {
        select() {
          return {
            eq(_campo: string, valor: unknown) {
              const filtradas = filas.filter((f) => f.nivel === valor);
              return {
                order() {
                  return Promise.resolve({ data: filtradas, error: null });
                },
              };
            },
            order() {
              return {
                limit(n: number) {
                  return Promise.resolve({ data: filas.slice(0, n), error: null });
                },
              };
            },
          };
        },
      };
    },
  };
}

Deno.test("listarUsuariosRecientes devuelve los dias restantes de prueba solo en gratis", async () => {
  const ahora = new Date();
  const cliente = fakeSupabaseListado([
    { wa_id: "51111", nombre_perfil: "Ana", nivel: "gratis", created_at: ahora.toISOString() },
    { wa_id: "51222", nombre_perfil: "Beto", nivel: "basico", created_at: ahora.toISOString() },
  ]);

  const usuarios = await listarUsuariosRecientes(cliente, 20);

  assertEquals(usuarios.length, 2);
  assertEquals(usuarios[0].diasRestantesPrueba, PRUEBA_LIMITE_DIAS);
  assertEquals(usuarios[1].diasRestantesPrueba, null);
});

Deno.test("listarUsuariosPorVencer excluye a los ya vencidos y a los que recien empiezan", async () => {
  const ahora = new Date();
  const haceUnDia = (dias: number) => new Date(ahora.getTime() - dias * 24 * 60 * 60 * 1000).toISOString();

  const cliente = fakeSupabaseListado([
    // Vence en 1 dia: dentro de la ventana de 3.
    { wa_id: "51111", nombre_perfil: "Por vencer", nivel: "gratis", created_at: haceUnDia(6) },
    // Recien empezo: le quedan 7 dias, fuera de la ventana de 3.
    { wa_id: "51222", nombre_perfil: "Recien", nivel: "gratis", created_at: haceUnDia(0) },
    // Ya vencido: no es "por vencer".
    { wa_id: "51333", nombre_perfil: "Vencido", nivel: "gratis", created_at: haceUnDia(10) },
    // Plan pagado: la prueba no le aplica.
    { wa_id: "51444", nombre_perfil: "Pagado", nivel: "basico", created_at: haceUnDia(6) },
  ]);

  const usuarios = await listarUsuariosPorVencer(cliente, 3);

  assertEquals(usuarios.length, 1);
  assertEquals(usuarios[0].waId, "51111");
});

Deno.test("la identidad de canal no colisiona con un chat_id de Telegram", () => {
  const identidad = identidadCanal("51987654321");

  assertEquals(identidad, "wa:51987654321");
  // Un chat_id de Telegram es numérico: el prefijo evita que dos usuarios
  // distintos compartan cuota o historial por tener el mismo número.
  assert(Number.isNaN(Number(identidad)));
});
