/** Garantias de que el canal es inbound-only.
 *
 * Cubre los casos 12, 13 y 14 de la lista de pruebas del MVP: no se pueden
 * enviar plantillas, ni mensajes proactivos, ni responder sin un mensaje
 * entrante real. fetch se reemplaza por un doble: ninguna prueba llama a
 * Meta.
 */

import {
  assert,
  assertEquals,
  assertRejects,
  assertStringIncludes,
} from "jsr:@std/assert@1";
import {
  type ConfigWhatsApp,
  crearContextoInbound,
  MAX_CARACTERES_MENSAJE,
  modoSoloEntrante,
  partirMensajeLargo,
  sendWhatsAppServiceMessage,
} from "./whatsapp-api.ts";

const CONFIG: ConfigWhatsApp = {
  accessToken: "token-de-prueba",
  phoneNumberId: "PHONE_ID",
  graphApiVersion: "v21.0",
  outboundMode: "",
};

type LlamadaFetch = { url: string; body: Record<string, unknown> };

/** Reemplaza fetch y devuelve lo que se habria enviado a Meta. */
function espiarFetch(): { llamadas: LlamadaFetch[]; restaurar: () => void } {
  const original = globalThis.fetch;
  const llamadas: LlamadaFetch[] = [];

  globalThis.fetch = ((url: string | URL | Request, init?: RequestInit) => {
    llamadas.push({
      url: String(url),
      body: JSON.parse(String(init?.body ?? "{}")),
    });

    return Promise.resolve(
      new Response(JSON.stringify({ messages: [{ id: "wamid.RESPUESTA" }] }), { status: 200 }),
    );
  }) as typeof fetch;

  return { llamadas, restaurar: () => { globalThis.fetch = original; } };
}

const mensajeEntrante = { messageId: "wamid.ENTRANTE", waId: "51987654321" };

// --- 14: no se puede responder sin contexto inbound ---

Deno.test("no se puede crear un contexto sin un mensaje entrante real", () => {
  let fallo = false;
  try {
    crearContextoInbound({ messageId: "", waId: "" });
  } catch {
    fallo = true;
  }
  assert(fallo, "debería rechazar un contexto vacío");

  fallo = false;
  try {
    // Un job que quisiera escribirle a un número suelto no tiene messageId.
    crearContextoInbound({ messageId: "", waId: "51987654321" });
  } catch {
    fallo = true;
  }
  assert(fallo, "debería rechazar un contexto sin mensaje entrante");
});

Deno.test("el destinatario siempre es quien escribió, no un parámetro", async () => {
  const espia = espiarFetch();
  try {
    const contexto = crearContextoInbound(mensajeEntrante);
    await sendWhatsAppServiceMessage(contexto, "respuesta", CONFIG);

    assertEquals(espia.llamadas.length, 1);
    assertEquals(espia.llamadas[0].body.to, "51987654321");
  } finally {
    espia.restaurar();
  }
});

// --- 13: no hay mensajes proactivos ---

Deno.test("cualquier modo distinto de inbound_only bloquea el envío", async () => {
  const espia = espiarFetch();
  try {
    const contexto = crearContextoInbound(mensajeEntrante);

    for (const modo of ["broadcast", "outbound", "campaign", "todos"]) {
      await assertRejects(
        () => sendWhatsAppServiceMessage(contexto, "hola", { ...CONFIG, outboundMode: modo }),
        Error,
        "inbound_only",
      );
    }

    assertEquals(espia.llamadas.length, 0, "no debió llamarse a Meta ni una vez");
  } finally {
    espia.restaurar();
  }
});

Deno.test("sin WHATSAPP_OUTBOUND_MODE se asume el modo seguro", () => {
  assertEquals(modoSoloEntrante(undefined), true);
  assertEquals(modoSoloEntrante(""), true);
  assertEquals(modoSoloEntrante("inbound_only"), true);
  assertEquals(modoSoloEntrante("  INBOUND_ONLY  "), true);
  assertEquals(modoSoloEntrante("broadcast"), false);
});

// --- 12: no se envían plantillas ---

Deno.test("el mensaje enviado es de tipo text, nunca template", async () => {
  const espia = espiarFetch();
  try {
    const contexto = crearContextoInbound(mensajeEntrante);
    await sendWhatsAppServiceMessage(contexto, "respuesta de servicio", CONFIG);

    const body = espia.llamadas[0].body;
    assertEquals(body.type, "text");
    assertEquals(body.messaging_product, "whatsapp");
    assertEquals(body.template, undefined);
    assert(!JSON.stringify(body).includes("template"));
  } finally {
    espia.restaurar();
  }
});

Deno.test("el módulo no expone ninguna función de plantillas ni de difusión", async () => {
  const modulo = await import("./whatsapp-api.ts");
  const exportados = Object.keys(modulo).join(",").toLowerCase();

  assert(!exportados.includes("template"), `no debe exportar plantillas: ${exportados}`);
  assert(!exportados.includes("broadcast"), `no debe exportar difusión: ${exportados}`);
  assert(!exportados.includes("campaign"));
  assert(!exportados.includes("notify"));
});

Deno.test("la versión de Graph API viene de la configuración", async () => {
  const espia = espiarFetch();
  try {
    const contexto = crearContextoInbound(mensajeEntrante);
    await sendWhatsAppServiceMessage(contexto, "hola", { ...CONFIG, graphApiVersion: "v23.0" });

    assertStringIncludes(espia.llamadas[0].url, "/v23.0/PHONE_ID/messages");
  } finally {
    espia.restaurar();
  }
});

Deno.test("no se envían mensajes vacíos ni sin credenciales", async () => {
  const espia = espiarFetch();
  try {
    const contexto = crearContextoInbound(mensajeEntrante);

    await assertRejects(() => sendWhatsAppServiceMessage(contexto, "   ", CONFIG));
    await assertRejects(
      () => sendWhatsAppServiceMessage(contexto, "hola", { ...CONFIG, accessToken: "" }),
      Error,
      "WHATSAPP_ACCESS_TOKEN",
    );

    assertEquals(espia.llamadas.length, 0);
  } finally {
    espia.restaurar();
  }
});

Deno.test("un error de Meta se propaga sin filtrar el token", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = (() =>
    Promise.resolve(new Response('{"error":{"code":131030}}', { status: 400 }))) as typeof fetch;

  try {
    const contexto = crearContextoInbound(mensajeEntrante);
    const error = await assertRejects(() =>
      sendWhatsAppServiceMessage(contexto, "hola", CONFIG)
    );

    assert(!String(error).includes("token-de-prueba"));
  } finally {
    globalThis.fetch = original;
  }
});

// --- Troceo de respuestas largas ---

Deno.test("una respuesta larga se parte respetando el límite de WhatsApp", () => {
  const linea = "Artículo 30 del Reglamento de Establecimientos Farmacéuticos. ";
  const partes = partirMensajeLargo(linea.repeat(200));

  assert(partes.length > 1);
  for (const parte of partes) {
    assert(
      parte.length <= MAX_CARACTERES_MENSAJE,
      `parte de ${parte.length} caracteres supera el límite`,
    );
  }
});

Deno.test("un texto corto se envía en un solo mensaje", () => {
  assertEquals(partirMensajeLargo("respuesta corta"), ["respuesta corta"]);
  assertEquals(partirMensajeLargo("   "), []);
});

Deno.test("una respuesta larga se envía en varias llamadas al mismo destinatario", async () => {
  const espia = espiarFetch();
  try {
    const contexto = crearContextoInbound(mensajeEntrante);
    const resultado = await sendWhatsAppServiceMessage(contexto, "x".repeat(9000), CONFIG);

    assert(resultado.enviados > 1);
    assertEquals(espia.llamadas.length, resultado.enviados);
    for (const llamada of espia.llamadas) {
      assertEquals(llamada.body.to, "51987654321");
    }
  } finally {
    espia.restaurar();
  }
});
