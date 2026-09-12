/** Seguridad y parseo del webhook de Meta.
 *
 * Cubre los casos 1 a 6 y 15 de la lista de pruebas del MVP: verificacion
 * GET (token correcto/incorrecto), firma HMAC valida/invalida, evento de
 * statuses sin mensajes, mensaje de texto valido y evento malformado.
 * Ninguna prueba llama a Meta.
 */

import { assert, assertEquals } from "jsr:@std/assert@1";
import {
  extraerMensajeEntrante,
  firmaValida,
  truncarParaLog,
  verificarWebhookGet,
} from "./webhook.ts";

const APP_SECRET = "secreto-de-prueba-no-real";

async function firmar(rawBody: string, secret = APP_SECRET): Promise<string> {
  const clave = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const firma = new Uint8Array(
    await crypto.subtle.sign("HMAC", clave, new TextEncoder().encode(rawBody)),
  );
  const hex = Array.from(firma).map((b) => b.toString(16).padStart(2, "0")).join("");
  return `sha256=${hex}`;
}

function payloadMensajeTexto(texto = "hola", messageId = "wamid.TEST123") {
  return {
    object: "whatsapp_business_account",
    entry: [{
      id: "WABA_ID",
      changes: [{
        field: "messages",
        value: {
          messaging_product: "whatsapp",
          metadata: { display_phone_number: "51999999999", phone_number_id: "PHONE_ID" },
          contacts: [{ profile: { name: "Edwin QF" }, wa_id: "51987654321" }],
          messages: [{
            from: "51987654321",
            id: messageId,
            timestamp: "1757692800",
            type: "text",
            text: { body: texto },
          }],
        },
      }],
    }],
  };
}

// --- 1 y 2: verificacion GET del webhook ---

Deno.test("GET con verify token correcto devuelve el challenge", () => {
  const url = new URL(
    "https://x.functions.supabase.co/whatsapp-bot?hub.mode=subscribe&hub.verify_token=token-ok&hub.challenge=1234567",
  );
  const resultado = verificarWebhookGet(url, "token-ok");

  assert(resultado.ok);
  assertEquals(resultado.challenge, "1234567");
});

Deno.test("GET con verify token incorrecto es rechazado", () => {
  const url = new URL(
    "https://x.functions.supabase.co/whatsapp-bot?hub.mode=subscribe&hub.verify_token=token-malo&hub.challenge=1234567",
  );
  const resultado = verificarWebhookGet(url, "token-ok");

  assertEquals(resultado.ok, false);
});

Deno.test("GET sin hub.mode=subscribe es rechazado", () => {
  const url = new URL(
    "https://x.functions.supabase.co/whatsapp-bot?hub.verify_token=token-ok&hub.challenge=1",
  );
  assertEquals(verificarWebhookGet(url, "token-ok").ok, false);
});

Deno.test("GET es rechazado si no hay verify token configurado", () => {
  const url = new URL(
    "https://x.functions.supabase.co/whatsapp-bot?hub.mode=subscribe&hub.verify_token=&hub.challenge=1",
  );
  assertEquals(verificarWebhookGet(url, "").ok, false);
});

// --- 3 y 4: firma X-Hub-Signature-256 ---

Deno.test("POST con firma valida se acepta", async () => {
  const rawBody = JSON.stringify(payloadMensajeTexto());
  const firma = await firmar(rawBody);

  assertEquals(await firmaValida(rawBody, firma, APP_SECRET), true);
});

Deno.test("POST con firma invalida se rechaza", async () => {
  const rawBody = JSON.stringify(payloadMensajeTexto());

  assertEquals(
    await firmaValida(rawBody, "sha256=00112233445566778899aabbccddeeff", APP_SECRET),
    false,
  );
});

Deno.test("una firma de otro secreto se rechaza", async () => {
  const rawBody = JSON.stringify(payloadMensajeTexto());
  const firmaAjena = await firmar(rawBody, "otro-secreto");

  assertEquals(await firmaValida(rawBody, firmaAjena, APP_SECRET), false);
});

Deno.test("si el cuerpo cambia, la firma deja de ser valida", async () => {
  const original = JSON.stringify(payloadMensajeTexto("hola"));
  const firma = await firmar(original);
  const manipulado = JSON.stringify(payloadMensajeTexto("otro texto"));

  assertEquals(await firmaValida(manipulado, firma, APP_SECRET), false);
});

Deno.test("sin cabecera de firma o sin app secret se rechaza", async () => {
  const rawBody = JSON.stringify(payloadMensajeTexto());

  assertEquals(await firmaValida(rawBody, null, APP_SECRET), false);
  assertEquals(await firmaValida(rawBody, await firmar(rawBody), ""), false);
  assertEquals(await firmaValida(rawBody, "firma-sin-prefijo", APP_SECRET), false);
});

// --- 5, 6 y 15: parseo de eventos ---

Deno.test("mensaje de texto valido se extrae completo", () => {
  const mensaje = extraerMensajeEntrante(payloadMensajeTexto("buscar paracetamol"));

  assert(mensaje);
  assertEquals(mensaje.messageId, "wamid.TEST123");
  assertEquals(mensaje.waId, "51987654321");
  assertEquals(mensaje.nombrePerfil, "Edwin QF");
  assertEquals(mensaje.tipo, "text");
  assertEquals(mensaje.texto, "buscar paracetamol");
  assertEquals(mensaje.phoneNumberId, "PHONE_ID");
});

Deno.test("evento de statuses (sin messages) se ignora", () => {
  const payload = {
    object: "whatsapp_business_account",
    entry: [{
      id: "WABA_ID",
      changes: [{
        field: "messages",
        value: {
          messaging_product: "whatsapp",
          metadata: { phone_number_id: "PHONE_ID" },
          statuses: [{
            id: "wamid.STATUS1",
            status: "delivered",
            timestamp: "1757692800",
            recipient_id: "51987654321",
          }],
        },
      }],
    }],
  };

  assertEquals(extraerMensajeEntrante(payload), null);
});

Deno.test("eventos malformados o desconocidos no rompen el parseo", () => {
  assertEquals(extraerMensajeEntrante(null), null);
  assertEquals(extraerMensajeEntrante("no soy un objeto"), null);
  assertEquals(extraerMensajeEntrante({}), null);
  assertEquals(extraerMensajeEntrante({ object: "page", entry: [] }), null);
  assertEquals(extraerMensajeEntrante({ object: "whatsapp_business_account" }), null);
  assertEquals(
    extraerMensajeEntrante({ object: "whatsapp_business_account", entry: [{ changes: null }] }),
    null,
  );
  // messages presente pero sin id ni from utilizables.
  assertEquals(
    extraerMensajeEntrante({
      object: "whatsapp_business_account",
      entry: [{ changes: [{ value: { messages: [{ type: "text" }] } }] }],
    }),
    null,
  );
});

Deno.test("un mensaje que no es de texto se extrae con su tipo real", () => {
  // El canal debe poder responder "solo texto por ahora" en vez de ignorarlo.
  const payload = payloadMensajeTexto();
  // deno-lint-ignore no-explicit-any
  const valor = (payload as any).entry[0].changes[0].value;
  valor.messages[0] = {
    from: "51987654321",
    id: "wamid.AUDIO1",
    timestamp: "1757692800",
    type: "audio",
    audio: { id: "media-id" },
  };

  const mensaje = extraerMensajeEntrante(payload);
  assert(mensaje);
  assertEquals(mensaje.tipo, "audio");
  assertEquals(mensaje.texto, "");
});

Deno.test("los logs no exponen el identificador completo", () => {
  const truncado = truncarParaLog("wamid.HBgLNTE5ODc2NTQzMjEVAgARGBI5", 8);

  assert(truncado.length < 12);
  assert(!truncado.includes("NTE5ODc2NTQzMjE"));
});
