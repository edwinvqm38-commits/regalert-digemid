/** Verificacion y parseo del webhook de WhatsApp Cloud API (Meta).
 *
 * Modulo puro salvo por Web Crypto (disponible en Deno sin permisos): no
 * toca Supabase ni hace red, para poder probar la seguridad del webhook sin
 * llamar a Meta.
 */

export type MensajeEntranteWhatsApp = {
  messageId: string;
  waId: string;
  telefono: string;
  nombrePerfil: string;
  timestamp: string;
  tipo: string;
  /** Solo viene con contenido cuando tipo === "text". */
  texto: string;
  phoneNumberId: string;
};

export type ResultadoVerificacion =
  | { ok: true; challenge: string }
  | { ok: false; motivo: string };

/** GET de verificacion del webhook (Meta lo llama una vez al configurarlo).
 * El token nunca se registra en logs; solo se devuelve si coincide. */
export function verificarWebhookGet(
  url: URL,
  verifyTokenEsperado: string,
): ResultadoVerificacion {
  const mode = url.searchParams.get("hub.mode");
  const token = url.searchParams.get("hub.verify_token");
  const challenge = url.searchParams.get("hub.challenge");

  if (!verifyTokenEsperado) {
    return { ok: false, motivo: "WHATSAPP_VERIFY_TOKEN no configurado" };
  }

  if (mode !== "subscribe") {
    return { ok: false, motivo: "hub.mode inválido" };
  }

  if (!token || token !== verifyTokenEsperado) {
    return { ok: false, motivo: "hub.verify_token inválido" };
  }

  if (!challenge) {
    return { ok: false, motivo: "falta hub.challenge" };
  }

  return { ok: true, challenge };
}

function hexAUint8Array(hex: string): Uint8Array | null {
  if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) return null;
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

/** Comparacion en tiempo constante: comparar firmas con === filtra
 * informacion por el tiempo de respuesta (timing attack). */
function igualesEnTiempoConstante(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diferencia = 0;
  for (let i = 0; i < a.length; i++) diferencia |= a[i] ^ b[i];
  return diferencia === 0;
}

/** Valida X-Hub-Signature-256 (HMAC SHA-256 del cuerpo crudo con el App
 * Secret), segun la especificacion de Meta. Debe calcularse sobre el body
 * EXACTO recibido: cualquier re-serializacion del JSON cambia el hash. */
export async function firmaValida(
  rawBody: string,
  cabeceraFirma: string | null,
  appSecret: string,
): Promise<boolean> {
  if (!appSecret || !cabeceraFirma) return false;
  if (!cabeceraFirma.startsWith("sha256=")) return false;

  const firmaRecibida = hexAUint8Array(cabeceraFirma.slice("sha256=".length).trim());
  if (!firmaRecibida) return false;

  const clave = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(appSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );

  const firmaEsperada = new Uint8Array(
    await crypto.subtle.sign("HMAC", clave, new TextEncoder().encode(rawBody)),
  );

  return igualesEnTiempoConstante(firmaRecibida, firmaEsperada);
}

/** Extrae el primer mensaje entrante real del payload de Meta.
 *
 * Devuelve null para todo lo que no sea un mensaje consultable (statuses de
 * entrega/lectura, cambios administrativos, eventos desconocidos o
 * malformados): esos se responden 200 OK sin procesar, porque Meta los
 * reintenta si no recibe 200. */
export function extraerMensajeEntrante(payload: unknown): MensajeEntranteWhatsApp | null {
  if (!payload || typeof payload !== "object") return null;

  // deno-lint-ignore no-explicit-any
  const cuerpo = payload as any;

  if (cuerpo.object !== "whatsapp_business_account") return null;

  const entradas = Array.isArray(cuerpo.entry) ? cuerpo.entry : [];

  for (const entrada of entradas) {
    const cambios = Array.isArray(entrada?.changes) ? entrada.changes : [];

    for (const cambio of cambios) {
      const valor = cambio?.value;
      if (!valor || typeof valor !== "object") continue;

      // statuses (sent/delivered/read) llegan sin "messages": no son
      // consultas del usuario, se ignoran de forma segura.
      const mensajes = Array.isArray(valor.messages) ? valor.messages : [];
      if (!mensajes.length) continue;

      const mensaje = mensajes[0];
      const messageId = typeof mensaje?.id === "string" ? mensaje.id : "";
      const waId = typeof mensaje?.from === "string" ? mensaje.from : "";

      if (!messageId || !waId) continue;

      const contacto = Array.isArray(valor.contacts) ? valor.contacts[0] : null;

      return {
        messageId,
        waId,
        telefono: waId,
        nombrePerfil: typeof contacto?.profile?.name === "string" ? contacto.profile.name : "",
        timestamp: typeof mensaje?.timestamp === "string" ? mensaje.timestamp : "",
        tipo: typeof mensaje?.type === "string" ? mensaje.type : "desconocido",
        texto: typeof mensaje?.text?.body === "string" ? mensaje.text.body : "",
        phoneNumberId: typeof valor?.metadata?.phone_number_id === "string"
          ? valor.metadata.phone_number_id
          : "",
      };
    }
  }

  return null;
}

/** Para logs: nunca se registra el message_id completo ni el numero entero
 * del usuario (§21). */
export function truncarParaLog(valor: string, visibles = 8): string {
  if (!valor) return "";
  if (valor.length <= visibles) return `${valor.slice(0, 3)}…`;
  return `${valor.slice(0, visibles)}…`;
}
