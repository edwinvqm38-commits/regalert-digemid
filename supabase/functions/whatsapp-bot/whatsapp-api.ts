/** Cliente de WhatsApp Cloud API (Meta) restringido a respuestas de servicio.
 *
 * INBOUND-ONLY POR CONSTRUCCION, no solo por configuracion:
 *
 *   - La unica funcion de envio, sendWhatsAppServiceMessage(), NO recibe un
 *     numero de destino. Recibe un ContextoInbound, y responde siempre al
 *     wa_id que viene dentro de el.
 *   - Un ContextoInbound solo puede crearse con crearContextoInbound(), que
 *     exige un mensaje entrante ya validado (id + wa_id reales).
 *   - No existe ninguna funcion que acepte una lista de destinatarios, ni
 *     soporte de message templates. Un job/cron/workflow que quisiera usar
 *     este modulo para un broadcast no tiene por donde: tendria que
 *     fabricar mensajes entrantes falsos.
 *   - Ademas WHATSAPP_OUTBOUND_MODE actua como cerrojo explicito; si falta,
 *     se asume "inbound_only" (el valor seguro).
 *
 * Esto importa por costo y por politica de Meta: los mensajes iniciados por
 * el negocio requieren plantillas aprobadas y se cobran por conversacion;
 * las respuestas dentro de la ventana de servicio de 24h no.
 */

export const MODO_INBOUND_ONLY = "inbound_only";

/** Limite de caracteres de un mensaje de texto en WhatsApp Cloud API. */
export const MAX_CARACTERES_MENSAJE = 4096;

export type ConfigWhatsApp = {
  accessToken: string;
  phoneNumberId: string;
  graphApiVersion: string;
  /** Valor de WHATSAPP_OUTBOUND_MODE; vacio o ausente => inbound_only. */
  outboundMode: string;
};

/** Marca de tipo: impide construir un contexto a mano con un objeto literal
 * (TypeScript rechaza `{ waId: "51...", messageId: "x" }` como ContextoInbound
 * porque no puede producir la marca privada). */
declare const MARCA_INBOUND: unique symbol;

export type ContextoInbound = {
  readonly waId: string;
  readonly messageId: string;
  readonly [MARCA_INBOUND]: true;
};

export function crearContextoInbound(
  mensaje: { messageId: string; waId: string },
): ContextoInbound {
  if (!mensaje?.waId || !mensaje?.messageId) {
    throw new Error(
      "Contexto inbound inválido: solo se puede responder a un mensaje entrante real.",
    );
  }

  return {
    waId: mensaje.waId,
    messageId: mensaje.messageId,
  } as ContextoInbound;
}

export function modoSoloEntrante(outboundMode: string | undefined | null): boolean {
  const modo = (outboundMode ?? "").trim().toLowerCase();
  // Sin variable configurada se asume el modo seguro (§1).
  return modo === "" || modo === MODO_INBOUND_ONLY;
}

/** Parte un texto largo en varios mensajes respetando el limite de WhatsApp,
 * cortando por linea (y por palabra si una linea sola ya excede el limite)
 * para no partir una cita normativa a la mitad de una palabra. */
export function partirMensajeLargo(
  texto: string,
  limite = MAX_CARACTERES_MENSAJE,
): string[] {
  const contenido = (texto ?? "").trim();
  if (!contenido) return [];
  if (contenido.length <= limite) return [contenido];

  const partes: string[] = [];
  let actual = "";

  const empujar = () => {
    if (actual.trim()) partes.push(actual.trim());
    actual = "";
  };

  for (const linea of contenido.split("\n")) {
    if (linea.length > limite) {
      empujar();
      let resto = linea;
      while (resto.length > limite) {
        const corte = resto.lastIndexOf(" ", limite);
        const indice = corte > limite * 0.5 ? corte : limite;
        partes.push(resto.slice(0, indice).trim());
        resto = resto.slice(indice).trim();
      }
      actual = resto;
      continue;
    }

    const candidato = actual ? `${actual}\n${linea}` : linea;
    if (candidato.length > limite) {
      empujar();
      actual = linea;
    } else {
      actual = candidato;
    }
  }

  empujar();
  return partes;
}

export type ResultadoEnvio = {
  enviados: number;
  /** ids de mensaje que devolvio Meta, para trazabilidad. */
  messageIds: string[];
};

/**
 * Responde por WhatsApp al usuario que escribio (y solo a el).
 *
 * @param contexto Contexto del mensaje entrante que se esta respondiendo.
 *                 Define el destinatario: no hay forma de enviar a otro.
 */
export async function sendWhatsAppServiceMessage(
  contexto: ContextoInbound,
  texto: string,
  config: ConfigWhatsApp,
): Promise<ResultadoEnvio> {
  if (!modoSoloEntrante(config.outboundMode)) {
    throw new Error(
      `WHATSAPP_OUTBOUND_MODE="${config.outboundMode}" no está soportado: ` +
        `este canal es ${MODO_INBOUND_ONLY} (no envía mensajes proactivos ni plantillas).`,
    );
  }

  if (!contexto?.waId) {
    throw new Error("No hay contexto inbound: no se puede iniciar una conversación de WhatsApp.");
  }

  if (!config.accessToken || !config.phoneNumberId) {
    throw new Error("Faltan WHATSAPP_ACCESS_TOKEN o WHATSAPP_PHONE_NUMBER_ID");
  }

  const partes = partirMensajeLargo(texto);
  if (!partes.length) {
    throw new Error("No se envía un mensaje vacío por WhatsApp.");
  }

  const url =
    `https://graph.facebook.com/${config.graphApiVersion}/${config.phoneNumberId}/messages`;
  const messageIds: string[] = [];

  for (const parte of partes) {
    const respuesta = await fetch(url, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${config.accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        messaging_product: "whatsapp",
        recipient_type: "individual",
        to: contexto.waId,
        type: "text",
        text: { preview_url: false, body: parte },
      }),
    });

    if (!respuesta.ok) {
      // El cuerpo del error de Meta no trae secretos (si trae codigos de
      // error y el id del telefono), pero se recorta por las dudas.
      const detalle = (await respuesta.text()).slice(0, 300);
      throw new Error(`WhatsApp Cloud API ${respuesta.status}: ${detalle}`);
    }

    const data = await respuesta.json().catch(() => ({}));
    const id = data?.messages?.[0]?.id;
    if (typeof id === "string") messageIds.push(id);
  }

  return { enviados: partes.length, messageIds };
}
