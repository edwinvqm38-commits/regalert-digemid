/** Persistencia del canal WhatsApp: idempotencia, usuarios y log.
 *
 * Los usuarios de WhatsApp viven en digemid_whatsapp_usuarios, NO en
 * digemid_bot_usuarios. Motivo (documentado tambien en la migracion):
 * agents/agent_notify.py y scripts/enviar_recordatorio_planes.py recorren
 * digemid_bot_usuarios / digemid_suscripciones y envian mensajes por
 * Telegram a cada chat_id que encuentran. Guardar ahi a un usuario de
 * WhatsApp lo convertiria en destinatario de envios proactivos, que es
 * justamente lo que este canal no debe hacer.
 *
 * El log de consultas SI se comparte (digemid_bot_consultas) con el
 * identificador "wa:<wa_id>": esa tabla solo registra y cuenta, nadie envia
 * mensajes a partir de ella, y compartirla mantiene la trazabilidad y los
 * limites diarios en un solo lugar.
 *
 * El cliente de Supabase se recibe por parametro para poder probar esta
 * capa con un doble de prueba, sin credenciales ni red.
 */

import { getLimaStartOfDayIso } from "../_shared/fechas-lima.ts";

// deno-lint-ignore no-explicit-any
export type SupabaseLike = any;

export const USUARIOS_TABLE = "digemid_whatsapp_usuarios";
export const MENSAJES_TABLE = "digemid_whatsapp_mensajes_procesados";
export const CONSULTAS_TABLE = "digemid_bot_consultas";

/** Prefijo de canal: evita colisionar con un chat_id de Telegram (numerico)
 * y deja ver en el log de que canal vino cada consulta. */
export function identidadCanal(waId: string): string {
  return `wa:${waId}`;
}

/** Marca el mensaje como recibido. Devuelve false si el message_id ya
 * existia, es decir si Meta reintento un webhook ya procesado.
 *
 * Se resuelve con el UNIQUE de la tabla (insertar y mirar el error) en vez
 * de "leer y despues insertar": dos reintentos simultaneos del mismo
 * webhook pasarian los dos la lectura previa y el mensaje se contestaria
 * dos veces. */
export async function reservarMensaje(
  supabase: SupabaseLike,
  mensaje: { messageId: string; waId: string },
): Promise<boolean> {
  const { error } = await supabase
    .from(MENSAJES_TABLE)
    .insert({
      message_id: mensaje.messageId,
      wa_id: mensaje.waId,
      status: "recibido",
    });

  if (!error) return true;

  // 23505 = unique_violation en Postgres.
  if (error?.code === "23505") return false;

  throw error;
}

export async function cerrarMensaje(
  supabase: SupabaseLike,
  messageId: string,
  status: "procesado" | "error" | "ignorado",
  comando?: string,
  errorDetalle?: string,
): Promise<void> {
  await supabase
    .from(MENSAJES_TABLE)
    .update({
      status,
      processed_at: new Date().toISOString(),
      comando: comando ?? null,
      error_detalle: errorDetalle ? errorDetalle.slice(0, 500) : null,
    })
    .eq("message_id", messageId);
}

/** Registra (o refresca) al usuario de WhatsApp y devuelve su nivel de plan.
 * Un usuario nuevo queda en "gratis": no hay alta automatica de planes por
 * este canal en el MVP. */
export async function upsertUsuarioWhatsApp(
  supabase: SupabaseLike,
  mensaje: { waId: string; telefono: string; nombrePerfil: string },
): Promise<string> {
  const { data: existente } = await supabase
    .from(USUARIOS_TABLE)
    .select("id, nivel, mensajes_recibidos")
    .eq("wa_id", mensaje.waId)
    .maybeSingle();

  if (existente) {
    await supabase
      .from(USUARIOS_TABLE)
      .update({
        last_seen_at: new Date().toISOString(),
        mensajes_recibidos: (existente.mensajes_recibidos ?? 0) + 1,
        nombre_perfil: mensaje.nombrePerfil || null,
      })
      .eq("wa_id", mensaje.waId);

    return existente.nivel ?? "gratis";
  }

  await supabase.from(USUARIOS_TABLE).insert({
    wa_id: mensaje.waId,
    telefono: mensaje.telefono,
    nombre_perfil: mensaje.nombrePerfil || null,
    mensajes_recibidos: 1,
    last_seen_at: new Date().toISOString(),
  });

  return "gratis";
}

export async function logConsulta(
  supabase: SupabaseLike,
  params: {
    waId: string;
    command: string;
    queryText?: string;
    resultCount?: number;
    status: string;
  },
): Promise<void> {
  try {
    await supabase.from(CONSULTAS_TABLE).insert({
      telegram_chat_id: identidadCanal(params.waId),
      telegram_user_id: identidadCanal(params.waId),
      command: params.command,
      query_text: params.queryText ?? null,
      result_count: params.resultCount ?? 0,
      status: params.status,
      raw: { canal: "whatsapp" },
    });
  } catch (_error) {
    // El log nunca debe impedir responder al usuario.
  }
}

export async function contarConsultasIaHoy(
  supabase: SupabaseLike,
  waId: string,
): Promise<number> {
  const { count, error } = await supabase
    .from(CONSULTAS_TABLE)
    .select("id", { count: "exact", head: true })
    .eq("telegram_chat_id", identidadCanal(waId))
    .eq("command", "/consulta")
    .gte("created_at", getLimaStartOfDayIso());

  if (error) throw error;

  return count ?? 0;
}
