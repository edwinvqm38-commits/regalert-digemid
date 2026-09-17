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

/** Dias de prueba gratuita completa desde el primer contacto (created_at).
 * Pasado este plazo, un usuario en nivel "gratis" pierde el acceso por
 * completo (no solo el limite diario de IA) hasta que tenga un plan pagado. */
export const PRUEBA_LIMITE_DIAS = 7;

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

export type UsuarioWhatsApp = {
  nivel: string;
  createdAt: string;
};

/** Registra (o refresca) al usuario de WhatsApp y devuelve su nivel de plan
 * junto con la fecha de su primer contacto (created_at), que es el punto de
 * partida de la prueba gratuita. Un usuario nuevo queda en "gratis": no hay
 * alta automatica de planes por este canal en el MVP. */
export async function upsertUsuarioWhatsApp(
  supabase: SupabaseLike,
  mensaje: { waId: string; telefono: string; nombrePerfil: string },
): Promise<UsuarioWhatsApp> {
  const { data: existente } = await supabase
    .from(USUARIOS_TABLE)
    .select("id, nivel, mensajes_recibidos, created_at")
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

    return { nivel: existente.nivel ?? "gratis", createdAt: existente.created_at };
  }

  const creadoEn = new Date().toISOString();

  await supabase.from(USUARIOS_TABLE).insert({
    wa_id: mensaje.waId,
    telefono: mensaje.telefono,
    nombre_perfil: mensaje.nombrePerfil || null,
    mensajes_recibidos: 1,
    last_seen_at: creadoEn,
    created_at: creadoEn,
  });

  return { nivel: "gratis", createdAt: creadoEn };
}

/** true mientras el usuario siga dentro de los PRUEBA_LIMITE_DIAS desde su
 * primer contacto. Pura: no depende de la hora del sistema mas alla del
 * parametro `ahora`, para poder probarla sin mockear Date. */
export function enPeriodoDePrueba(
  createdAt: string,
  ahora: Date = new Date(),
  diasLimite = PRUEBA_LIMITE_DIAS,
): boolean {
  const inicio = new Date(createdAt).getTime();
  if (Number.isNaN(inicio)) return false;

  const limiteMs = diasLimite * 24 * 60 * 60 * 1000;
  return ahora.getTime() - inicio < limiteMs;
}

/** Un usuario tiene acceso si tiene un plan pagado, o si su plan gratis
 * todavia esta dentro del periodo de prueba. Pasada la prueba sin plan
 * pagado, se corta el acceso por completo (no solo el limite diario de IA):
 * asi lo decidio el negocio para el canal de WhatsApp. */
export function usuarioTieneAcceso(usuario: UsuarioWhatsApp, ahora: Date = new Date()): boolean {
  if (usuario.nivel !== "gratis") return true;
  return enPeriodoDePrueba(usuario.createdAt, ahora);
}

/** Dias enteros que le quedan de prueba a partir de `ahora` (0 si ya vencio).
 * Redondea hacia arriba: a un usuario que arranco hace 6 dias y 1 hora le
 * quedan "1 dia", no "0.96". Pura, para no depender del reloj del sistema en
 * los tests. */
export function diasRestantesPrueba(
  createdAt: string,
  ahora: Date = new Date(),
  diasLimite = PRUEBA_LIMITE_DIAS,
): number | null {
  const inicio = new Date(createdAt).getTime();
  if (Number.isNaN(inicio)) return null;

  const finMs = inicio + diasLimite * 24 * 60 * 60 * 1000;
  const restanteMs = finMs - ahora.getTime();

  return Math.max(0, Math.ceil(restanteMs / (24 * 60 * 60 * 1000)));
}

export type ResumenUsuarioAdmin = {
  waId: string;
  nombrePerfil: string | null;
  nivel: string;
  createdAt: string;
  /** null cuando el usuario tiene un plan pagado (la prueba no le aplica). */
  diasRestantesPrueba: number | null;
};

// deno-lint-ignore no-explicit-any
function aResumenAdmin(fila: any, ahora: Date): ResumenUsuarioAdmin {
  return {
    waId: fila.wa_id,
    nombrePerfil: fila.nombre_perfil ?? null,
    nivel: fila.nivel,
    createdAt: fila.created_at,
    diasRestantesPrueba: fila.nivel === "gratis" ? diasRestantesPrueba(fila.created_at, ahora) : null,
  };
}

/** Para el comando admin "usuarios": los mas recientes primero, sin
 * distinguir estado (uso general, no pensado para vencimientos). */
export async function listarUsuariosRecientes(
  supabase: SupabaseLike,
  limite = 20,
): Promise<ResumenUsuarioAdmin[]> {
  const { data, error } = await supabase
    .from(USUARIOS_TABLE)
    .select("wa_id, nombre_perfil, nivel, created_at")
    .order("created_at", { ascending: false })
    .limit(limite);

  if (error) throw error;

  const ahora = new Date();
  return (data ?? []).map((fila: unknown) => aResumenAdmin(fila, ahora));
}

/** Para el comando admin "vencen [dias]": usuarios en plan gratis cuya
 * prueba termina dentro de la ventana pedida, pero que TODAVIA no vencio
 * (0 dias restantes = ya vencido, no es "por vencer"). No envia nada por su
 * cuenta: solo lista, para que el admin decida que hacer manualmente. */
export async function listarUsuariosPorVencer(
  supabase: SupabaseLike,
  diasVentana = 3,
  limite = 30,
): Promise<ResumenUsuarioAdmin[]> {
  const { data, error } = await supabase
    .from(USUARIOS_TABLE)
    .select("wa_id, nombre_perfil, nivel, created_at")
    .eq("nivel", "gratis")
    .order("created_at", { ascending: true });

  if (error) throw error;

  const ahora = new Date();
  return (data ?? [])
    .map((fila: unknown) => aResumenAdmin(fila, ahora))
    .filter((u: ResumenUsuarioAdmin) => u.diasRestantesPrueba !== null && u.diasRestantesPrueba > 0 &&
      u.diasRestantesPrueba <= diasVentana)
    .slice(0, limite);
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
