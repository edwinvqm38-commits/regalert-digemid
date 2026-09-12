/** Acceso a los datos de DIGEMID (alertas y normativa), independiente del
 * canal que los pida.
 *
 * Las consultas, el orden y los desempates replican exactamente los que ya
 * usa supabase/functions/telegram-bot/index.ts, para que una misma pregunta
 * responda lo mismo en Telegram y en WhatsApp. El cliente de Supabase se
 * recibe por parametro (no se crea aqui) para poder probar estas funciones
 * sin credenciales ni red.
 *
 * Distincion que NO se debe perder (es juridica, no cosmetica):
 *   published_date = fecha oficial de publicacion del documento.
 *   created_at     = fecha en que RegAlert registro el documento.
 * /hoy, /semana y /mes filtran por published_date; /recientes por created_at.
 */

import { getCurrentWeekBounds } from "./fechas-lima.ts";

// deno-lint-ignore no-explicit-any
export type SupabaseLike = any;

export const ALERT_SELECT =
  "id, alert_number, alert_title, published_date, published_date_display, detail_url, pdf_source_url, drive_file_url, drive_download_url, telegram_file_id, process_status";
export const WEEK_ALERT_SELECT =
  "id, document_key, title, published_date, published_date_display, source_section, file_url, detail_url, telegram_file_id, process_status";
export const RECENT_ALERT_SELECT =
  "id, document_key, title, published_date, published_date_display, created_at, source_section, file_url, detail_url, telegram_file_id, process_status";
export const NORMATIVE_SELECT =
  "id, document_key, title, source_section, published_date, published_date_display, detail_url, file_url, process_status, created_at";

/** alert_number/document_key tiene forma "99-2026": ordenarlo como texto
 * rompe el orden numerico apenas hay numeros de distinta cantidad de digitos
 * ("100-2026" quedaria antes que "93-2026"). */
export function numeroAlertaOrdenable(valor: string | null | undefined): number {
  if (!valor) return -1;
  const match = valor.match(/^(\d+)/);
  return match ? parseInt(match[1], 10) : -1;
}

/** DIGEMID publica varias alertas con la misma published_date y el pipeline
 * las inserta en un mismo upsert (mismo created_at). Sin desempate, un LIMIT
 * sobre ese empate deja fuera alertas reales de forma arbitraria. */
// deno-lint-ignore no-explicit-any
export function ordenarPorFechaYNumero(rows: any[]): any[] {
  return [...rows].sort((a, b) => {
    if (a.published_date !== b.published_date) {
      return a.published_date < b.published_date ? 1 : -1;
    }
    return numeroAlertaOrdenable(b.alert_number) - numeroAlertaOrdenable(a.alert_number);
  });
}

export async function getLatestAlerts(supabase: SupabaseLike, limit = 5) {
  const fetchLimit = Math.max(limit * 6, 30);

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .order("published_date", { ascending: false })
    .limit(fetchLimit);

  if (error) throw error;

  return ordenarPorFechaYNumero(data ?? []).slice(0, limit);
}

export async function getTodayAlerts(supabase: SupabaseLike) {
  const today = new Date().toISOString().slice(0, 10);

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .eq("published_date", today);

  if (error) throw error;

  return ordenarPorFechaYNumero(data ?? []);
}

export async function getMonthAlerts(supabase: SupabaseLike) {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .gte("published_date", start.toISOString().slice(0, 10))
    .lt("published_date", next.toISOString().slice(0, 10))
    .order("published_date", { ascending: false })
    .limit(20);

  if (error) throw error;

  return data ?? [];
}

export async function getAlertasSemana(supabase: SupabaseLike, limit = 10) {
  const { weekStart, weekEnd } = getCurrentWeekBounds();

  const { data, error, count } = await supabase
    .from("digemid_documentos")
    .select(WEEK_ALERT_SELECT, { count: "exact" })
    .eq("source_type", "alerta")
    .not("published_date", "is", null)
    .gte("published_date", weekStart)
    .lte("published_date", weekEnd)
    .order("published_date", { ascending: false })
    .order("document_key", { ascending: false })
    .limit(limit);

  if (error) throw error;

  return {
    rows: data ?? [],
    total: count ?? (data?.length ?? 0),
    weekStart,
    weekEnd,
  };
}

export async function getRecentAlerts(supabase: SupabaseLike, limit = 10) {
  const sevenDaysAgoIso = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from("digemid_documentos")
    .select(RECENT_ALERT_SELECT)
    .eq("source_type", "alerta")
    .gte("created_at", sevenDaysAgoIso)
    .order("created_at", { ascending: false })
    .order("published_date", { ascending: false })
    .order("document_key", { ascending: false })
    .limit(limit);

  if (error) throw error;

  return data ?? [];
}

export async function searchAlerts(supabase: SupabaseLike, query: string) {
  const cleanQuery = query.trim();
  const limit = 10;

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .ilike("alert_title", `%${cleanQuery}%`)
    .order("published_date", { ascending: false })
    .limit(Math.max(limit * 4, 40));

  if (error) throw error;

  return ordenarPorFechaYNumero(data ?? []).slice(0, limit);
}

export async function getAlertDetail(supabase: SupabaseLike, alertNumber: string) {
  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .eq("alert_number", alertNumber.trim())
    .limit(1)
    .maybeSingle();

  if (error) throw error;

  return data;
}

/** documento_tipo/documento_subtipo solo viven dentro de "raw" (no llegan
 * como columna): el prefijo del document_key es la forma confiable de
 * mostrar Ley/Decreto/Resolucion. */
const NORMATIVA_TIPO_POR_PREFIJO: Record<string, string> = {
  LEY: "Ley",
  DS: "Decreto Supremo",
  DL: "Decreto Legislativo",
  DU: "Decreto de Urgencia",
  RM: "Resolución Ministerial",
  RD: "Resolución Directoral",
  RS: "Resolución Suprema",
};

const NORMATIVA_TIPO_POR_SECCION: Record<string, string> = {
  "normas-legales": "Norma Legal",
  "resolucion-ministerial": "Resolución Ministerial",
  "decreto-supremo": "Decreto Supremo",
};

// deno-lint-ignore no-explicit-any
export function tipoNormativa(row: any): string {
  const prefijo = String(row.document_key ?? "").split("-")[0]?.toUpperCase();
  if (prefijo && NORMATIVA_TIPO_POR_PREFIJO[prefijo]) {
    return NORMATIVA_TIPO_POR_PREFIJO[prefijo];
  }
  return NORMATIVA_TIPO_POR_SECCION[row.source_section] ?? "Documento normativo";
}

export async function getLatestNormativa(supabase: SupabaseLike, limit = 8) {
  const { data, error } = await supabase
    .from("digemid_documentos")
    .select(NORMATIVE_SELECT)
    .eq("source_type", "normativa")
    .order("published_date", { ascending: false })
    .limit(Math.max(limit * 6, 40));

  if (error) throw error;

  const rows = [...(data ?? [])].sort((a, b) => {
    if (a.published_date !== b.published_date) {
      return a.published_date < b.published_date ? 1 : -1;
    }
    if (a.created_at !== b.created_at) {
      return a.created_at < b.created_at ? 1 : -1;
    }
    return a.id < b.id ? 1 : a.id > b.id ? -1 : 0;
  });

  // DIGEMID a veces lista la misma norma bajo dos secciones; se colapsan por
  // detail_url para no mostrarla dos veces.
  const detallesVistos = new Set<string>();
  // deno-lint-ignore no-explicit-any
  const filasUnicas = rows.filter((row: any) => {
    if (row.detail_url) {
      if (detallesVistos.has(row.detail_url)) return false;
      detallesVistos.add(row.detail_url);
    }
    return true;
  });

  return filasUnicas.slice(0, limit);
}
