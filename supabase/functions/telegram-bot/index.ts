import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";

const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ??
  JSON.parse(Deno.env.get("SUPABASE_SECRET_KEYS") ?? "{}").service_role ??
  "";

const TELEGRAM_BOT_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const BOT_ALLOWED_CHAT_IDS = Deno.env.get("BOT_ALLOWED_CHAT_IDS") ?? "";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`;
type TelegramUpdate = {
  message?: {
    message_id: number;
    chat: {
      id: number | string;
      first_name?: string;
      username?: string;
    };
    from?: {
      id: number | string;
      first_name?: string;
      username?: string;
    };
    text?: string;
  };
  callback_query?: {
    id: string;
    from: {
      id: number | string;
      first_name?: string;
      username?: string;
    };
    message?: {
      message_id: number;
      chat: {
        id: number | string;
      };
    };
    data?: string;
  };
};

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function isAllowed(chatId: string): boolean {
  if (!BOT_ALLOWED_CHAT_IDS.trim()) {
    return true;
  }

  const allowed = BOT_ALLOWED_CHAT_IDS
    .split(",")
    .map((item: string) => item.trim())
    .filter((item: string) => item.length > 0);

  return allowed.includes(chatId);
}

function mainMenu() {
  return {
    inline_keyboard: [
      [{ text: "🚨 Alertas DIGEMID", callback_data: "menu:alertas" }],
      [
        { text: "🆕 Últimas", callback_data: "alertas:ultimas" },
        { text: "🗓️ Este mes", callback_data: "alertas:mes" },
      ],
      [
        { text: "📅 Hoy", callback_data: "alertas:hoy" },
        { text: "🔎 Buscar", callback_data: "alertas:buscar_info" },
      ],
      [{ text: "ℹ️ Ayuda", callback_data: "menu:ayuda" }],
    ],
  };
}

function alertasMenu() {
  return {
    inline_keyboard: [
      [
        { text: "🆕 Últimas 5", callback_data: "alertas:ultimas" },
        { text: "📅 Hoy", callback_data: "alertas:hoy" },
      ],
      [
        { text: "📆 Semana", callback_data: "alertas:semana" },
        { text: "🕒 Recientes", callback_data: "alertas:recientes" },
      ],
      [
        { text: "🗓️ Este mes", callback_data: "alertas:mes" },
        { text: "🔢 Por número", callback_data: "alertas:numero_info" },
      ],
      [{ text: "🔎 Buscar por palabra", callback_data: "alertas:buscar_info" }],
      [{ text: "⬅️ Volver", callback_data: "menu:principal" }],
    ],
  };
}

async function telegram(method: string, payload: Record<string, unknown>) {
  const response = await fetch(`${TELEGRAM_API}/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Telegram error ${response.status}: ${text}`);
  }

  return await response.json();
}

async function sendMessage(
  chatId: string,
  text: string,
  replyMarkup?: Record<string, unknown>,
) {
  return await telegram("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    reply_markup: replyMarkup,
  });
}

async function editMessage(
  chatId: string,
  messageId: number,
  text: string,
  replyMarkup?: Record<string, unknown>,
) {
  return await telegram("editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    parse_mode: "HTML",
    disable_web_page_preview: false,
    reply_markup: replyMarkup,
  });
}

async function answerCallback(callbackId: string) {
  return await telegram("answerCallbackQuery", {
    callback_query_id: callbackId,
  });
}

async function logConsulta(params: {
  chatId: string;
  userId?: string;
  command: string;
  queryText?: string;
  resultCount?: number;
  status: string;
  raw?: Record<string, unknown>;
}) {
  try {
    await supabase.from("digemid_bot_consultas").insert({
      telegram_chat_id: params.chatId,
      telegram_user_id: params.userId ?? null,
      command: params.command,
      query_text: params.queryText ?? null,
      result_count: params.resultCount ?? 0,
      status: params.status,
      raw: params.raw ?? {},
    });
  } catch (_error) {
    // No bloquea la respuesta del bot.
  }
}

async function upsertUsuario(update: TelegramUpdate, chatId: string) {
  const from = update.message?.from ?? update.callback_query?.from;

  if (!from) return;

  try {
    await supabase.from("digemid_bot_usuarios").upsert(
      {
        telegram_chat_id: chatId,
        telegram_user_id: String(from.id),
        nombre: from.first_name ?? null,
        username: from.username ?? null,
        estado: "activo",
        last_seen_at: new Date().toISOString(),
      },
      {
        onConflict: "telegram_chat_id",
      },
    );
  } catch (_error) {
    // No bloquea la respuesta del bot.
  }
}

function formatAlertList(title: string, rows: any[]) {
  if (!rows.length) {
    return `${title}\n\n📭 No encontré alertas para esta consulta.`;
  }

  const lines = [title, ""];

  for (const row of rows) {
    lines.push(`🚨 <b>${escapeHtml(row.alert_number)}</b>`);
    lines.push(`📌 ${escapeHtml(row.alert_title)}`);
    lines.push(
      `📅 ${escapeHtml(row.published_date_display ?? row.published_date ?? "Sin fecha")}`,
    );
    lines.push(`🔗 ${escapeHtml(row.detail_url)}`);
    lines.push("");
  }

  lines.push(`✅ Total mostrado: ${rows.length}`);

  return lines.join("\n");
}

function formatAlertDetail(row: any) {
  const pdfUrl = row.drive_file_url || row.drive_download_url || row.pdf_source_url;

  const lines = [
    `🚨 <b>Alerta DIGEMID N.° ${escapeHtml(row.alert_number)}</b>`,
    "",
    "📌 <b>Título:</b>",
    escapeHtml(row.alert_title),
    "",
    `📅 <b>Publicación:</b> ${escapeHtml(row.published_date_display ?? row.published_date ?? "Sin fecha")}`,
    `📋 <b>Estado:</b> ${escapeHtml(row.process_status ?? "Registrada")}`,
    "",
    "📎 <b>Documento:</b>",
    pdfUrl ? "PDF disponible" : "PDF aún no registrado en el sistema",
  ];

  return lines.join("\n");
}

function detailButtons(row: any) {
  const buttons: any[] = [];

  if (row.detail_url) {
    buttons.push([{ text: "🔗 Ver alerta", url: row.detail_url }]);
  }

  const pdfUrl = row.drive_file_url || row.drive_download_url || row.pdf_source_url;

  if (pdfUrl) {
    buttons.push([{ text: "⬇️ Abrir PDF", url: pdfUrl }]);
  }

  buttons.push([{ text: "⬅️ Volver a alertas", callback_data: "menu:alertas" }]);

  return {
    inline_keyboard: buttons,
  };
}

function getLimaDateParts() {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Lima",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
  });

  const parts = formatter.formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value ?? "1970";
  const month = parts.find((part) => part.type === "month")?.value ?? "01";
  const day = parts.find((part) => part.type === "day")?.value ?? "01";
  const weekdayLabel = parts.find((part) => part.type === "weekday")?.value ?? "Mon";

  const weekdayMap: Record<string, number> = {
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
    Sun: 7,
  };

  return {
    isoDate: `${year}-${month}-${day}`,
    isoWeekday: weekdayMap[weekdayLabel] ?? 1,
  };
}

function shiftIsoDate(isoDate: string, days: number) {
  const [year, month, day] = isoDate.split("-").map(Number);
  const utcDate = new Date(Date.UTC(year, month - 1, day));
  utcDate.setUTCDate(utcDate.getUTCDate() + days);
  return utcDate.toISOString().slice(0, 10);
}

function getCurrentWeekBounds() {
  const { isoDate, isoWeekday } = getLimaDateParts();
  const weekStart = shiftIsoDate(isoDate, -(isoWeekday - 1));
  const weekEnd = shiftIsoDate(weekStart, 6);

  return { weekStart, weekEnd };
}

function formatWeekAlertList(rows: any[], total: number, shownLimit: number) {
  const title = "📅 <b>Alertas DIGEMID de esta semana</b>";

  if (!rows.length) {
    return [
      title,
      "",
      "No se encontraron alertas publicadas esta semana.",
      "",
      "Puedes probar con /ultimas.",
    ].join("\n");
  }

  const lines = [title, ""];

  if (total > shownLimit) {
    lines.push(`Mostrando las últimas ${shownLimit} alertas de esta semana.`);
    lines.push("");
  }

  rows.forEach((row, index) => {
    lines.push(`${index + 1}. <b>Alerta DIGEMID N° ${escapeHtml(row.document_key)}</b>`);
    lines.push(`Fecha: ${escapeHtml(row.published_date_display ?? row.published_date ?? "Sin fecha")}`);
    lines.push(`Estado: ${escapeHtml(row.process_status ?? "Sin estado")}`);
    lines.push(`Sección: ${escapeHtml(row.source_section ?? "Sin sección")}`);

    if (row.title) {
      lines.push(`Título: ${escapeHtml(row.title)}`);
    }

    if (row.file_url) {
      lines.push(`PDF: ${escapeHtml(row.file_url)}`);
    }

    if (row.detail_url) {
      lines.push(`Detalle: ${escapeHtml(row.detail_url)}`);
    }

    lines.push("");
  });

  lines.push(`Total: ${total} ${total === 1 ? "alerta encontrada." : "alertas encontradas."}`);

  return lines.join("\n");
}

function formatCreatedAtSimple(value: string | null | undefined) {
  if (!value) {
    return "Sin fecha";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return new Intl.DateTimeFormat("es-PE", {
    timeZone: "America/Lima",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function formatRecentAlertList(rows: any[]) {
  const title = "🕒 <b>Alertas registradas recientemente</b>";

  if (!rows.length) {
    return [
      title,
      "",
      "No se encontraron alertas registradas en los últimos 7 días.",
      "",
      "Puedes probar con /ultimas o /semana.",
    ].join("\n");
  }

  const lines = [title, ""];

  rows.forEach((row, index) => {
    lines.push(`${index + 1}. <b>Alerta DIGEMID N° ${escapeHtml(row.document_key)}</b>`);
    lines.push(`Fecha publicada: ${escapeHtml(row.published_date_display ?? row.published_date ?? "Sin fecha")}`);
    lines.push(`Registrada: ${escapeHtml(formatCreatedAtSimple(row.created_at))}`);
    lines.push(`Estado: ${escapeHtml(row.process_status ?? "Sin estado")}`);
    lines.push(`Sección: ${escapeHtml(row.source_section ?? "Sin sección")}`);

    if (row.title) {
      lines.push(`Título: ${escapeHtml(row.title)}`);
    }

    if (row.file_url) {
      lines.push(`PDF: ${escapeHtml(row.file_url)}`);
    }

    if (row.detail_url) {
      lines.push(`Detalle: ${escapeHtml(row.detail_url)}`);
    }

    lines.push("");
  });

  lines.push(`Total: ${rows.length} ${rows.length === 1 ? "alerta encontrada." : "alertas encontradas."}`);

  return lines.join("\n");
}

const ALERT_SELECT =
  "alert_number, alert_title, published_date, published_date_display, detail_url, pdf_source_url, drive_file_url, drive_download_url, process_status";
const WEEK_ALERT_SELECT =
  "document_key, title, published_date, published_date_display, source_section, file_url, detail_url, process_status";
const RECENT_ALERT_SELECT =
  "document_key, title, published_date, published_date_display, created_at, source_section, file_url, detail_url, process_status";

async function getLatestAlerts(limit = 5) {
  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .order("published_date", { ascending: false })
    .limit(limit);

  if (error) throw error;

  return data ?? [];
}

async function getTodayAlerts() {
  const today = new Date().toISOString().slice(0, 10);

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .eq("published_date", today)
    .order("alert_number", { ascending: false });

  if (error) throw error;

  return data ?? [];
}

async function getMonthAlerts() {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));

  const startIso = start.toISOString().slice(0, 10);
  const nextIso = next.toISOString().slice(0, 10);

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .gte("published_date", startIso)
    .lt("published_date", nextIso)
    .order("published_date", { ascending: false })
    .limit(20);

  if (error) throw error;

  return data ?? [];
}

async function getAlertasSemana(limit = 10) {
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

async function getRecentAlerts(limit = 10) {
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

async function searchAlerts(query: string) {
  const cleanQuery = query.trim();

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .ilike("alert_title", `%${cleanQuery}%`)
    .order("published_date", { ascending: false })
    .limit(10);

  if (error) throw error;

  return data ?? [];
}

async function getAlertDetail(alertNumber: string) {
  const cleanNumber = alertNumber.trim();

  const { data, error } = await supabase
    .from("digemid_alertas_v")
    .select(ALERT_SELECT)
    .eq("alert_number", cleanNumber)
    .limit(1)
    .maybeSingle();

  if (error) throw error;

  return data;
}

async function handleCommand(
  chatId: string,
  userId: string | undefined,
  text: string,
) {
  const trimmed = text.trim();

  if (trimmed === "/start" || trimmed === "/menu") {
    await logConsulta({ chatId, userId, command: "/start", status: "ok" });

    return await sendMessage(
      chatId,
      [
        "🤖 <b>RegAlert DIGEMID</b>",
        "",
        "Hola, Edwin.",
        "Puedo ayudarte a consultar alertas registradas de DIGEMID.",
        "",
        "Selecciona una opción:",
      ].join("\n"),
      mainMenu(),
    );
  }

  if (trimmed === "/ayuda") {
    await logConsulta({ chatId, userId, command: "/ayuda", status: "ok" });

    return await sendMessage(
      chatId,
      [
        "ℹ️ <b>Ayuda RegAlert DIGEMID</b>",
        "",
        "Comandos disponibles:",
        "",
        "🚨 /alertas — Menú de alertas",
        "🆕 /ultimas — Últimas alertas",
        "📅 /hoy — Alertas publicadas hoy",
        "📆 /semana — Alertas publicadas esta semana",
        "🕒 /recientes — Alertas registradas en los últimos 7 días",
        "🗓️ /mes — Alertas del mes",
        "🔢 /detalle 50-2026 — Ver detalle",
        "🔎 /buscar texto — Buscar por palabra",
      ].join("\n"),
      mainMenu(),
    );
  }

  if (trimmed === "/alertas") {
    await logConsulta({ chatId, userId, command: "/alertas", status: "ok" });

    return await sendMessage(
      chatId,
      "🚨 <b>Alertas DIGEMID</b>\n\n¿Qué deseas consultar?",
      alertasMenu(),
    );
  }

  if (trimmed === "/ultimas") {
    const rows = await getLatestAlerts(5);
    await logConsulta({
      chatId,
      userId,
      command: "/ultimas",
      resultCount: rows.length,
      status: "ok",
    });

    return await sendMessage(
      chatId,
      formatAlertList("🆕 <b>Últimas alertas DIGEMID</b>", rows),
      alertasMenu(),
    );
  }

  if (trimmed === "/hoy") {
    const rows = await getTodayAlerts();
    await logConsulta({
      chatId,
      userId,
      command: "/hoy",
      resultCount: rows.length,
      status: "ok",
    });

    return await sendMessage(
      chatId,
      formatAlertList("📅 <b>Alertas DIGEMID de hoy</b>", rows),
      alertasMenu(),
    );
  }

  if (trimmed === "/semana") {
    const { rows, total } = await getAlertasSemana(10);
    await logConsulta({
      chatId,
      userId,
      command: "/semana",
      resultCount: rows.length,
      status: "ok",
    });

    return await sendMessage(
      chatId,
      formatWeekAlertList(rows, total, 10),
      alertasMenu(),
    );
  }

  if (trimmed === "/recientes") {
    const rows = await getRecentAlerts(10);
    await logConsulta({
      chatId,
      userId,
      command: "/recientes",
      resultCount: rows.length,
      status: "ok",
    });

    return await sendMessage(
      chatId,
      formatRecentAlertList(rows),
      alertasMenu(),
    );
  }

  if (trimmed === "/mes") {
    const rows = await getMonthAlerts();
    await logConsulta({
      chatId,
      userId,
      command: "/mes",
      resultCount: rows.length,
      status: "ok",
    });

    return await sendMessage(
      chatId,
      formatAlertList("🗓️ <b>Alertas DIGEMID del mes</b>", rows),
      alertasMenu(),
    );
  }

  if (trimmed.startsWith("/detalle")) {
    const number = trimmed.replace("/detalle", "").trim();

    if (!number) {
      return await sendMessage(
        chatId,
        "🔢 Escribe el número de alerta.\n\nEjemplo:\n<code>/detalle 50-2026</code>",
        alertasMenu(),
      );
    }

    const row = await getAlertDetail(number);

    await logConsulta({
      chatId,
      userId,
      command: "/detalle",
      queryText: number,
      resultCount: row ? 1 : 0,
      status: "ok",
    });

    if (!row) {
      return await sendMessage(
        chatId,
        `📭 No encontré la alerta <b>${escapeHtml(number)}</b>.`,
        alertasMenu(),
      );
    }

    return await sendMessage(chatId, formatAlertDetail(row), detailButtons(row));
  }

  if (trimmed.startsWith("/buscar")) {
    const query = trimmed.replace("/buscar", "").trim();

    if (!query) {
      return await sendMessage(
        chatId,
        "🔎 Escribe una palabra para buscar.\n\nEjemplo:\n<code>/buscar retiro</code>",
        alertasMenu(),
      );
    }

    const rows = await searchAlerts(query);

    await logConsulta({
      chatId,
      userId,
      command: "/buscar",
      queryText: query,
      resultCount: rows.length,
      status: "ok",
    });

    return await sendMessage(
      chatId,
      formatAlertList(`🔎 <b>Resultados para:</b> ${escapeHtml(query)}`, rows),
      alertasMenu(),
    );
  }

  return await sendMessage(
    chatId,
    "No reconocí esa consulta.\n\nUsa /start para ver el menú principal.",
    mainMenu(),
  );
}

async function handleCallback(update: TelegramUpdate) {
  const callback = update.callback_query!;
  const data = callback.data ?? "";
  const chatId = String(callback.message?.chat.id ?? "");

  await answerCallback(callback.id);

  console.log("HANDLE_CALLBACK_DATA:", data);
  console.log("HANDLE_CALLBACK_CHAT_ID:", chatId);

  if (!chatId) {
    return;
  }

  if (data === "menu:principal") {
    return await sendMessage(
      chatId,
      "🤖 <b>RegAlert DIGEMID</b>\n\nSelecciona una opción:",
      mainMenu(),
    );
  }

  if (data === "menu:alertas") {
    return await sendMessage(
      chatId,
      "🚨 <b>Alertas DIGEMID</b>\n\n¿Qué deseas consultar?",
      alertasMenu(),
    );
  }

  if (data === "menu:ayuda") {
    return await sendMessage(
      chatId,
      [
        "ℹ️ <b>Ayuda RegAlert DIGEMID</b>",
        "",
        "Puedes usar:",
        "",
        "🆕 /ultimas",
        "📅 /hoy",
        "📆 /semana",
        "🕒 /recientes",
        "🗓️ /mes",
        "🔢 /detalle 50-2026",
        "🔎 /buscar texto",
      ].join("\n"),
      mainMenu(),
    );
  }

  if (data === "alertas:ultimas") {
    const rows = await getLatestAlerts(5);

    return await sendMessage(
      chatId,
      formatAlertList("🆕 <b>Últimas alertas DIGEMID</b>", rows),
      alertasMenu(),
    );
  }

  if (data === "alertas:hoy") {
    const rows = await getTodayAlerts();

    return await sendMessage(
      chatId,
      formatAlertList("📅 <b>Alertas DIGEMID de hoy</b>", rows),
      alertasMenu(),
    );
  }

  if (data === "alertas:semana") {
    const { rows, total } = await getAlertasSemana(10);

    return await sendMessage(
      chatId,
      formatWeekAlertList(rows, total, 10),
      alertasMenu(),
    );
  }

  if (data === "alertas:recientes") {
    const rows = await getRecentAlerts(10);

    return await sendMessage(
      chatId,
      formatRecentAlertList(rows),
      alertasMenu(),
    );
  }

  if (data === "alertas:mes") {
    const rows = await getMonthAlerts();

    return await sendMessage(
      chatId,
      formatAlertList("🗓️ <b>Alertas DIGEMID del mes</b>", rows),
      alertasMenu(),
    );
  }

  if (data === "alertas:buscar_info") {
    return await sendMessage(
      chatId,
      "🔎 <b>Buscar alerta</b>\n\nEscribe una consulta así:\n\n<code>/buscar retiro</code>\n<code>/buscar producto</code>",
      alertasMenu(),
    );
  }

  if (data === "alertas:numero_info") {
    return await sendMessage(
      chatId,
      "🔢 <b>Consultar por número</b>\n\nEscribe:\n\n<code>/detalle 50-2026</code>",
      alertasMenu(),
    );
  }

  return await sendMessage(
    chatId,
    "No reconocí esa opción.\n\nVuelve al menú principal.",
    mainMenu(),
  );
}

serve(async (req: Request) => {
  try {
    if (req.method !== "POST") {
      return new Response("RegAlert DIGEMID Telegram Bot OK", {
        status: 200,
      });
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !TELEGRAM_BOT_TOKEN) {
      return new Response("Faltan variables de entorno", {
        status: 500,
      });
    }

    const update = (await req.json()) as TelegramUpdate;
    console.log("UPDATE_RECIBIDO:", JSON.stringify(update));
    console.log("TIENE_MESSAGE:", Boolean(update.message));
    console.log("TIENE_CALLBACK:", Boolean(update.callback_query));
    console.log("CALLBACK_DATA:", update.callback_query?.data ?? null);

    const chatId = String(
      update.message?.chat.id ?? update.callback_query?.message?.chat.id ?? "",
    );

    const userId = String(
      update.message?.from?.id ?? update.callback_query?.from?.id ?? "",
    );

    if (!chatId) {
      return new Response("Sin chat_id", { status: 200 });
    }

    if (!isAllowed(chatId)) {
      await sendMessage(chatId, "⛔ No tienes acceso autorizado a este bot.");
      return new Response("No autorizado", { status: 200 });
    }

    await upsertUsuario(update, chatId);

    if (update.callback_query) {
      await handleCallback(update);
      return new Response("OK", { status: 200 });
    }

    const text = update.message?.text ?? "/start";
    await handleCommand(chatId, userId, text);

    return new Response("OK", { status: 200 });
  } catch (error) {
    console.error(error);

    return new Response("Error interno", {
      status: 500,
    });
  }
});
