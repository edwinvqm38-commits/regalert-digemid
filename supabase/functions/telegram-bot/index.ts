import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";

const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ??
  JSON.parse(Deno.env.get("SUPABASE_SECRET_KEYS") ?? "{}").service_role ??
  "";

const TELEGRAM_BOT_TOKEN = Deno.env.get("TELEGRAM_BOT_TOKEN") ?? "";
const BOT_ALLOWED_CHAT_IDS = Deno.env.get("BOT_ALLOWED_CHAT_IDS") ?? "";
const DEEPSEEK_API_KEY = Deno.env.get("DEEPSEEK_API_KEY") ?? "";
const GEMINI_API_KEY = Deno.env.get("GEMINI_API_KEY") ?? "";
const GEMINI_MODEL = "gemini-flash-latest";
const ADMIN_CHAT_IDS = Deno.env.get("ADMIN_CHAT_IDS") ?? "";
const YAPE_NUMERO = Deno.env.get("YAPE_NUMERO") ?? "";
const YAPE_TITULAR = Deno.env.get("YAPE_TITULAR") ?? "";

const NIVEL_LIMITES_DIARIOS: Record<string, number | null> = {
  gratis: 5,
  basico: 30,
  consultoria: 100,
  empresarial: null,
};

const LIMITE_DIARIO_GLOBAL = 300;

const NIVEL_PRECIOS: Record<string, number> = {
  gratis: 0,
  basico: 29,
  consultoria: 79,
  empresarial: 199,
};

const CONSULTA_SYSTEM_PROMPT = `Eres un asistente que responde preguntas sobre alertas y \
normativa de DIGEMID (Peru) usando UNICAMENTE el texto de los documentos que \
se te entregan como contexto.

Reglas estrictas:
- No inventes datos que no esten en el contexto.
- Si el contexto no contiene la respuesta, dilo explicitamente en vez de adivinar.
- Cita siempre el documento (numero de alerta o codigo de norma), su fecha y la \
PAGINA donde esta el sustento. Cada bloque del contexto indica su document_key y \
su numero de pagina.
- No reemplazas al Director Tecnico ni a la autoridad sanitaria; tu respuesta \
es informativa, no una decision regulatoria.
- Para resaltar nombres de productos, numeros de alerta/norma y terminos clave, \
usa negrita en formato HTML de Telegram: <b>texto</b>. No uses markdown (**texto**).

Estructura SIEMPRE tu respuesta en este formato exacto, pensado para leerse \
rapido en un celular:

<b>[resumen de la respuesta en una sola linea, en negrita]</b>

[2 a 4 lineas de detalle de apoyo, con terminos clave en <b>negrita</b>]

📌 Fuente: <b>[numero de alerta o codigo de norma]</b> — [fecha], pag. [numero de pagina]

No agregues secciones adicionales ni encabezados fuera de esta estructura. El link \
oficial del documento se muestra aparte en un boton, no lo incluyas en el texto.`;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const TELEGRAM_API = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}`;
type TelegramUpdate = {
  message?: {
    message_id: number;
    chat: {
      id: number | string;
      type?: string;
      first_name?: string;
      username?: string;
    };
    from?: {
      id: number | string;
      first_name?: string;
      username?: string;
    };
    text?: string;
    voice?: {
      file_id: string;
      duration?: number;
      mime_type?: string;
    };
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
        type?: string;
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

function formatConsultaAnswer(rawAnswer: string): string {
  // Escapa todo primero (seguridad), y despues convierte negrita en
  // cualquiera de los dos formatos que el modelo pueda haber usado:
  // markdown (**texto**) o HTML real (<b>texto</b>, que quedo escapado).
  const escaped = escapeHtml(rawAnswer);
  return escaped
    .replace(/\*\*(.+?)\*\*/g, "<b>$1</b>")
    .replace(/&lt;b&gt;(.+?)&lt;\/b&gt;/g, "<b>$1</b>");
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

function isAdmin(chatId: string): boolean {
  const admins = ADMIN_CHAT_IDS
    .split(",")
    .map((item: string) => item.trim())
    .filter((item: string) => item.length > 0);

  return admins.includes(chatId);
}

const KEYBOARD_LABEL_COMMANDS: Record<string, string> = {
  "🚨 Últimas alertas": "/ultimas",
  "🔎 Buscar": "/buscar",
  "ℹ️ Ayuda": "/ayuda",
};

async function persistentKeyboard(chatId: string) {
  return {
    keyboard: [
      ["🚨 Últimas alertas", "🔎 Buscar"],
      [await consultaIaLabel(chatId), "ℹ️ Ayuda"],
    ],
    resize_keyboard: true,
    is_persistent: true,
  };
}

function mainMenu(incluirDemo = false) {
  const filas: any[] = [];

  if (incluirDemo) {
    filas.push([{ text: "🧪 Probar una consulta de ejemplo", callback_data: "demo:ejemplo" }]);
  }

  filas.push(
    [{ text: "🚨 Alertas DIGEMID", callback_data: "menu:alertas" }],
    [
      { text: "🆕 Últimas", callback_data: "alertas:ultimas" },
      { text: "📅 Hoy", callback_data: "alertas:hoy" },
    ],
    [
      { text: "📆 Semana", callback_data: "alertas:semana" },
      { text: "🕒 Recientes", callback_data: "alertas:recientes" },
    ],
    [
      { text: "🗓️ Este mes", callback_data: "alertas:mes" },
      { text: "🔎 Buscar", callback_data: "alertas:buscar_info" },
    ],
    [{ text: "💳 Ver planes", callback_data: "menu:planes" }],
    [
      { text: "🪪 Mi perfil", callback_data: "cuenta:miperfil" },
      { text: "📝 Registrarme", callback_data: "cuenta:registrarme_info" },
    ],
    [{ text: "ℹ️ Ayuda", callback_data: "menu:ayuda" }],
  );

  return { inline_keyboard: filas };
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

function planesKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "Solicitar Básico — S/29/mes", callback_data: "plan:basico" }],
      [{ text: "Solicitar Consultoría — S/79/mes", callback_data: "plan:consultoria" }],
      [{ text: "Solicitar Empresarial — S/199/mes", callback_data: "plan:empresarial" }],
    ],
  };
}

const NOMBRES_PLAN: Record<string, string> = {
  basico: "Básico",
  consultoria: "Consultoría",
  empresarial: "Empresarial",
};

function trialKeyboard(nivelInteres?: string) {
  const ordenNiveles = ["basico", "consultoria", "empresarial"];
  const ordenados = nivelInteres
    ? [nivelInteres, ...ordenNiveles.filter((nivel) => nivel !== nivelInteres)]
    : ordenNiveles;

  const filas: { text: string; callback_data: string }[][] = [
    [{ text: "🎁 Empezar prueba gratuita", callback_data: "trial:iniciar" }],
  ];

  for (const nivel of ordenados) {
    const marca = nivel === nivelInteres ? "⭐ " : "";
    filas.push([
      {
        text: `💳 ${marca}${NOMBRES_PLAN[nivel]} — S/${NIVEL_PRECIOS[nivel]}/mes`,
        callback_data: `plan:${nivel}`,
      },
    ]);
  }

  return { inline_keyboard: filas };
}

const TRIAL_TEXTO =
  "🎉 <b>¡Bienvenido a RegAlert DIGEMID!</b>\n\n" +
  "Para usar el bot necesitas elegir una opción:\n" +
  "✅ Alertas de DIGEMID directo a tu Telegram (no solo cuando preguntas)\n" +
  "✅ Hasta 5 consultas con IA al día, citando la norma exacta\n\n" +
  "La <b>prueba gratuita</b> dura hasta <b>14 días o 3 alertas</b>, lo que llegue primero.\n\n" +
  "O, si ya sabes que quieres suscribirte, elige tu plan abajo:";

const ACCESO_REQUERIDO_TEXTO =
  "🔒 <b>Necesitas una prueba gratuita activa o un plan para usar esto.</b>\n\n" +
  "La prueba gratuita dura <b>14 días o 3 alertas</b>, lo que llegue primero. Elige una opción:";

const PLANES_TEXTO_CORTO =
  "• <b>Básico</b> S/29 — 30 consultas/día\n• <b>Consultoría</b> S/79 — 100/día\n• <b>Empresarial</b> S/199 — sin límite";

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

async function getBotIdentity(): Promise<{ username: string; id: number } | null> {
  try {
    const response = await fetch(`${TELEGRAM_API}/getMe`);
    const data = await response.json();
    return data.ok ? { username: data.result.username, id: data.result.id } : null;
  } catch {
    return null;
  }
}

async function consumirInvitacion(codigo: string, chatId: string): Promise<void> {
  const { data: invitacion, error } = await supabase
    .from("digemid_bot_invitaciones")
    .select("id, telefono, nombre, estado")
    .eq("codigo", codigo)
    .maybeSingle();

  if (error || !invitacion || invitacion.estado !== "pendiente") {
    return;
  }

  await supabase
    .from("digemid_bot_invitaciones")
    .update({ estado: "usado", telegram_chat_id: chatId, used_at: new Date().toISOString() })
    .eq("id", invitacion.id);

  // Invitacion directa = caso manual: queda exenta de la prueba con limite
  // de tiempo/alertas, igual que los usuarios ya existentes antes del cambio.
  const actualizaciones: Record<string, unknown> = { plan_gratis_legado: true };
  if (invitacion.telefono) actualizaciones.telefono = invitacion.telefono;
  if (invitacion.nombre) actualizaciones.nombre = invitacion.nombre;

  await supabase
    .from("digemid_bot_usuarios")
    .update(actualizaciones)
    .eq("telegram_chat_id", chatId);

  const admins = ADMIN_CHAT_IDS
    .split(",")
    .map((item: string) => item.trim())
    .filter((item: string) => item.length > 0);

  const nombreMostrado = invitacion.nombre || "Usuario nuevo";

  for (const adminId of admins) {
    await sendMessage(
      adminId,
      `🆕 <b>Nuevo usuario registrado</b>\n\nNombre: ${escapeHtml(nombreMostrado)}\nTeléfono: ${escapeHtml(invitacion.telefono ?? "sin dato")}\nchat_id: <code>${escapeHtml(chatId)}</code>\n\nUsa <code>/activar ${escapeHtml(chatId)} nivel dias</code> para darle un plan.`,
    );
  }
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

async function upsertUsuario(update: TelegramUpdate, chatId: string): Promise<{ isNew: boolean }> {
  const from = update.message?.from ?? update.callback_query?.from;

  if (!from) return { isNew: false };

  try {
    const { data: existing } = await supabase
      .from("digemid_bot_usuarios")
      .select("id")
      .eq("telegram_chat_id", chatId)
      .maybeSingle();

    if (existing) {
      // No tocamos "nombre" aqui: /renombrar o /registrarme pueden haberlo
      // personalizado, y no queremos que un mensaje cualquiera lo pise con
      // el nombre de Telegram.
      await supabase
        .from("digemid_bot_usuarios")
        .update({
          telegram_user_id: String(from.id),
          username: from.username ?? null,
          estado: "activo",
          last_seen_at: new Date().toISOString(),
        })
        .eq("telegram_chat_id", chatId);

      return { isNew: false };
    }

    await supabase.from("digemid_bot_usuarios").insert({
      telegram_chat_id: chatId,
      telegram_user_id: String(from.id),
      nombre: from.first_name ?? null,
      username: from.username ?? null,
      estado: "activo",
      last_seen_at: new Date().toISOString(),
    });

    return { isNew: true };
  } catch (_error) {
    // No bloquea la respuesta del bot.
    return { isNew: false };
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

function mainMenuText() {
  return [
    "🤖 <b>RegAlert DIGEMID</b>",
    "",
    "Selecciona una opción:",
  ].join("\n");
}

function helpText(esAdmin = false) {
  const base = [
    "ℹ️ <b>Comandos disponibles</b>",
    "",
    "<b>/start</b>",
    "Inicia el bot y muestra la bienvenida.",
    "",
    "<b>/menu</b>",
    "Muestra el menú principal con botones.",
    "",
    "<b>/ayuda</b>",
    "Muestra esta guía de comandos y opciones.",
    "",
    "<b>/ultimas</b>",
    "Muestra las últimas alertas registradas.",
    "",
    "<b>/hoy</b>",
    "Muestra alertas publicadas hoy.",
    "",
    "<b>/semana</b>",
    "Muestra alertas publicadas oficialmente esta semana usando published_date.",
    "",
    "<b>/mes</b>",
    "Muestra alertas publicadas este mes.",
    "",
    "<b>/recientes</b>",
    "Muestra alertas registradas recientemente en el sistema usando created_at.",
    "",
    "<b>/buscar texto</b>",
    "Busca alertas por palabra clave. Ejemplo: /buscar retiro",
    "",
    "<b>/consulta pregunta</b>",
    "Responde en lenguaje natural citando la alerta/norma fuente. Ejemplo: /consulta que paso con el Opdivo falsificado",
    "",
    "<b>🎙️ Nota de voz</b>",
    "Mándame un audio con tu pregunta y te respondo igual que con /consulta — no hace falta escribir ningún comando.",
    "",
    "<b>/suscribirme nivel</b>",
    "Pide activar un plan pagado (basico, consultoria o empresarial). Ejemplo: /suscribirme basico",
    "",
    "<b>/pague codigo_de_operacion</b>",
    "Reporta el código de operación de tu Yape luego de pagar un plan. Ejemplo: /pague 000123456",
    "",
    "<b>/registrarme Tu Nombre</b>",
    "Registra el nombre con el que quieres identificarte en tu cuenta o membresía.",
    "",
    "<b>/miperfil</b>",
    "Muestra tu nombre registrado y el estado de tu prueba o plan.",
    "",
    "<b>/detalle 50-2026</b>",
    "Consulta una alerta por número o código.",
    "",
    "📌 <b>Opciones del menú</b>",
    "",
    "<b>🆕 Últimas 5</b>",
    "Muestra las últimas 5 alertas.",
    "",
    "<b>📅 Hoy</b>",
    "Muestra alertas publicadas hoy.",
    "",
    "<b>📆 Semana</b>",
    "Muestra alertas publicadas durante la semana actual.",
    "",
    "<b>🕒 Recientes</b>",
    "Muestra alertas registradas recientemente en la base de datos.",
    "",
    "<b>🗓️ Este mes</b>",
    "Muestra alertas publicadas durante el mes actual.",
    "",
    "<b>🔢 Por número</b>",
    "Permite consultar una alerta por número o código, por ejemplo 50-2026.",
    "",
    "<b>🔎 Buscar por palabra</b>",
    "Permite buscar por texto, producto, laboratorio, lote o término relacionado.",
    "",
    "<b>⬅️ Volver</b>",
    "Regresa al menú anterior o al menú principal.",
    "",
    "<b>💳 Ver planes</b>",
    "Muestra los 3 planes pagados con botones para solicitarlos.",
    "",
    "<b>🪪 Mi perfil</b>",
    "Muestra tu nombre registrado y el estado de tu prueba o plan.",
    "",
    "<b>📝 Registrarme</b>",
    "Te recuerda cómo fijar el nombre con el que te identificas.",
    "",
    "Usa /menu para volver al panel principal.",
  ];

  if (!esAdmin) {
    return base.join("\n");
  }

  const admin = [
    "",
    "🔐 <b>Comandos de administrador</b>",
    "",
    "<b>/activar chat_id [nivel dias metodo_pago]</b>",
    "Activa un plan pagado. Sin nivel/dias, muestra botones rapidos.",
    "",
    "<b>/desactivar chat_id</b>",
    "Cancela la suscripcion de un usuario.",
    "",
    "<b>/usuarios</b>",
    "Resumen: total de usuarios, conteo por estado/nivel y pendientes de pago.",
    "",
    "<b>/membresias</b>",
    "Lista completa de suscripciones con fechas de inicio y fin.",
    "",
    "<b>/directorio</b>",
    "Lista a todos por estado (plan activo, prueba activa, sin continuar, nunca empezó) con botón para enviar recordatorio.",
    "",
    "<b>/ingresos</b>",
    "Ingresos del mes actual, desglosados por plan.",
    "",
    "<b>/invitar telefono [nombre]</b>",
    "Genera un enlace de invitacion para un usuario nuevo (WhatsApp + Telegram).",
    "",
    "<b>/renombrar chat_id nombre</b>",
    "Cambia el nombre mostrado de un usuario.",
    "",
    "<b>/gratis chat_id</b>",
    "Deja a esa persona con acceso gratis permanente (caso manual, sin límite de prueba).",
    "",
    "<b>/pagosyape</b>",
    "Suma los montos reales que los usuarios reportaron con /pague este mes (confirmados y pendientes).",
    "",
    "<b>/saldodeepseek</b>",
    "Consulta al instante el saldo de la API de DeepSeek y proyecta días restantes.",
  ];

  return [...base, ...admin].join("\n");
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

function getCurrentMonthBoundsLima() {
  const { isoDate } = getLimaDateParts();
  const [year, month] = isoDate.split("-").map(Number);
  const startIso = `${year}-${String(month).padStart(2, "0")}-01`;
  const nextIso = new Date(Date.UTC(year, month, 1)).toISOString().slice(0, 10);

  return { startIso, nextIso };
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
  "id, alert_number, alert_title, published_date, published_date_display, detail_url, pdf_source_url, drive_file_url, drive_download_url, telegram_file_id, process_status";
const WEEK_ALERT_SELECT =
  "id, document_key, title, published_date, published_date_display, source_section, file_url, detail_url, telegram_file_id, process_status";
const RECENT_ALERT_SELECT =
  "id, document_key, title, published_date, published_date_display, created_at, source_section, file_url, detail_url, telegram_file_id, process_status";

const MAX_PDFS_POR_CONSULTA = 3;

async function enviarPdfAlerta(chatId: string, row: any): Promise<void> {
  if (!row?.id) return;

  const fileRef =
    row.telegram_file_id ||
    row.pdf_source_url ||
    row.file_url ||
    row.drive_file_url ||
    row.drive_download_url;

  if (!fileRef) return;

  const numero = row.alert_number ?? row.document_key ?? "";
  const titulo = String(row.alert_title ?? row.title ?? "").slice(0, 200);

  try {
    const result: any = await telegram("sendDocument", {
      chat_id: chatId,
      document: fileRef,
      caption: `📄 <b>${escapeHtml(numero)}</b> — ${escapeHtml(titulo)}`,
      parse_mode: "HTML",
    });

    if (!row.telegram_file_id) {
      const fileId = result?.result?.document?.file_id;
      if (fileId) {
        await supabase.from("digemid_documentos").update({ telegram_file_id: fileId }).eq("id", row.id);
      }
    }
  } catch (_error) {
    // No bloquea la respuesta del bot si falla el envio del PDF adjunto.
  }
}

async function enviarPdfsAlertas(chatId: string, rows: any[]): Promise<void> {
  for (const row of rows.slice(0, MAX_PDFS_POR_CONSULTA)) {
    await enviarPdfAlerta(chatId, row);
  }
}

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

async function searchConsultaChunks(query: string, limit = 4) {
  // buscar_paginas_texto filtra palabras vacias y ordena por relevancia -
  // websearch_to_tsquery exige que aparezcan todas las palabras, lo cual
  // falla con preguntas en lenguaje natural (ej. "que paso con...").
  const { data, error } = await supabase.rpc("buscar_paginas_texto", {
    query_texto: query,
    limite: limit,
  });

  if (error) throw error;

  return data ?? [];
}

function buildConsultaContext(chunks: any[]) {
  return chunks
    .map((chunk) => {
      return [
        `[Documento ${chunk.document_key} - ${chunk.title} - ${chunk.published_date} - pagina ${chunk.page_number}]`,
        chunk.text_content,
        `Link oficial: ${chunk.detail_url}`,
      ].join("\n");
    })
    .join("\n\n---\n\n");
}

async function callDeepseek(userContent: string): Promise<string> {
  const response = await fetch("https://api.deepseek.com/chat/completions", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${DEEPSEEK_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-chat",
      messages: [
        { role: "system", content: CONSULTA_SYSTEM_PROMPT },
        { role: "user", content: userContent },
      ],
      max_tokens: 1024,
    }),
  });

  if (!response.ok) {
    throw new Error(`DeepSeek error ${response.status}: ${await response.text()}`);
  }

  const data = await response.json();
  return data.choices?.[0]?.message?.content ?? "";
}

async function callGemini(userContent: string): Promise<string> {
  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        system_instruction: { parts: [{ text: CONSULTA_SYSTEM_PROMPT }] },
        contents: [{ role: "user", parts: [{ text: userContent }] }],
        generationConfig: { maxOutputTokens: 1024 },
      }),
    },
  );

  if (!response.ok) {
    throw new Error(`Gemini error ${response.status}: ${await response.text()}`);
  }

  const data = await response.json();
  const parts = data.candidates?.[0]?.content?.parts ?? [];
  return parts.map((p: any) => p.text ?? "").join("");
}

function consultaSources(chunks: any[]) {
  const seen = new Set<string>();
  const sources: { documentKey: string; url: string; page: number }[] = [];

  for (const chunk of chunks) {
    if (!chunk.detail_url || seen.has(chunk.detail_url)) continue;
    seen.add(chunk.detail_url);
    sources.push({
      documentKey: chunk.document_key,
      url: chunk.detail_url,
      page: chunk.page_number ?? 0,
    });
  }

  return sources;
}

async function suggestSimilarAlerts(question: string, limit = 3) {
  const { data, error } = await supabase.rpc("sugerir_alertas_similares", {
    query_texto: question,
    limite: limit,
  });

  if (error) throw error;

  return (data ?? []) as {
    document_key: string;
    title: string;
    published_date: string;
    detail_url: string;
  }[];
}

function getLimaStartOfDayIso(): string {
  // Lima (America/Lima) es UTC-5 todo el año, sin horario de verano.
  const { isoDate } = getLimaDateParts();
  return `${isoDate}T05:00:00.000Z`;
}

async function contarConsultasHoy(chatId?: string): Promise<number> {
  let query = supabase
    .from("digemid_bot_consultas")
    .select("id", { count: "exact", head: true })
    .eq("command", "/consulta")
    .gte("created_at", getLimaStartOfDayIso());

  if (chatId) {
    query = query.eq("telegram_chat_id", chatId);
  }

  const { count, error } = await query;

  if (error) throw error;

  return count ?? 0;
}

async function getNivelUsuario(chatId: string): Promise<string> {
  const { data, error } = await supabase
    .from("digemid_suscripciones")
    .select("nivel, estado, fecha_fin")
    .eq("telegram_chat_id", chatId)
    .maybeSingle();

  if (error || !data || data.estado !== "activo") {
    return "gratis";
  }

  if (data.fecha_fin && data.fecha_fin < getLimaDateParts().isoDate) {
    return "gratis";
  }

  return data.nivel;
}

type EstadoAcceso = {
  legado: boolean;
  pruebaEstado: string | null;
  nivelPagado: string | null; // nivel real si tiene una suscripcion pagada activa
};

async function getEstadoAcceso(chatId: string): Promise<EstadoAcceso> {
  const { data: usuario } = await supabase
    .from("digemid_bot_usuarios")
    .select("plan_gratis_legado, prueba_estado")
    .eq("telegram_chat_id", chatId)
    .maybeSingle();

  const nivel = await getNivelUsuario(chatId);

  return {
    legado: usuario?.plan_gratis_legado ?? false,
    pruebaEstado: usuario?.prueba_estado ?? null,
    nivelPagado: nivel !== "gratis" ? nivel : null,
  };
}

function tieneAccesoActivo(estado: EstadoAcceso): boolean {
  return estado.legado || estado.pruebaEstado === "activa" || Boolean(estado.nivelPagado);
}

async function consultaIaLabel(chatId: string): Promise<string> {
  const estado = await getEstadoAcceso(chatId);

  if (!tieneAccesoActivo(estado)) {
    return "🤖 Consulta IA (activar prueba)";
  }

  const nivel = estado.nivelPagado ?? "gratis";
  const limite = NIVEL_LIMITES_DIARIOS[nivel] ?? NIVEL_LIMITES_DIARIOS.gratis;

  if (limite === null) {
    return "🤖 Consulta IA (sin límite)";
  }

  const usadas = await contarConsultasHoy(chatId);
  const restantes = Math.max(0, limite - usadas);

  return `🤖 Consulta IA (quedan ${restantes})`;
}

function formatResumenUsuarios(totalUsuarios: number, suscripciones: any[]): string {
  const hoy = getLimaDateParts().isoDate;

  const conteoPorEstado: Record<string, number> = {};
  const conteoPorNivel: Record<string, number> = {};
  const pendientes: any[] = [];

  for (const sub of suscripciones) {
    const vencida = sub.estado === "activo" && sub.fecha_fin && sub.fecha_fin < hoy;
    const estadoEfectivo = vencida ? "vencido" : sub.estado;

    conteoPorEstado[estadoEfectivo] = (conteoPorEstado[estadoEfectivo] ?? 0) + 1;

    if (estadoEfectivo === "activo") {
      conteoPorNivel[sub.nivel] = (conteoPorNivel[sub.nivel] ?? 0) + 1;
    }

    if (estadoEfectivo === "pendiente_pago" || estadoEfectivo === "vencido") {
      pendientes.push({ ...sub, estadoEfectivo });
    }
  }

  const lines = [
    "👥 <b>Resumen de usuarios</b>",
    "",
    `Usuarios que han usado el bot: <b>${totalUsuarios}</b>`,
    `Suscripciones registradas: <b>${suscripciones.length}</b>`,
    "",
    "<b>Por estado</b>",
    `✅ Activos: ${conteoPorEstado.activo ?? 0}`,
    `⏳ Pendientes de pago: ${conteoPorEstado.pendiente_pago ?? 0}`,
    `⚠️ Vencidos: ${conteoPorEstado.vencido ?? 0}`,
    `🚫 Cancelados: ${conteoPorEstado.cancelado ?? 0}`,
  ];

  const nivelesPagados = ["basico", "consultoria", "empresarial"].filter(
    (nivel) => conteoPorNivel[nivel],
  );

  if (nivelesPagados.length) {
    lines.push("", "<b>Activos por nivel</b>");
    for (const nivel of nivelesPagados) {
      lines.push(`• ${escapeHtml(nivel)}: ${conteoPorNivel[nivel]}`);
    }
  }

  lines.push("", "<b>Pendientes de seguimiento</b>");

  if (!pendientes.length) {
    lines.push("✅ Nadie pendiente de pago o vencido por ahora.");
  } else {
    for (const sub of pendientes.slice(0, 20)) {
      const etiqueta = sub.estadoEfectivo === "vencido" ? "venció" : "pendiente desde";
      const referencia = sub.telegram_username
        ? `@${sub.telegram_username}`
        : sub.telegram_chat_id;

      lines.push(
        `• <code>${escapeHtml(referencia)}</code> — ${escapeHtml(sub.nivel)} (${etiqueta} ${escapeHtml(sub.fecha_fin ?? "sin fecha")})`,
      );
    }

    if (pendientes.length > 20) {
      lines.push(`… y ${pendientes.length - 20} más.`);
    }
  }

  return lines.join("\n");
}

function referenciaUsuario(sub: any, nombresPorChatId: Map<string, string>): string {
  const nombre = nombresPorChatId.get(sub.telegram_chat_id);

  if (nombre) return nombre;
  if (sub.telegram_username) return `@${sub.telegram_username}`;

  return sub.telegram_chat_id;
}

function formatMembresias(suscripciones: any[], nombresPorChatId: Map<string, string>): string {
  const hoy = getLimaDateParts().isoDate;

  const grupos: Record<string, any[]> = {
    activo: [],
    pendiente_pago: [],
    vencido: [],
    cancelado: [],
  };

  for (const sub of suscripciones) {
    const vencida = sub.estado === "activo" && sub.fecha_fin && sub.fecha_fin < hoy;
    const estadoEfectivo = vencida ? "vencido" : sub.estado;
    (grupos[estadoEfectivo] ?? (grupos[estadoEfectivo] = [])).push(sub);
  }

  const lines = ["📋 <b>Membresías</b>", ""];

  const secciones: [string, string][] = [
    ["activo", "✅ Activas"],
    ["pendiente_pago", "⏳ Pendientes de pago"],
    ["vencido", "⚠️ Vencidas"],
    ["cancelado", "🚫 Canceladas"],
  ];

  for (const [clave, titulo] of secciones) {
    const items = grupos[clave] ?? [];
    if (!items.length) continue;

    lines.push(`<b>${titulo}</b>`);
    for (const sub of items) {
      const referencia = referenciaUsuario(sub, nombresPorChatId);
      lines.push(
        `• <b>${escapeHtml(referencia)}</b> — ${escapeHtml(sub.nivel)} · ${escapeHtml(sub.fecha_inicio ?? "?")} → ${escapeHtml(sub.fecha_fin ?? "sin fecha")}`,
      );
    }
    lines.push("");
  }

  if (!suscripciones.length) {
    lines.push("Todavía no hay ninguna suscripción registrada.");
  }

  return lines.join("\n").trimEnd();
}

function nombreDirectorio(persona: any): string {
  return persona.nombre || (persona.username ? `@${persona.username}` : persona.telegram_chat_id);
}

function formatDirectorio(
  personas: any[],
  subPorChatId: Map<string, { nivel: string; estado: string; fecha_fin: string | null }>,
  hoy: string,
): { texto: string; candidatos: { chatId: string; nombre: string }[] } {
  const gratisPermanente: any[] = [];
  const conPlanActivo: { persona: any; sub: any }[] = [];
  const enPruebaActiva: any[] = [];
  const lapsos: { persona: any; motivo: string }[] = [];
  const sinNada: any[] = [];

  for (const persona of personas) {
    const sub = subPorChatId.get(persona.telegram_chat_id);
    let estadoEfectivo: string | null = null;

    if (sub) {
      const vencida = sub.estado === "activo" && sub.fecha_fin && sub.fecha_fin < hoy;
      estadoEfectivo = vencida ? "vencido" : sub.estado;
    }

    if (estadoEfectivo === "activo") {
      // Un plan pagado activo manda siempre, incluso si la cuenta tambien
      // quedo marcada como gratis_legado (ej. usuarios grandfathered que
      // despues se suscribieron a un plan real).
      conPlanActivo.push({ persona, sub });
    } else if (persona.prueba_estado === "activa") {
      enPruebaActiva.push(persona);
    } else if (persona.plan_gratis_legado) {
      gratisPermanente.push(persona);
    } else if (estadoEfectivo === "vencido") {
      lapsos.push({ persona, motivo: `plan ${sub!.nivel} vencido (${sub!.fecha_fin ?? "sin fecha"})` });
    } else if (estadoEfectivo === "cancelado") {
      lapsos.push({ persona, motivo: `plan ${sub!.nivel} cancelado` });
    } else if (estadoEfectivo === "pendiente_pago") {
      lapsos.push({ persona, motivo: `pidió plan ${sub!.nivel} pero no completó el pago` });
    } else if (persona.prueba_estado === "finalizada") {
      lapsos.push({ persona, motivo: "terminó su prueba gratuita sin suscribirse" });
    } else {
      sinNada.push(persona);
    }
  }

  const lines = ["🗂 <b>Directorio de usuarios</b>", ""];

  lines.push(`<b>♾️ Gratis permanente — caso manual (${gratisPermanente.length})</b>`);
  if (!gratisPermanente.length) {
    lines.push("Nadie por ahora.");
  } else {
    for (const persona of gratisPermanente) {
      lines.push(`• ${escapeHtml(nombreDirectorio(persona))}`);
    }
  }
  lines.push("");

  lines.push(`<b>✅ Con plan activo (${conPlanActivo.length})</b>`);
  if (!conPlanActivo.length) {
    lines.push("Nadie por ahora.");
  } else {
    for (const { persona, sub } of conPlanActivo) {
      lines.push(`• <b>${escapeHtml(nombreDirectorio(persona))}</b> — ${escapeHtml(sub.nivel)} hasta ${escapeHtml(sub.fecha_fin ?? "sin fecha")}`);
    }
  }
  lines.push("");

  lines.push(`<b>🎁 En prueba gratuita activa (${enPruebaActiva.length})</b>`);
  if (!enPruebaActiva.length) {
    lines.push("Nadie por ahora.");
  } else {
    for (const persona of enPruebaActiva) {
      lines.push(`• <b>${escapeHtml(nombreDirectorio(persona))}</b> — ${persona.prueba_alertas_enviadas ?? 0}/3 alertas usadas`);
    }
  }
  lines.push("");

  lines.push(`<b>⏳ Sin continuar — candidatos a recordatorio (${lapsos.length})</b>`);
  if (!lapsos.length) {
    lines.push("Nadie por ahora.");
  } else {
    for (const { persona, motivo } of lapsos) {
      lines.push(`• <b>${escapeHtml(nombreDirectorio(persona))}</b> — ${escapeHtml(motivo)}`);
    }
  }
  lines.push("");

  lines.push(`<b>💤 Nunca empezaron prueba ni plan (${sinNada.length})</b>`);
  if (!sinNada.length) {
    lines.push("Nadie por ahora.");
  } else {
    for (const persona of sinNada) {
      lines.push(`• ${escapeHtml(nombreDirectorio(persona))}`);
    }
  }

  if (lapsos.length) {
    lines.push("", "Toca un botón abajo para enviarle un recordatorio amigable con sus opciones de plan.");
  }

  const candidatos = lapsos.map(({ persona }) => ({
    chatId: persona.telegram_chat_id as string,
    nombre: nombreDirectorio(persona),
  }));

  return { texto: lines.join("\n").trimEnd(), candidatos };
}

function formatIngresos(altas: any[], startIso: string): string {
  const desglose: Record<string, { cantidad: number; subtotal: number }> = {};
  let total = 0;

  for (const sub of altas) {
    const precio = NIVEL_PRECIOS[sub.nivel] ?? 0;
    if (!desglose[sub.nivel]) desglose[sub.nivel] = { cantidad: 0, subtotal: 0 };
    desglose[sub.nivel].cantidad += 1;
    desglose[sub.nivel].subtotal += precio;
    total += precio;
  }

  const mesLabel = new Date(`${startIso}T12:00:00Z`).toLocaleDateString("es-PE", {
    month: "long",
    year: "numeric",
    timeZone: "America/Lima",
  });

  const lines = [
    `💰 <b>Ingresos de ${escapeHtml(mesLabel)}</b>`,
    "",
    `<b>Total: S/ ${total.toFixed(2)}</b>`,
    "",
  ];

  const nivelesOrden = ["basico", "consultoria", "empresarial"];
  const huboAltas = nivelesOrden.some((nivel) => desglose[nivel]);

  if (!huboAltas) {
    lines.push("Todavía no hay altas ni renovaciones pagadas este mes.");
  } else {
    lines.push("<b>Por plan</b>");
    for (const nivel of nivelesOrden) {
      const info = desglose[nivel];
      if (!info) continue;
      lines.push(
        `• ${escapeHtml(nivel)}: ${info.cantidad} × S/ ${NIVEL_PRECIOS[nivel]} = <b>S/ ${info.subtotal.toFixed(2)}</b>`,
      );
    }
  }

  return lines.join("\n");
}

function formatPagosYape(pagos: any[], startIso: string): string {
  let totalConfirmado = 0;
  let totalPendiente = 0;
  let countConfirmado = 0;
  let countPendiente = 0;
  let countRechazado = 0;

  for (const pago of pagos) {
    const monto = Number(pago.monto_esperado) || 0;
    if (pago.estado === "confirmado") {
      totalConfirmado += monto;
      countConfirmado += 1;
    } else if (pago.estado === "pendiente") {
      totalPendiente += monto;
      countPendiente += 1;
    } else if (pago.estado === "rechazado") {
      countRechazado += 1;
    }
  }

  const mesLabel = new Date(`${startIso}T12:00:00Z`).toLocaleDateString("es-PE", {
    month: "long",
    year: "numeric",
    timeZone: "America/Lima",
  });

  return [
    `💸 <b>Pagos Yape reportados de ${escapeHtml(mesLabel)}</b>`,
    "",
    `<b>Confirmados: S/ ${totalConfirmado.toFixed(2)}</b> (${countConfirmado} pago${countConfirmado === 1 ? "" : "s"})`,
    `Pendientes de verificar: S/ ${totalPendiente.toFixed(2)} (${countPendiente})`,
    `Rechazados: ${countRechazado}`,
    "",
    "Nota: esto suma los montos que los usuarios reportaron con /pague. " +
      "Es distinto de /ingresos, que calcula según planes activados × precio de lista.",
  ].join("\n");
}

async function consultarSaldoDeepseek(): Promise<{ balanceUsd: number | null; isAvailable: boolean; raw: any }> {
  const response = await fetch("https://api.deepseek.com/user/balance", {
    headers: { Authorization: `Bearer ${DEEPSEEK_API_KEY}` },
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(`DeepSeek balance error ${response.status}: ${JSON.stringify(data)}`);
  }

  const infos = data.balance_infos ?? [];
  const usd = infos.find((info: any) => info.currency === "USD") ?? infos[0];
  const balanceUsd = usd ? Number(usd.total_balance) : null;
  const isAvailable = Boolean(data.is_available ?? (balanceUsd !== null && balanceUsd > 0));

  return { balanceUsd, isAvailable, raw: data };
}

async function guardarSnapshotSaldoDeepseek(balanceUsd: number | null, isAvailable: boolean, raw: any): Promise<void> {
  try {
    await supabase.from("deepseek_balance_historial").insert({
      balance_usd: balanceUsd,
      is_available: isAvailable,
      raw,
    });
  } catch (error) {
    console.error("No se pudo guardar snapshot de saldo DeepSeek:", error);
  }
}

async function proyectarDiasRestantesDeepseek(balanceActual: number): Promise<number | null> {
  const desde = new Date();
  desde.setDate(desde.getDate() - 14);

  const { data: historial, error } = await supabase
    .from("deepseek_balance_historial")
    .select("checked_at, balance_usd")
    .gte("checked_at", desde.toISOString())
    .order("checked_at", { ascending: true });

  if (error || !historial) return null;

  const filtrado = historial.filter((row: any) => row.balance_usd !== null);
  if (filtrado.length < 2) return null;

  let ultimoIndiceRecarga = -1;
  for (let i = 1; i < filtrado.length; i++) {
    if (filtrado[i].balance_usd > filtrado[i - 1].balance_usd) {
      ultimoIndiceRecarga = i;
    }
  }

  const tramo = ultimoIndiceRecarga >= 0 ? filtrado.slice(ultimoIndiceRecarga) : filtrado;
  if (tramo.length < 2) return null;

  const primero = tramo[0];
  const ultimo = tramo[tramo.length - 1];

  const diasTranscurridos =
    (new Date(ultimo.checked_at).getTime() - new Date(primero.checked_at).getTime()) / 86_400_000;
  const consumoTotal = primero.balance_usd - ultimo.balance_usd;

  if (diasTranscurridos <= 0 || consumoTotal <= 0) return null;

  const consumoDiarioPromedio = consumoTotal / diasTranscurridos;
  return balanceActual / consumoDiarioPromedio;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

/**
 * Descarga una nota de voz de Telegram y la transcribe con Gemini (entiende
 * audio de forma nativa, sin sumar una libreria/proveedor nuevo). Se le pide
 * transcripcion literal, no un resumen, para no perder matices de la
 * pregunta antes de que entre al mismo pipeline de busqueda+respuesta que
 * usa /consulta.
 */
async function transcribirNotaDeVoz(fileId: string): Promise<string> {
  if (!GEMINI_API_KEY) {
    throw new Error("Falta GEMINI_API_KEY para transcribir audio.");
  }

  const fileInfoResponse = await fetch(`${TELEGRAM_API}/getFile?file_id=${fileId}`);
  const fileInfo = await fileInfoResponse.json();

  if (!fileInfo.ok) {
    throw new Error(`No se pudo obtener el archivo de audio: ${JSON.stringify(fileInfo)}`);
  }

  const filePath = fileInfo.result.file_path as string;
  const audioResponse = await fetch(`https://api.telegram.org/file/bot${TELEGRAM_BOT_TOKEN}/${filePath}`);

  if (!audioResponse.ok) {
    throw new Error(`No se pudo descargar el audio (status ${audioResponse.status}).`);
  }

  const audioBuffer = new Uint8Array(await audioResponse.arrayBuffer());
  const audioBase64 = bytesToBase64(audioBuffer);

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              {
                text:
                  "Transcribe este audio en español de forma literal, palabra por palabra, " +
                  "sin traducir ni resumir ni agregar comentarios. Devuelve unicamente el texto transcrito.",
              },
              { inline_data: { mime_type: "audio/ogg", data: audioBase64 } },
            ],
          },
        ],
        generationConfig: { maxOutputTokens: 512 },
      }),
    },
  );

  if (!response.ok) {
    throw new Error(`Gemini (transcripción) error ${response.status}: ${await response.text()}`);
  }

  const data = await response.json();
  const parts = data.candidates?.[0]?.content?.parts ?? [];
  return parts.map((p: any) => p.text ?? "").join("").trim();
}

async function answerConsulta(
  question: string,
): Promise<{ answer: string; sources: { documentKey: string; url: string }[] }> {
  const chunks = await searchConsultaChunks(question);

  if (!chunks.length) {
    const suggestions = await suggestSimilarAlerts(question);

    if (!suggestions.length) {
      return {
        answer: "No encontré documentos relacionados con esa consulta en la base de datos.",
        sources: [],
      };
    }

    return {
      answer: "No encontré una coincidencia exacta para tu pregunta. ¿Quizás te refieres a alguna de estas alertas?",
      sources: suggestions.map((s) => ({ documentKey: s.document_key, url: s.detail_url })),
    };
  }

  const context = buildConsultaContext(chunks);
  const userContent = `Contexto:\n\n${context}\n\nPregunta: ${question}`;
  const sources = consultaSources(chunks);

  if (DEEPSEEK_API_KEY) {
    try {
      return { answer: await callDeepseek(userContent), sources };
    } catch (error) {
      console.error("DeepSeek falló, probando respaldo Gemini:", error);
    }
  }

  if (GEMINI_API_KEY) {
    return { answer: await callGemini(userContent), sources };
  }

  throw new Error("Falta configurar DEEPSEEK_API_KEY (principal) o GEMINI_API_KEY (respaldo)");
}

async function enviarMiPerfil(chatId: string): Promise<void> {
  const { data: usuario, error: usuarioError } = await supabase
    .from("digemid_bot_usuarios")
    .select("nombre, nombre_confirmado, prueba_estado, prueba_alertas_enviadas, prueba_inicio, plan_interes")
    .eq("telegram_chat_id", chatId)
    .maybeSingle();

  if (usuarioError || !usuario) {
    return await sendMessage(chatId, "⚠️ No pude encontrar tu perfil. Escribe /start primero.");
  }

  const { data: suscripcion } = await supabase
    .from("digemid_suscripciones")
    .select("nivel, estado, fecha_fin")
    .eq("telegram_chat_id", chatId)
    .eq("estado", "activo")
    .maybeSingle();

  const lines = [
    "🪪 <b>Tu perfil</b>",
    "",
    `Nombre registrado: <b>${escapeHtml(usuario.nombre ?? "sin registrar")}</b>` +
      (usuario.nombre_confirmado ? "" : " (por defecto — usa /registrarme para elegir el tuyo)"),
  ];

  if (suscripcion) {
    lines.push(`Plan activo: <b>${escapeHtml(NOMBRES_PLAN[suscripcion.nivel] ?? suscripcion.nivel)}</b> hasta ${escapeHtml(suscripcion.fecha_fin ?? "sin fecha")}.`);
  } else if (usuario.prueba_estado === "activa") {
    lines.push(`Prueba gratuita activa: ${usuario.prueba_alertas_enviadas ?? 0}/3 alertas usadas.`);
  } else if (usuario.prueba_estado === "finalizada") {
    lines.push("Tu prueba gratuita ya terminó. Escribe /suscribirme para ver los planes.");
  } else {
    lines.push("Todavía no tienes plan ni prueba gratuita activa. Escribe /suscribirme para empezar.");
  }

  return await sendMessage(chatId, lines.join("\n"));
}

async function solicitarPlan(
  chatId: string,
  userId: string | undefined,
  nivelSolicitado: string,
): Promise<void> {
  await logConsulta({ chatId, userId, command: "/suscribirme", queryText: nivelSolicitado, status: "ok" });

  await supabase
    .from("digemid_bot_usuarios")
    .update({ pago_pendiente_nivel: nivelSolicitado })
    .eq("telegram_chat_id", chatId);

  const precio = NIVEL_PRECIOS[nivelSolicitado];

  if (!YAPE_NUMERO) {
    // Admin aun no configuro el Yape receptor: avisamos y caemos al flujo manual anterior.
    const admins = ADMIN_CHAT_IDS
      .split(",")
      .map((item: string) => item.trim())
      .filter((item: string) => item.length > 0);

    for (const adminId of admins) {
      await sendMessage(
        adminId,
        `⚠️ Falta configurar YAPE_NUMERO. Solicitud pendiente de <code>${escapeHtml(chatId)}</code> para plan <b>${escapeHtml(nivelSolicitado)}</b>. Usa <code>/activar ${escapeHtml(chatId)} ${escapeHtml(nivelSolicitado)} 30</code> para activarlo manualmente.`,
      );
    }

    return await sendMessage(
      chatId,
      `✅ Solicitud registrada. En breve te contactamos para coordinar el pago y activar tu plan <b>${escapeHtml(nivelSolicitado)}</b> (S/${precio}/mes).`,
    );
  }

  await sendMessage(
    chatId,
    `💳 Para activar el plan <b>${escapeHtml(nivelSolicitado)}</b> (S/${precio}/mes):\n\n` +
      `1️⃣ Yapea <b>S/${precio}</b> a este número: <b>${escapeHtml(YAPE_NUMERO)}</b>${YAPE_TITULAR ? ` (${escapeHtml(YAPE_TITULAR)})` : ""}\n` +
      `2️⃣ Cuando termines, escribe aquí:\n<code>/pague codigo_de_operacion</code>\n\n` +
      `El código de operación te lo muestra Yape al confirmar el pago. En cuanto lo verifiquemos, activamos tu plan.`,
  );
}

async function reportarPagoYape(
  chatId: string,
  userId: string | undefined,
  codigoOperacion: string,
): Promise<void> {
  const { data: usuario } = await supabase
    .from("digemid_bot_usuarios")
    .select("nombre, telefono, pago_pendiente_nivel")
    .eq("telegram_chat_id", chatId)
    .maybeSingle();

  const nivel = usuario?.pago_pendiente_nivel;

  if (!nivel || !(nivel in NIVEL_PRECIOS)) {
    return await sendMessage(
      chatId,
      "Primero elige un plan con <code>/suscribirme basico</code> (o consultoria/empresarial) y luego reporta tu pago.",
    );
  }

  const monto = NIVEL_PRECIOS[nivel];
  const nombreMostrado = usuario?.nombre || "Usuario";

  const { data: pago, error } = await supabase
    .from("digemid_pagos_yape")
    .insert({
      chat_id: chatId,
      nivel,
      monto_esperado: monto,
      codigo_operacion: codigoOperacion,
    })
    .select("id")
    .single();

  if (error) {
    if (error.code === "23505") {
      // Codigo de operacion repetido: alguien ya lo registro antes (mismo usuario o
      // alguien que lo comparte). No se acepta un codigo dos veces.
      const admins = ADMIN_CHAT_IDS
        .split(",")
        .map((item: string) => item.trim())
        .filter((item: string) => item.length > 0);

      for (const adminId of admins) {
        await sendMessage(
          adminId,
          `⚠️ <b>Código de operación repetido</b>\n\nchat_id <code>${escapeHtml(chatId)}</code> reportó el código <code>${escapeHtml(codigoOperacion)}</code>, que ya estaba registrado. Revisa si es un intento de reusar/compartir un pago.`,
        );
      }

      return await sendMessage(
        chatId,
        "⚠️ Ese código de operación ya fue registrado antes. Si crees que es un error, contáctanos.",
      );
    }

    return await sendMessage(chatId, `⚠️ Error al reportar el pago: ${escapeHtml(error.message)}`);
  }

  const admins = ADMIN_CHAT_IDS
    .split(",")
    .map((item: string) => item.trim())
    .filter((item: string) => item.length > 0);

  for (const adminId of admins) {
    await sendMessage(
      adminId,
      `💰 <b>Nuevo pago Yape reportado</b>\n\nNombre: ${escapeHtml(nombreMostrado)}\nTeléfono: ${escapeHtml(usuario?.telefono ?? "sin dato")}\nchat_id: <code>${escapeHtml(chatId)}</code>\nPlan: <b>${escapeHtml(nivel)}</b> (S/${monto}/mes)\nCódigo de operación: <code>${escapeHtml(codigoOperacion)}</code>\n\nVerifica en tu Yape que el código y el monto coincidan antes de confirmar.`,
      {
        inline_keyboard: [
          [
            { text: "✅ Confirmar pago", callback_data: `pago:confirmar:${pago.id}` },
            { text: "❌ Rechazar", callback_data: `pago:rechazar:${pago.id}` },
          ],
        ],
      },
    );
  }

  await sendMessage(
    chatId,
    "✅ Recibimos tu código de operación. En cuanto lo verifiquemos, activamos tu plan (normalmente en minutos).",
  );
}

async function activarSuscripcion(
  targetChatId: string,
  nivel: string,
  dias: number,
  metodoPago?: string,
): Promise<{ fechaFin: string; error: { message: string } | null }> {
  const { isoDate } = getLimaDateParts();
  const fechaFin = shiftIsoDate(isoDate, dias);

  const { error } = await supabase.from("digemid_suscripciones").upsert(
    {
      telegram_chat_id: targetChatId,
      nivel,
      estado: "activo",
      fecha_inicio: isoDate,
      fecha_fin: fechaFin,
      metodo_pago: metodoPago ?? null,
    },
    { onConflict: "telegram_chat_id" },
  );

  return { fechaFin, error };
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
  chatType: string,
  esUsuarioNuevo = false,
) {
  const trimmed = text.trim();

  const mappedCommand = KEYBOARD_LABEL_COMMANDS[trimmed];
  if (mappedCommand) {
    return await handleCommand(chatId, userId, mappedCommand, chatType, esUsuarioNuevo);
  }

  if (trimmed.startsWith("🤖 Consulta IA")) {
    return await handleCommand(chatId, userId, "/consulta", chatType, esUsuarioNuevo);
  }

  const COMANDOS_CON_ACCESO_EXACTO = ["/ultimas", "/hoy", "/semana", "/mes", "/recientes"];
  const requiereAcceso =
    COMANDOS_CON_ACCESO_EXACTO.includes(trimmed) ||
    trimmed.startsWith("/detalle") ||
    trimmed.startsWith("/buscar") ||
    trimmed.startsWith("/consulta");

  if (requiereAcceso && !isAdmin(chatId)) {
    const estado = await getEstadoAcceso(chatId);
    if (!tieneAccesoActivo(estado)) {
      return await sendMessage(chatId, ACCESO_REQUERIDO_TEXTO, trialKeyboard());
    }
  }

  const esComandoStart = trimmed === "/start" || trimmed.startsWith("/start ");

  if (esComandoStart || trimmed === "/menu") {
    await logConsulta({
      chatId,
      userId,
      command: esComandoStart ? "/start" : "/menu",
      status: "ok",
    });

    if (esComandoStart) {
      const payload = trimmed.startsWith("/start ") ? trimmed.slice(7).trim() : "";

      await sendMessage(chatId, "👋 Bienvenido a RegAlert DIGEMID.", await persistentKeyboard(chatId));

      if (esUsuarioNuevo) {
        await sendMessage(
          chatId,
          "📝 Para identificarte en tus consultas de cuenta o membresía, cuéntanos con qué nombre quieres registrarte:\n" +
            "<code>/registrarme Tu Nombre Completo</code>\n\n" +
            "Ese será siempre el nombre con el que te reconocemos, aunque cambies tu nombre de Telegram. Puedes verlo cuando quieras con <code>/miperfil</code>.",
        );
      }

      if (payload.startsWith("plan_")) {
        // Deep-link desde la landing page: /start plan_basico, plan_consultoria, plan_prueba...
        const nivelSolicitado = payload.slice(5).toLowerCase();

        if (nivelSolicitado === "prueba") {
          await supabase
            .from("digemid_bot_usuarios")
            .update({ origen: "landing_page" })
            .eq("telegram_chat_id", chatId);

          await sendMessage(chatId, TRIAL_TEXTO, trialKeyboard());
          return;
        }

        if (nivelSolicitado in NIVEL_PRECIOS && nivelSolicitado !== "gratis") {
          await supabase
            .from("digemid_bot_usuarios")
            .update({ origen: "landing_page", plan_interes: nivelSolicitado })
            .eq("telegram_chat_id", chatId);

          await sendMessage(chatId, TRIAL_TEXTO, trialKeyboard(nivelSolicitado));
          return;
        }
      } else if (payload) {
        await consumirInvitacion(payload, chatId);
      } else {
        // /start sin ningun payload: entrada organica (alguien encontro el bot
        // directo en Telegram). Si todavia no eligio ni prueba ni plan, se le
        // ofrece lo mismo que a quien llega por la landing page.
        const estado = await getEstadoAcceso(chatId);

        if (!estado.legado && !estado.pruebaEstado && !estado.nivelPagado) {
          await supabase
            .from("digemid_bot_usuarios")
            .update({ origen: "organico" })
            .eq("telegram_chat_id", chatId);

          await sendMessage(chatId, TRIAL_TEXTO, trialKeyboard());
          return;
        }
      }
    }

    return await sendMessage(
      chatId,
      esComandoStart
        ? `${mainMenuText()}\n\n💡 <b>¿Primera vez?</b> Toca "Probar una consulta de ejemplo" y mira cómo respondo con la fuente oficial citada.`
        : mainMenuText(),
      mainMenu(esComandoStart),
    );
  }

  if (trimmed === "/ayuda") {
    await logConsulta({ chatId, userId, command: "/ayuda", status: "ok" });

    return await sendMessage(chatId, helpText(isAdmin(chatId)), mainMenu());
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

    await sendMessage(
      chatId,
      formatAlertList("🆕 <b>Últimas alertas DIGEMID</b>", rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
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

    await sendMessage(
      chatId,
      formatAlertList("📅 <b>Alertas DIGEMID de hoy</b>", rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
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

    await sendMessage(
      chatId,
      formatWeekAlertList(rows, total, 10),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
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

    await sendMessage(
      chatId,
      formatRecentAlertList(rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
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

    await sendMessage(
      chatId,
      formatAlertList("🗓️ <b>Alertas DIGEMID del mes</b>", rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
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

    await sendMessage(chatId, formatAlertDetail(row), detailButtons(row));
    await enviarPdfAlerta(chatId, row);
    return;
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

    await sendMessage(
      chatId,
      formatAlertList(`🔎 <b>Resultados para:</b> ${escapeHtml(query)}`, rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
  }

  if (trimmed === "/chatid") {
    await logConsulta({ chatId, userId, command: "/chatid", status: "ok" });

    const identity = await getBotIdentity();
    const botIdentity = identity ? `@${identity.username} (id ${identity.id})` : "desconocido";

    return await sendMessage(
      chatId,
      `🆔 El chat_id de este chat/grupo es:\n\n<code>${escapeHtml(chatId)}</code>\n\nCópialo para usarlo como TELEGRAM_CHAT_ID.\n\n🤖 Este bot es: <b>${escapeHtml(botIdentity)}</b>`,
    );
  }

  if (trimmed.startsWith("/suscribirme")) {
    const nivelSolicitado = trimmed.replace("/suscribirme", "").trim().toLowerCase();

    if (!nivelSolicitado || !(nivelSolicitado in NIVEL_PRECIOS) || nivelSolicitado === "gratis") {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/suscribirme basico</code>\n\nPlanes disponibles:\n• <b>basico</b> — S/29/mes (30 consultas/día)\n• <b>consultoria</b> — S/79/mes (100 consultas/día)\n• <b>empresarial</b> — S/199/mes (sin límite)",
      );
    }

    return await solicitarPlan(chatId, userId, nivelSolicitado);
  }

  if (trimmed.startsWith("/pague")) {
    const codigo = trimmed.replace("/pague", "").trim();

    if (!codigo) {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/pague codigo_de_operacion</code>\n\nEjemplo:\n<code>/pague 000123456</code>",
      );
    }

    await logConsulta({ chatId, userId, command: "/pague", queryText: codigo, status: "ok" });
    return await reportarPagoYape(chatId, userId, codigo);
  }

  if (trimmed.startsWith("/activar")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const parts = trimmed.split(/\s+/).slice(1);
    const [targetChatId, nivel, diasStr, metodoPago] = parts;

    if (!targetChatId) {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/activar chat_id [nivel dias metodo_pago]</code>\n\nEjemplo:\n<code>/activar 123456789 basico 30 yape</code>\n\nO solo <code>/activar chat_id</code> para elegir el plan con botones.",
      );
    }

    if (!nivel || !diasStr) {
      return await sendMessage(
        chatId,
        `¿Qué plan le doy a <code>${escapeHtml(targetChatId)}</code>?`,
        {
          inline_keyboard: [
            [{ text: "Básico (S/29) — 30 días", callback_data: `activar:${targetChatId}:basico:30` }],
            [{ text: "Consultoría (S/79) — 30 días", callback_data: `activar:${targetChatId}:consultoria:30` }],
            [{ text: "Empresarial (S/199) — 30 días", callback_data: `activar:${targetChatId}:empresarial:30` }],
          ],
        },
      );
    }

    if (!(nivel in NIVEL_LIMITES_DIARIOS)) {
      return await sendMessage(chatId, "⚠️ Nivel inválido. Usa: gratis, basico, consultoria o empresarial.");
    }

    const dias = parseInt(diasStr, 10);

    if (!Number.isFinite(dias) || dias <= 0) {
      return await sendMessage(chatId, "⚠️ Los días deben ser un número entero positivo.");
    }

    const { fechaFin, error } = await activarSuscripcion(targetChatId, nivel, dias, metodoPago);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al activar: ${escapeHtml(error.message)}`);
    }

    return await sendMessage(
      chatId,
      `✅ Activado <b>${escapeHtml(nivel)}</b> para <code>${escapeHtml(targetChatId)}</code> hasta <b>${escapeHtml(fechaFin)}</b>.`,
    );
  }

  if (trimmed === "/usuarios") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const [{ count: totalUsuarios }, { data: suscripciones, error: suscripcionesError }] =
      await Promise.all([
        supabase.from("digemid_bot_usuarios").select("id", { count: "exact", head: true }),
        supabase
          .from("digemid_suscripciones")
          .select("telegram_chat_id, telegram_username, nivel, estado, fecha_fin")
          .order("fecha_fin", { ascending: true }),
      ]);

    if (suscripcionesError) {
      return await sendMessage(chatId, `⚠️ Error al consultar usuarios: ${escapeHtml(suscripcionesError.message)}`);
    }

    return await sendMessage(chatId, formatResumenUsuarios(totalUsuarios ?? 0, suscripciones ?? []));
  }

  if (trimmed === "/membresias") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { data: suscripciones, error: suscripcionesError } = await supabase
      .from("digemid_suscripciones")
      .select("telegram_chat_id, telegram_username, nivel, estado, fecha_inicio, fecha_fin")
      .order("fecha_fin", { ascending: true });

    if (suscripcionesError) {
      return await sendMessage(chatId, `⚠️ Error al consultar membresías: ${escapeHtml(suscripcionesError.message)}`);
    }

    const chatIds = [...new Set((suscripciones ?? []).map((s) => s.telegram_chat_id))];
    const nombresPorChatId = new Map<string, string>();

    if (chatIds.length) {
      const { data: usuarios } = await supabase
        .from("digemid_bot_usuarios")
        .select("telegram_chat_id, nombre")
        .in("telegram_chat_id", chatIds);

      for (const u of usuarios ?? []) {
        if (u.nombre) nombresPorChatId.set(u.telegram_chat_id, u.nombre);
      }
    }

    return await sendMessage(chatId, formatMembresias(suscripciones ?? [], nombresPorChatId));
  }

  if (trimmed === "/directorio") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const [{ data: usuarios, error: usuariosError }, { data: suscripciones, error: suscripcionesError }] =
      await Promise.all([
        supabase
          .from("digemid_bot_usuarios")
          .select("telegram_chat_id, nombre, username, origen, plan_interes, prueba_estado, prueba_alertas_enviadas, plan_gratis_legado")
          .order("created_at", { ascending: false }),
        supabase
          .from("digemid_suscripciones")
          .select("telegram_chat_id, nivel, estado, fecha_fin")
          .order("fecha_fin", { ascending: false }),
      ]);

    if (usuariosError || suscripcionesError) {
      return await sendMessage(
        chatId,
        `⚠️ Error al consultar el directorio: ${escapeHtml((usuariosError ?? suscripcionesError)!.message)}`,
      );
    }

    const hoy = getLimaDateParts().isoDate;
    const subPorChatId = new Map<string, { nivel: string; estado: string; fecha_fin: string | null }>();

    for (const sub of suscripciones ?? []) {
      // Ya viene ordenado por fecha_fin desc: nos quedamos con la mas reciente por chat_id.
      if (!subPorChatId.has(sub.telegram_chat_id)) {
        subPorChatId.set(sub.telegram_chat_id, sub);
      }
    }

    const personas = (usuarios ?? []).filter((u) => !u.telegram_chat_id.startsWith("-"));

    const { texto, candidatos } = formatDirectorio(personas, subPorChatId, hoy);

    const botones = candidatos.slice(0, 15).map((c) => [
      { text: `📣 Recordar a ${c.nombre}`, callback_data: `recordatorio:${c.chatId}` },
    ]);

    return await sendMessage(
      chatId,
      texto,
      botones.length ? { inline_keyboard: botones } : undefined,
    );
  }

  if (trimmed === "/ingresos") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { startIso, nextIso } = getCurrentMonthBoundsLima();

    const { data: altas, error: ingresosError } = await supabase
      .from("digemid_suscripciones")
      .select("nivel")
      .neq("nivel", "gratis")
      .gte("fecha_inicio", startIso)
      .lt("fecha_inicio", nextIso);

    if (ingresosError) {
      return await sendMessage(chatId, `⚠️ Error al calcular ingresos: ${escapeHtml(ingresosError.message)}`);
    }

    return await sendMessage(chatId, formatIngresos(altas ?? [], startIso));
  }

  if (trimmed === "/pagosyape") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { startIso, nextIso } = getCurrentMonthBoundsLima();

    const { data: pagos, error: pagosError } = await supabase
      .from("digemid_pagos_yape")
      .select("nivel, monto_esperado, estado")
      .gte("creado_at", startIso)
      .lt("creado_at", nextIso);

    if (pagosError) {
      return await sendMessage(chatId, `⚠️ Error al calcular pagos Yape: ${escapeHtml(pagosError.message)}`);
    }

    return await sendMessage(chatId, formatPagosYape(pagos ?? [], startIso));
  }

  if (trimmed === "/saldodeepseek") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    try {
      const { balanceUsd, isAvailable, raw } = await consultarSaldoDeepseek();

      if (balanceUsd === null) {
        return await sendMessage(chatId, "⚠️ No pude interpretar el saldo devuelto por DeepSeek.");
      }

      await guardarSnapshotSaldoDeepseek(balanceUsd, isAvailable, raw);
      const diasRestantes = await proyectarDiasRestantesDeepseek(balanceUsd);

      const lineas = [
        "🔋 <b>Saldo DeepSeek</b>",
        "",
        `Saldo actual: <b>$${balanceUsd.toFixed(2)} USD</b>`,
        `Disponible: ${isAvailable ? "sí" : "no"}`,
      ];

      if (diasRestantes !== null) {
        lineas.push(`Proyección al ritmo de consumo actual: <b>~${diasRestantes.toFixed(1)} días restantes</b>`);
      } else {
        lineas.push("Aún no hay suficiente historial para proyectar días restantes.");
      }

      return await sendMessage(chatId, lineas.join("\n"));
    } catch (error) {
      return await sendMessage(chatId, `⚠️ Error al consultar el saldo de DeepSeek: ${escapeHtml(String(error))}`);
    }
  }

  if (trimmed.startsWith("/invitar")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const parts = trimmed.split(/\s+/).slice(1);
    const telefono = parts[0];
    const nombre = parts.slice(1).join(" ").trim();

    if (!telefono) {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/invitar telefono nombre</code>\n\nEjemplo:\n<code>/invitar +51987654321 Juan Perez</code>",
      );
    }

    const codigo = crypto.randomUUID().replace(/-/g, "").slice(0, 10);

    const { error: invitacionError } = await supabase.from("digemid_bot_invitaciones").insert({
      codigo,
      telefono,
      nombre: nombre || null,
      creado_por: chatId,
    });

    if (invitacionError) {
      return await sendMessage(chatId, `⚠️ Error al crear invitación: ${escapeHtml(invitacionError.message)}`);
    }

    const identity = await getBotIdentity();

    if (!identity) {
      return await sendMessage(chatId, "⚠️ No pude generar el enlace (no se pudo identificar al bot).");
    }

    const telegramLink = `https://t.me/${identity.username}?start=${codigo}`;
    const telefonoLimpio = telefono.replace(/\D/g, "");
    const mensajeWhatsapp = `Hola${nombre ? " " + nombre : ""}! Aquí tienes acceso al bot de alertas DIGEMID: ${telegramLink}`;
    const waLink = `https://wa.me/${telefonoLimpio}?text=${encodeURIComponent(mensajeWhatsapp)}`;

    return await sendMessage(
      chatId,
      `✅ Invitación creada${nombre ? ` para <b>${escapeHtml(nombre)}</b>` : ""} (${escapeHtml(telefono)}).\n\n📲 Envíaselo por WhatsApp con un clic:\n${escapeHtml(waLink)}\n\n🔗 O el enlace directo de Telegram:\n${escapeHtml(telegramLink)}\n\nTe aviso apenas toque \"Iniciar\".`,
    );
  }

  if (trimmed.startsWith("/renombrar")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const parts = trimmed.split(/\s+/).slice(1);
    const targetChatId = parts[0];
    const nuevoNombre = parts.slice(1).join(" ").trim();

    if (!targetChatId || !nuevoNombre) {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/renombrar chat_id nuevo nombre</code>\n\nEjemplo:\n<code>/renombrar 123456789 Juan Perez</code>",
      );
    }

    const { error } = await supabase
      .from("digemid_bot_usuarios")
      .update({ nombre: nuevoNombre, nombre_confirmado: true })
      .eq("telegram_chat_id", targetChatId);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al renombrar: ${escapeHtml(error.message)}`);
    }

    return await sendMessage(
      chatId,
      `✅ <code>${escapeHtml(targetChatId)}</code> ahora se llama <b>${escapeHtml(nuevoNombre)}</b>.`,
    );
  }

  if (trimmed.startsWith("/gratis")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const targetChatId = trimmed.split(/\s+/)[1];

    if (!targetChatId) {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/gratis chat_id</code>\n\nDeja a esa persona exenta de la prueba con límite de tiempo/alertas (acceso gratis para siempre, caso manual).",
      );
    }

    const { error } = await supabase
      .from("digemid_bot_usuarios")
      .update({ plan_gratis_legado: true })
      .eq("telegram_chat_id", targetChatId);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error: ${escapeHtml(error.message)}`);
    }

    return await sendMessage(
      chatId,
      `✅ <code>${escapeHtml(targetChatId)}</code> queda con acceso gratis permanente (caso manual).`,
    );
  }

  if (trimmed.startsWith("/registrarme")) {
    const nuevoNombre = trimmed.replace("/registrarme", "").trim();

    if (!nuevoNombre) {
      return await sendMessage(
        chatId,
        "Uso:\n<code>/registrarme Tu Nombre Completo</code>\n\nEjemplo:\n<code>/registrarme Juan Pérez</code>",
      );
    }

    const { error } = await supabase
      .from("digemid_bot_usuarios")
      .update({ nombre: nuevoNombre, nombre_confirmado: true })
      .eq("telegram_chat_id", chatId);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al registrar tu nombre: ${escapeHtml(error.message)}`);
    }

    return await sendMessage(
      chatId,
      `✅ Quedaste registrado como <b>${escapeHtml(nuevoNombre)}</b>.\n\nUsa este nombre cuando nos escribas por dudas de tu cuenta o membresía. Puedes verlo cuando quieras con <code>/miperfil</code>.`,
    );
  }

  if (trimmed === "/miperfil") {
    return await enviarMiPerfil(chatId);
  }

  if (trimmed.startsWith("/desactivar")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const parts = trimmed.split(/\s+/).slice(1);
    const [targetChatId] = parts;

    if (!targetChatId) {
      return await sendMessage(chatId, "Uso:\n<code>/desactivar chat_id</code>");
    }

    const { error } = await supabase
      .from("digemid_suscripciones")
      .update({ estado: "cancelado" })
      .eq("telegram_chat_id", targetChatId);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al desactivar: ${escapeHtml(error.message)}`);
    }

    return await sendMessage(chatId, `✅ Suscripción de <code>${escapeHtml(targetChatId)}</code> cancelada.`);
  }

  if (trimmed.startsWith("/consulta")) {
    if (chatType !== "private") {
      const identity = await getBotIdentity();
      const link = identity ? `\n\nEscríbeme por privado: https://t.me/${identity.username}` : "";

      return await sendMessage(
        chatId,
        `🤖 Las consultas con IA solo funcionan en el chat privado con el bot, para que cada quien vea sus propias respuestas.${link}`,
      );
    }

    const question = trimmed.replace("/consulta", "").trim();

    if (!question) {
      return await sendMessage(
        chatId,
        "🤖 <b>Consulta IA</b>\n\nEscribe tu pregunta despues de /consulta y te respondo citando la alerta o norma oficial.\n\nEjemplo:\n<code>/consulta que paso con el Opdivo falsificado</code>",
      );
    }

    try {
      const nivel = await getNivelUsuario(chatId);
      const limiteUsuario = NIVEL_LIMITES_DIARIOS[nivel] ?? NIVEL_LIMITES_DIARIOS.gratis;

      const [consultasHoyUsuario, consultasHoyGlobal] = await Promise.all([
        limiteUsuario === null ? Promise.resolve(0) : contarConsultasHoy(chatId),
        contarConsultasHoy(),
      ]);

      if (consultasHoyGlobal >= LIMITE_DIARIO_GLOBAL) {
        await logConsulta({
          chatId,
          userId,
          command: "/consulta",
          queryText: question,
          status: "limite_global",
        });

        return await sendMessage(
          chatId,
          "⚠️ Se alcanzó el límite diario de consultas del sistema. Intenta de nuevo mañana.",
        );
      }

      if (limiteUsuario !== null && consultasHoyUsuario >= limiteUsuario) {
        await logConsulta({
          chatId,
          userId,
          command: "/consulta",
          queryText: question,
          status: "limite_usuario",
        });

        return await sendMessage(
          chatId,
          `⚠️ Alcanzaste tu límite diario de <b>${limiteUsuario}</b> consultas (plan <b>${escapeHtml(nivel)}</b>).\n\nTus alertas automáticas siguen llegando igual. Si quieres más consultas con IA:\n\n${PLANES_TEXTO_CORTO}`,
          planesKeyboard(),
        );
      }

      const { answer, sources } = await answerConsulta(question);

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: sources.length,
        status: "ok",
      });

      const sourceButtons = sources.length
        ? {
          inline_keyboard: sources
            .slice(0, 3)
            .map((source) => [
              { text: `📄 ${source.documentKey}${source.page ? " (pág. " + source.page + ")" : ""}`, url: source.url },
            ]),
        }
        : undefined;

      let pie = "";
      if (limiteUsuario !== null) {
        const restantes = Math.max(0, limiteUsuario - (consultasHoyUsuario + 1));
        pie = `\n\n<i>Te quedan ${restantes} de ${limiteUsuario} consultas hoy (plan ${escapeHtml(nivel)}).</i>`;
        if (restantes === 0) {
          pie += `\n💡 ¿Necesitas más? Escribe <code>/suscribirme basico</code>`;
        }
      }

      return await sendMessage(chatId, `🤖 ${formatConsultaAnswer(answer)}${pie}`, sourceButtons);
    } catch (error) {
      console.error("CONSULTA_ERROR:", error);

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        status: "error",
        raw: { error: String(error) },
      });

      return await sendMessage(
        chatId,
        "⚠️ No pude procesar la consulta en este momento. Intenta de nuevo en unos minutos.",
        alertasMenu(),
      );
    }
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

  const callbackUserId = String(callback.from?.id ?? "");

  const requiereAccesoCallback = data === "menu:alertas" || data.startsWith("alertas:");

  if (requiereAccesoCallback && !isAdmin(chatId)) {
    const estado = await getEstadoAcceso(chatId);
    if (!tieneAccesoActivo(estado)) {
      return await sendMessage(chatId, ACCESO_REQUERIDO_TEXTO, trialKeyboard());
    }
  }

  if (data === "demo:ejemplo") {
    const preguntaEjemplo = "que alertas hay sobre productos falsificados";

    await sendMessage(
      chatId,
      `🧪 <b>Demo</b> — Te muestro cómo respondo a:\n<i>"${escapeHtml(preguntaEjemplo)}"</i>`,
    );

    try {
      const { answer, sources } = await answerConsulta(preguntaEjemplo);

      const sourceButtons = sources.length
        ? {
          inline_keyboard: sources
            .slice(0, 3)
            .map((source) => [
              { text: `📄 ${source.documentKey}${source.page ? " (pág. " + source.page + ")" : ""}`, url: source.url },
            ]),
        }
        : undefined;

      await sendMessage(chatId, `🤖 ${formatConsultaAnswer(answer)}`, sourceButtons);
      return await sendMessage(
        chatId,
        "✅ Así de fácil. Ahora prueba tú: escribe <code>/consulta</code> seguido de tu pregunta.",
      );
    } catch (_error) {
      return await sendMessage(
        chatId,
        "Escribe <code>/consulta</code> seguido de tu pregunta y te respondo citando la fuente oficial.",
      );
    }
  }

  if (data === "trial:iniciar") {
    const { data: usuarioActual } = await supabase
      .from("digemid_bot_usuarios")
      .select("prueba_estado")
      .eq("telegram_chat_id", chatId)
      .maybeSingle();

    if (usuarioActual?.prueba_estado) {
      // Ya tuvo una prueba (activa o finalizada): no se puede reiniciar.
      return await sendMessage(
        chatId,
        "⚠️ Ya usaste tu prueba gratuita de RegAlert anteriormente, así que no se puede reiniciar. " +
          "Si quieres seguir recibiendo alertas y consultas con IA, elige un plan:",
        planesKeyboard(),
      );
    }

    await supabase
      .from("digemid_bot_usuarios")
      .update({
        prueba_estado: "activa",
        prueba_inicio: new Date().toISOString(),
        prueba_alertas_enviadas: 0,
      })
      .eq("telegram_chat_id", chatId);

    return await sendMessage(
      chatId,
      "✅ <b>Prueba gratuita activada.</b>\n\nLas próximas alertas de DIGEMID te llegarán aquí automáticamente. " +
        "Escribe <code>/consulta</code> seguido de tu pregunta cuando quieras (tienes 5 consultas/día).",
      mainMenu(),
    );
  }

  if (data.startsWith("plan:")) {
    const nivelSolicitado = data.slice(5).toLowerCase();

    if (nivelSolicitado in NIVEL_PRECIOS && nivelSolicitado !== "gratis") {
      return await solicitarPlan(chatId, callbackUserId, nivelSolicitado);
    }

    return;
  }

  if (data.startsWith("recordatorio:")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ No autorizado.");
    }

    const targetChatId = data.slice("recordatorio:".length);

    const { data: destinatario } = await supabase
      .from("digemid_bot_usuarios")
      .select("nombre, username")
      .eq("telegram_chat_id", targetChatId)
      .maybeSingle();

    const nombreDestino = destinatario?.nombre || (destinatario?.username ? `@${destinatario.username}` : "");
    const saludo = nombreDestino ? `Hola ${escapeHtml(nombreDestino)}` : "Hola";

    await sendMessage(
      targetChatId,
      `👋 ${saludo}, ¡te extrañamos por RegAlert DIGEMID!\n\n` +
        "DIGEMID sigue publicando alertas sanitarias todas las semanas, y no queremos que te pierdas la próxima. " +
        "Si quieres volver a recibir alertas automáticas y hacer consultas con IA citando la fuente oficial, elige tu plan aquí mismo:",
      planesKeyboard(),
    );

    return await sendMessage(
      chatId,
      `✅ Recordatorio enviado a <b>${escapeHtml(nombreDestino || targetChatId)}</b>.`,
    );
  }

  if (data.startsWith("pago:confirmar:") || data.startsWith("pago:rechazar:")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ No autorizado.");
    }

    const aprobar = data.startsWith("pago:confirmar:");
    const pagoId = data.split(":")[2];

    const { data: pago, error: pagoError } = await supabase
      .from("digemid_pagos_yape")
      .select("id, chat_id, nivel, monto_esperado, estado")
      .eq("id", pagoId)
      .maybeSingle();

    if (pagoError || !pago) {
      return await sendMessage(chatId, "⚠️ No encontré ese pago (¿ya fue procesado?).");
    }

    if (pago.estado !== "pendiente") {
      return await sendMessage(chatId, `⚠️ Ese pago ya estaba <b>${escapeHtml(pago.estado)}</b>.`);
    }

    await supabase
      .from("digemid_pagos_yape")
      .update({
        estado: aprobar ? "confirmado" : "rechazado",
        confirmado_at: new Date().toISOString(),
        confirmado_por: chatId,
      })
      .eq("id", pagoId);

    if (!aprobar) {
      await sendMessage(
        pago.chat_id,
        "⚠️ No pudimos verificar tu pago. Revisa el código de operación con <code>/pague codigo</code> o contáctanos.",
      );
      return await sendMessage(chatId, "❌ Pago rechazado.");
    }

    await supabase
      .from("digemid_bot_usuarios")
      .update({ pago_pendiente_nivel: null })
      .eq("telegram_chat_id", pago.chat_id);

    const { fechaFin, error } = await activarSuscripcion(pago.chat_id, pago.nivel, 30, "yape");

    if (error) {
      return await sendMessage(chatId, `⚠️ Pago confirmado pero hubo un error al activar: ${escapeHtml(error.message)}`);
    }

    await sendMessage(
      pago.chat_id,
      `✅ ¡Tu plan <b>${escapeHtml(pago.nivel)}</b> está activo hasta <b>${escapeHtml(fechaFin)}</b>! Gracias por tu pago.`,
    );

    return await sendMessage(
      chatId,
      `✅ Activado <b>${escapeHtml(pago.nivel)}</b> para <code>${escapeHtml(pago.chat_id)}</code> hasta <b>${escapeHtml(fechaFin)}</b>.`,
    );
  }

  if (data.startsWith("activar:")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ No autorizado.");
    }

    const [, targetChatId, nivel, diasStr] = data.split(":");
    const dias = parseInt(diasStr, 10);

    if (!targetChatId || !nivel || !Number.isFinite(dias)) {
      return await sendMessage(chatId, "⚠️ Botón inválido, intenta de nuevo con /activar.");
    }

    const { fechaFin, error } = await activarSuscripcion(targetChatId, nivel, dias);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al activar: ${escapeHtml(error.message)}`);
    }

    return await sendMessage(
      chatId,
      `✅ Activado <b>${escapeHtml(nivel)}</b> para <code>${escapeHtml(targetChatId)}</code> hasta <b>${escapeHtml(fechaFin)}</b>.`,
    );
  }

  if (data === "menu:principal") {
    return await sendMessage(chatId, mainMenuText(), mainMenu());
  }

  if (data === "menu:alertas") {
    return await sendMessage(
      chatId,
      "🚨 <b>Alertas DIGEMID</b>\n\n¿Qué deseas consultar?",
      alertasMenu(),
    );
  }

  if (data === "menu:ayuda") {
    return await sendMessage(chatId, helpText(isAdmin(chatId)), mainMenu());
  }

  if (data === "menu:planes") {
    return await sendMessage(
      chatId,
      `💳 <b>Planes disponibles</b>\n\n${PLANES_TEXTO_CORTO}\n\nToca uno para solicitarlo — te doy el número de Yape al instante.`,
      planesKeyboard(),
    );
  }

  if (data === "cuenta:miperfil") {
    return await enviarMiPerfil(chatId);
  }

  if (data === "cuenta:registrarme_info") {
    return await sendMessage(
      chatId,
      "📝 Escribe:\n<code>/registrarme Tu Nombre Completo</code>\n\nEjemplo:\n<code>/registrarme Juan Pérez</code>",
    );
  }

  if (data === "alertas:ultimas") {
    const rows = await getLatestAlerts(5);

    await sendMessage(
      chatId,
      formatAlertList("🆕 <b>Últimas alertas DIGEMID</b>", rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
  }

  if (data === "alertas:hoy") {
    const rows = await getTodayAlerts();

    await sendMessage(
      chatId,
      formatAlertList("📅 <b>Alertas DIGEMID de hoy</b>", rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
  }

  if (data === "alertas:semana") {
    const { rows, total } = await getAlertasSemana(10);

    await sendMessage(
      chatId,
      formatWeekAlertList(rows, total, 10),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
  }

  if (data === "alertas:recientes") {
    const rows = await getRecentAlerts(10);

    await sendMessage(
      chatId,
      formatRecentAlertList(rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
  }

  if (data === "alertas:mes") {
    const rows = await getMonthAlerts();

    await sendMessage(
      chatId,
      formatAlertList("🗓️ <b>Alertas DIGEMID del mes</b>", rows),
      alertasMenu(),
    );
    await enviarPdfsAlertas(chatId, rows);
    return;
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

    const chatType = String(
      update.message?.chat.type ?? update.callback_query?.message?.chat.type ?? "private",
    );

    if (!chatId) {
      return new Response("Sin chat_id", { status: 200 });
    }

    if (!isAllowed(chatId)) {
      await sendMessage(chatId, "⛔ No tienes acceso autorizado a este bot.");
      return new Response("No autorizado", { status: 200 });
    }

    const { isNew: esUsuarioNuevo } = await upsertUsuario(update, chatId);

    if (update.callback_query) {
      await handleCallback(update);
      return new Response("OK", { status: 200 });
    }

    if (update.message?.voice) {
      try {
        const transcripcion = await transcribirNotaDeVoz(update.message.voice.file_id);

        if (!transcripcion) {
          await sendMessage(
            chatId,
            "⚠️ No pude transcribir el audio con claridad. Intenta de nuevo hablando despacio, " +
              "o escribe tu pregunta con /consulta.",
          );
          return new Response("OK", { status: 200 });
        }

        // Se muestra la transcripcion antes de responder: si la IA entendio
        // mal el audio, el usuario lo nota de inmediato en vez de recibir
        // una respuesta que no corresponde a lo que pregunto.
        await sendMessage(chatId, `🎙️ Escuché: <i>"${escapeHtml(transcripcion)}"</i>`);
        await handleCommand(chatId, userId, `/consulta ${transcripcion}`, chatType, esUsuarioNuevo);
      } catch (error) {
        console.error("Error transcribiendo nota de voz:", error);
        await sendMessage(
          chatId,
          "⚠️ No pude procesar tu mensaje de voz. Intenta de nuevo o escribe tu pregunta con /consulta.",
        );
      }

      return new Response("OK", { status: 200 });
    }

    const text = update.message?.text ?? "/start";
    await handleCommand(chatId, userId, text, chatType, esUsuarioNuevo);

    return new Response("OK", { status: 200 });
  } catch (error) {
    console.error(error);

    return new Response("Error interno", {
      status: 500,
    });
  }
});
