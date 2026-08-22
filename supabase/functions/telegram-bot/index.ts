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

// Duracion maxima (segundos) de nota de voz por plan: una consulta hablada
// real rara vez pasa de 20-30s, y esto evita audios larguisimos que salen
// mas caros de transcribir. Los administradores no tienen limite.
const NIVEL_LIMITE_VOZ_SEGUNDOS: Record<string, number> = {
  gratis: 10,
  basico: 30,
  consultoria: 60,
  empresarial: 120,
};

const NIVEL_PRECIOS: Record<string, number> = {
  gratis: 0,
  basico: 29,
  consultoria: 79,
  empresarial: 199,
};

// TTS de Gemini es de pago (no free tier), por eso el audio de una respuesta
// se genera solo si el usuario toca el boton "Escuchar" (nunca automatico) y
// solo esta disponible desde el plan basico en adelante, para controlar costo.
const GEMINI_TTS_MODEL = "gemini-2.5-flash-preview-tts";
const TTS_MAX_CARACTERES = 600;

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
- Si el bloque que usaste para responder trae una linea "ADVERTENCIA DE \
CONFIABILIDAD", tu respuesta se apoya en una transcripcion no verificada por \
un humano (posible error de OCR, tabla aplanada a texto, o formula/notacion \
tecnica). En ese caso agrega una linea final: "⚠️ Verificar con el PDF \
original: [motivo breve]". No uses ese aviso si el bloque no trae la \
advertencia — no le bajes confianza a contenido ya verificado.
- Para resaltar nombres de productos, numeros de alerta/norma y terminos clave, \
usa negrita en formato HTML de Telegram: <b>texto</b>. No uses markdown (**texto**).

Estructura SIEMPRE tu respuesta en este formato exacto, pensado para leerse \
rapido en un celular:

<b>[resumen de la respuesta en una sola linea, en negrita]</b>

[2 a 4 lineas de detalle de apoyo, con terminos clave en <b>negrita</b>]

📌 Fuente: <b>[numero de alerta o codigo de norma]</b> — [fecha], pag. [numero de pagina]

[SOLO si el bloque usado trae "ADVERTENCIA DE CONFIABILIDAD": una linea final \
"⚠️ Verificar con el PDF original: [motivo breve]". Omite esta linea por completo \
si no aplica.]

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
    document?: {
      file_id: string;
      file_name?: string;
      mime_type?: string;
    };
    caption?: string;
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

function listaAdminChatIds(): string[] {
  return ADMIN_CHAT_IDS
    .split(",")
    .map((item: string) => item.trim())
    .filter((item: string) => item.length > 0);
}

const COMANDOS_USUARIO: { command: string; description: string }[] = [
  { command: "start", description: "Inicia el bot y muestra la bienvenida" },
  { command: "menu", description: "Menú principal con botones" },
  { command: "ayuda", description: "Guía de comandos y opciones" },
  { command: "ultimas", description: "Últimas alertas registradas" },
  { command: "hoy", description: "Alertas publicadas hoy" },
  { command: "semana", description: "Alertas publicadas esta semana" },
  { command: "mes", description: "Alertas publicadas este mes" },
  { command: "recientes", description: "Alertas registradas recientemente" },
  { command: "buscar", description: "Buscar alertas por palabra clave" },
  { command: "consulta", description: "Preguntar con IA citando la norma/alerta fuente" },
  { command: "suscribirme", description: "Solicitar activar un plan pagado" },
  { command: "pague", description: "Reportar tu código de operación Yape" },
  { command: "registrarme", description: "Registrar el nombre de tu cuenta" },
  { command: "miperfil", description: "Ver tu nombre y el estado de tu plan" },
  { command: "detalle", description: "Consultar una alerta por número" },
];

const COMANDOS_ADMIN: { command: string; description: string }[] = [
  ...COMANDOS_USUARIO,
  { command: "activar", description: "Activar un plan pagado a un usuario" },
  { command: "desactivar", description: "Cancelar la suscripción de un usuario" },
  { command: "usuarios", description: "Resumen de usuarios por estado/nivel" },
  { command: "membresias", description: "Lista completa de suscripciones" },
  { command: "directorio", description: "Usuarios por estado con recordatorio" },
  { command: "ingresos", description: "Ingresos del mes por plan" },
  { command: "invitar", description: "Generar invitación para un usuario nuevo" },
  { command: "renombrar", description: "Cambiar el nombre de un usuario" },
  { command: "gratis", description: "Dar acceso gratis permanente a un usuario" },
  { command: "pagosyape", description: "Sumar los pagos Yape reportados este mes" },
  { command: "saldodeepseek", description: "Consultar el saldo de la API DeepSeek" },
  { command: "normasrevisar", description: "Normas con páginas de baja calidad" },
  { command: "normassinpdf", description: "Normas sin PDF confirmado" },
  { command: "derogacionespendientes", description: "Revisar derogaciones/modificaciones detectadas por IA" },
  { command: "normapdf", description: "Instrucciones para subir el PDF de una norma" },
  { command: "normarevisar", description: "Corregir páginas de baja calidad de una norma" },
  { command: "tablasrevisar", description: "Normas con tablas sin verificar" },
  { command: "tablarevisar", description: "Verificar las tablas de una norma" },
  { command: "normaestado", description: "Reporte de fidelidad (global o por norma)" },
  { command: "reportenormas", description: "Reporte maestro HTML de todas las normas" },
  { command: "actualizarcomandos", description: "Refrescar este menú de comandos" },
];

/** Registra el menu nativo "/" de Telegram: uno reducido para cualquier
 * usuario (scope default) y uno completo (usuario + admin) solo para los
 * chats de ADMIN_CHAT_IDS (scope "chat"), para no mostrarle a un usuario
 * comun comandos administrativos que no puede usar. Se llama sola al
 * arrancar la funcion (fire-and-forget) y tambien via /actualizarcomandos
 * por si la funcion sigue "tibia" desde antes del ultimo deploy. */
async function actualizarComandosTelegram(): Promise<void> {
  await telegram("setMyCommands", { commands: COMANDOS_USUARIO });

  for (const adminId of listaAdminChatIds()) {
    await telegram("setMyCommands", {
      commands: COMANDOS_ADMIN,
      scope: { type: "chat", chat_id: adminId },
    });
  }
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
}): Promise<string | null> {
  try {
    const { data } = await supabase
      .from("digemid_bot_consultas")
      .insert({
        telegram_chat_id: params.chatId,
        telegram_user_id: params.userId ?? null,
        command: params.command,
        query_text: params.queryText ?? null,
        result_count: params.resultCount ?? 0,
        status: params.status,
        raw: params.raw ?? {},
      })
      .select("id")
      .single();

    return data?.id != null ? String(data.id) : null;
  } catch (_error) {
    // No bloquea la respuesta del bot.
    return null;
  }
}

// Envuelve PCM crudo (lo que devuelve la API de Gemini TTS, sin encabezado)
// en un contenedor WAV valido, a mano: no hay ffmpeg disponible en el edge
// runtime de Deno, y un header WAV son solo 44 bytes fijos.
function pcmAWav(pcm: Uint8Array, sampleRate = 24000, canales = 1, bitsPorMuestra = 16): Uint8Array {
  const blockAlign = (canales * bitsPorMuestra) / 8;
  const byteRate = sampleRate * blockAlign;
  const buffer = new ArrayBuffer(44 + pcm.length);
  const view = new DataView(buffer);

  const escribirTexto = (offset: number, texto: string) => {
    for (let i = 0; i < texto.length; i++) view.setUint8(offset + i, texto.charCodeAt(i));
  };

  escribirTexto(0, "RIFF");
  view.setUint32(4, 36 + pcm.length, true);
  escribirTexto(8, "WAVE");
  escribirTexto(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, canales, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, byteRate, true);
  view.setUint16(32, blockAlign, true);
  view.setUint16(34, bitsPorMuestra, true);
  escribirTexto(36, "data");
  view.setUint32(40, pcm.length, true);
  new Uint8Array(buffer, 44).set(pcm);

  return new Uint8Array(buffer);
}

async function generarAudioRespuesta(texto: string): Promise<Uint8Array> {
  const textoRecortado = texto.length > TTS_MAX_CARACTERES
    ? texto.slice(0, TTS_MAX_CARACTERES) + "..."
    : texto;

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_TTS_MODEL}:generateContent?key=${GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: textoRecortado }] }],
        generationConfig: {
          responseModalities: ["AUDIO"],
          speechConfig: {
            voiceConfig: { prebuiltVoiceConfig: { voiceName: "Kore" } },
          },
        },
      }),
    },
  );

  if (!response.ok) {
    throw new Error(`Gemini TTS error ${response.status}: ${await response.text()}`);
  }

  const data = await response.json();
  const base64Audio = data?.candidates?.[0]?.content?.parts?.[0]?.inlineData?.data;

  if (!base64Audio) {
    throw new Error("Gemini TTS no devolvió audio");
  }

  const pcmBytes = Uint8Array.from(atob(base64Audio), (c) => c.charCodeAt(0));
  return pcmAWav(pcmBytes);
}

async function enviarAudioRespuesta(chatId: string, wavBytes: Uint8Array, caption: string): Promise<void> {
  const formData = new FormData();
  formData.append("chat_id", chatId);
  formData.append("caption", caption);
  formData.append("audio", new Blob([wavBytes.buffer as ArrayBuffer], { type: "audio/wav" }), "respuesta.wav");

  const response = await fetch(`${TELEGRAM_API}/sendAudio`, {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error(`Telegram sendAudio error ${response.status}: ${await response.text()}`);
  }
}

async function enviarDocumentoTexto(
  chatId: string,
  contenido: string,
  nombreArchivo: string,
  mimeType: string,
  caption?: string,
): Promise<void> {
  const formData = new FormData();
  formData.append("chat_id", chatId);
  if (caption) formData.append("caption", caption);
  formData.append("document", new Blob([contenido], { type: mimeType }), nombreArchivo);

  const response = await fetch(`${TELEGRAM_API}/sendDocument`, {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error(`Telegram sendDocument error ${response.status}: ${await response.text()}`);
  }
}

const UMBRAL_BAJA_CALIDAD_NORMA = 0.5;

async function getPaginasBajaCalidad(documentKey: string) {
  const { data: norma } = await supabase
    .from("digemid_normas")
    .select("id, document_key, titulo, pdf_url")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (!norma) return null;

  const { data: paginas } = await supabase
    .from("digemid_norma_paginas")
    .select("page_number, quality_score, extraction_method, ocr_confidence, posible_formula, text_normalized, text_raw")
    .eq("norma_id", norma.id)
    .lt("quality_score", UMBRAL_BAJA_CALIDAD_NORMA)
    .eq("revisado_manual", false)
    .order("page_number");

  return { norma, paginas: paginas ?? [] };
}

/** Igual que getPaginasBajaCalidad pero para paginas con tablas detectadas
 * cuya correspondencia fila-columna nadie confirmo contra el PDF: el texto
 * puede tener quality_score alto (es legible) y aun asi tener una tabla con
 * columnas mal asignadas, como se confirmo a mano en DS-13-2022. */
async function getPaginasConTablas(documentKey: string) {
  const { data: norma } = await supabase
    .from("digemid_normas")
    .select("id, document_key, titulo, pdf_url")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (!norma) return null;

  const { data: paginas } = await supabase
    .from("digemid_norma_paginas")
    .select("page_number, quality_score, extraction_method, ocr_confidence, posible_formula, text_normalized, text_raw")
    .eq("norma_id", norma.id)
    .eq("has_tables", true)
    .eq("tabla_verificada", false)
    .order("page_number");

  return { norma, paginas: paginas ?? [] };
}

/** Parte una fila Markdown "| a | b |" en sus celdas, sin partir por un
 * pipe escapado ("\|") que pueda venir dentro del texto de una celda. */
function partirFilaMarkdown(linea: string): string[] {
  const celdas = linea.trim().split(/(?<!\\)\|/).map((c) => c.trim());
  if (celdas.length && celdas[0] === "") celdas.shift();
  if (celdas.length && celdas[celdas.length - 1] === "") celdas.pop();
  return celdas;
}

function esFilaSeparadoraMarkdown(celdas: string[]): boolean {
  return celdas.length > 0 && celdas.every((c) => /^:?-{2,}:?$/.test(c));
}

/** Etiqueta legible de una columna para el formato editable: usa el texto
 * del encabezado, o "Columna N" si venia vacio (columnas sin titulo son
 * comunes en las normas, ej. la 2da columna de varias tablas de DS-13-2022). */
function etiquetaColumna(header: string, indice: number): string {
  const limpio = header.trim();
  return limpio || `Columna ${indice + 1}`;
}

const ANCHO_MAX_COLUMNA_VISTA_PREVIA = 28;

/** Envuelve un texto a lineas de a lo sumo `ancho` caracteres, cortando por
 * palabra completa (nunca a la mitad de una palabra). */
function envolverTexto(texto: string, ancho: number): string[] {
  const palabras = texto.split(/\s+/).filter(Boolean);
  const lineas: string[] = [];
  let actual = "";

  for (const palabra of palabras) {
    const candidato = actual ? `${actual} ${palabra}` : palabra;
    if (candidato.length > ancho && actual) {
      lineas.push(actual);
      actual = palabra;
    } else {
      actual = candidato;
    }
  }
  if (actual) lineas.push(actual);
  return lineas.length ? lineas : [""];
}

/** Grilla de SOLO LECTURA con columnas siempre alineadas: cada celda se
 * envuelve (word-wrap) a un ancho fijo por columna, ocupando varias lineas
 * fisicas si hace falta, con las demas columnas de esa fila acolchadas en
 * blanco en las lineas de continuacion. A diferencia del acolchado sin
 * limite (PR #39), el ancho tope por columna mantiene cada linea corta, asi
 * que no depende de que el editor evite el word-wrap propio. Es solo para
 * que el admin UBIQUE visualmente los datos antes de editar en el bloque
 * "===== TABLA N =====" de abajo — no se vuelve a leer al reconstruir. */
function tablaAGrillaAlineada(filas: string[][], indiceSeparador: number): string[] {
  const nColumnas = filas[indiceSeparador - 1].length;
  const anchos = Array.from({ length: nColumnas }, (_, col) => {
    const max = Math.max(3, ...filas.map((f) => (f[col] ?? "").length));
    return Math.min(max, ANCHO_MAX_COLUMNA_VISTA_PREVIA);
  });

  const filaAGrilla = (fila: string[]): string[] => {
    const celdasEnvueltas = fila.map((c, i) => envolverTexto(c ?? "", anchos[i]));
    const maxLineas = Math.max(...celdasEnvueltas.map((c) => c.length));
    const lineasFila: string[] = [];
    for (let l = 0; l < maxLineas; l++) {
      const partes = celdasEnvueltas.map((c, i) => (c[l] ?? "").padEnd(anchos[i]));
      lineasFila.push("| " + partes.join(" | ") + " |");
    }
    return lineasFila;
  };

  const resultado: string[] = [];
  resultado.push(...filaAGrilla(filas[indiceSeparador - 1]));
  resultado.push("| " + anchos.map((a) => "-".repeat(a)).join(" | ") + " |");
  for (const fila of filas.slice(indiceSeparador + 1)) {
    resultado.push(...filaAGrilla(fila));
  }
  return resultado;
}

/** Convierte una tabla ya partida en filas a: (1) una vista previa de solo
 * lectura con columnas alineadas de verdad (envueltas a ancho fijo, para
 * que el admin ubique visualmente cada dato), seguida de (2) el formato
 * editable "Etiqueta: valor" por fila. Ningun acolchado con espacios puede
 * verse bien en un editor de texto que envuelve lineas largas (confirmado
 * por el usuario), asi que la parte que de verdad se edita usa un campo por
 * linea, que nunca se desalinea sin importar el ancho de la celda ni del
 * editor; la grilla de arriba es solo para lectura, se regenera cada vez y
 * se descarta al reconstruir (reconstruirTablasDesdeTextoEditable). */
function tablaATextoEditable(filas: string[][], indiceSeparador: number, numeroTabla: number): string[] {
  const encabezados = filas[indiceSeparador - 1].map((h, i) => etiquetaColumna(h, i));
  const cuerpo = filas.slice(indiceSeparador + 1);

  const lineas: string[] = [
    `----- VISTA PREVIA TABLA ${numeroTabla} (solo para ubicar los datos; NO la edites, no se guarda) -----`,
    ...tablaAGrillaAlineada(filas, indiceSeparador),
    `----- FIN VISTA PREVIA TABLA ${numeroTabla} -----`,
    "",
    `===== TABLA ${numeroTabla} (edita solo el texto despues de ":" en cada linea; no cambies las etiquetas ni las lineas "=====") =====`,
  ];

  cuerpo.forEach((fila, idx) => {
    lineas.push(`-- Fila ${idx + 1} --`);
    encabezados.forEach((etiqueta, col) => {
      lineas.push(`${etiqueta}: ${fila[col] ?? ""}`);
    });
  });

  lineas.push(`===== FIN TABLA ${numeroTabla} =====`);
  return lineas;
}

/** Reemplaza cada bloque "| ... |" de tabla real en el texto de una pagina
 * por su version en formato editable (tablaATextoEditable). El texto que no
 * es tabla queda intacto. Usada al armar la plantilla .txt que ve el admin. */
function convertirTablasATextoEditable(texto: string): string {
  const lineas = texto.split("\n");
  const resultado: string[] = [];
  let numeroTabla = 0;
  let i = 0;

  while (i < lineas.length) {
    if (!lineas[i].trim().startsWith("|")) {
      resultado.push(lineas[i]);
      i++;
      continue;
    }

    const inicioBloque = i;
    while (i < lineas.length && lineas[i].trim().startsWith("|")) i++;
    const bloque = lineas.slice(inicioBloque, i);

    const filas = bloque.map(partirFilaMarkdown);
    const nColumnas = Math.max(...filas.map((f) => f.length));
    const indiceSeparador = filas.findIndex((f) => esFilaSeparadoraMarkdown(f));

    if (bloque.length < 2 || nColumnas < 2 || indiceSeparador === -1) {
      resultado.push(...bloque);
      continue;
    }

    for (const fila of filas) while (fila.length < nColumnas) fila.push("");
    numeroTabla++;
    resultado.push(...tablaATextoEditable(filas, indiceSeparador, numeroTabla));
  }

  return resultado.join("\n");
}

/** Parsea UN bloque "===== TABLA N ... =====" .. "===== FIN TABLA N ====="
 * (ambas lineas incluidas) de vuelta a filas de tabla Markdown ("| a | b |").
 * Devuelve null si el bloque quedo con un formato irreconocible (por si el
 * admin borro alguna etiqueta o marca por error), para que quien lo llama
 * decida como degradar sin perder informacion. */
function textoEditableATablaMarkdown(lineasBloque: string[]): string[] | null {
  if (lineasBloque.length < 3) return null;

  const filasBody: string[][] = [];
  let etiquetas: string[] = [];
  let filaActual: Map<string, string> | null = null;
  let ordenEtiquetas: string[] = [];

  const cerrarFila = () => {
    if (!filaActual) return;
    if (!etiquetas.length) etiquetas = ordenEtiquetas.slice();
    const fila = filaActual;
    filasBody.push(etiquetas.map((e) => fila.get(e) ?? ""));
    filaActual = null;
  };

  for (let i = 1; i < lineasBloque.length - 1; i++) {
    const linea = lineasBloque[i];

    if (/^--\s*Fila\s+\d+\s*--\s*$/.test(linea.trim())) {
      cerrarFila();
      filaActual = new Map();
      ordenEtiquetas = [];
      continue;
    }

    const match = linea.match(/^(.+?):\s?(.*)$/);
    if (match && filaActual) {
      const [, etiqueta, valor] = match;
      filaActual.set(etiqueta, valor);
      ordenEtiquetas.push(etiqueta);
    }
  }
  cerrarFila();

  if (!etiquetas.length || !filasBody.length) return null;

  const escaparCelda = (v: string) => v.replace(/\|/g, "\\|");
  const filaMarkdown = (celdas: string[]) => "| " + celdas.map(escaparCelda).join(" | ") + " |";

  return [
    filaMarkdown(etiquetas),
    "| " + etiquetas.map(() => "---").join(" | ") + " |",
    ...filasBody.map(filaMarkdown),
  ];
}

/** Recorre el texto corregido que un admin reenvio y reconstruye cualquier
 * bloque "===== TABLA N ... =====" en tabla Markdown de nuevo, para que lo
 * que se guarda en Supabase (y usa /consulta) siga siendo Markdown limpio de
 * una fila por linea, sin importar el formato editable que vio el admin. Si
 * un bloque no se puede interpretar, se deja tal cual (no se pierde texto),
 * aunque queden visibles las marcas "=====". */
function reconstruirTablasDesdeTextoEditable(texto: string): string {
  const lineas = texto.split("\n");
  const resultado: string[] = [];
  let i = 0;

  while (i < lineas.length) {
    // La vista previa es solo de lectura: se descarta entera (editada o no)
    // en vez de guardarla, para no duplicar la tabla dentro del texto final.
    const inicioVistaPrevia = lineas[i].trim().match(/^-----\s*VISTA PREVIA TABLA\s+(\d+)\b.*-----\s*$/);
    if (inicioVistaPrevia) {
      const numeroVista = inicioVistaPrevia[1];
      const finVistaRegex = new RegExp(`^-----\\s*FIN VISTA PREVIA TABLA\\s+${numeroVista}\\s*-----\\s*$`);
      let k = i + 1;
      while (k < lineas.length && !finVistaRegex.test(lineas[k].trim())) k++;
      if (k < lineas.length) {
        i = k + 1;
        if (i < lineas.length && lineas[i].trim() === "") i++;
        continue;
      }
      // No se encontro el cierre de la vista previa: se deja tal cual para
      // no perder texto, aunque queden las marcas visibles.
      resultado.push(lineas[i]);
      i++;
      continue;
    }

    const inicioMatch = lineas[i].trim().match(/^=====\s*TABLA\s+(\d+)\b.*=====\s*$/);
    if (!inicioMatch) {
      resultado.push(lineas[i]);
      i++;
      continue;
    }

    const numero = inicioMatch[1];
    const finRegex = new RegExp(`^=====\\s*FIN TABLA\\s+${numero}\\s*=====\\s*$`);
    const inicioBloque = i;
    let j = i + 1;
    while (j < lineas.length && !finRegex.test(lineas[j].trim())) j++;

    if (j >= lineas.length) {
      resultado.push(lineas[i]);
      i++;
      continue;
    }

    const bloque = lineas.slice(inicioBloque, j + 1);
    const filasMarkdown = textoEditableATablaMarkdown(bloque);
    resultado.push(...(filasMarkdown ?? bloque));
    i = j + 1;
  }

  return resultado.join("\n");
}

/** Arma un <table> HTML real (no texto con pipes) a partir de las filas ya
 * partidas de un bloque Markdown. En un navegador la alineacion de columnas
 * la hace el motor de layout, no espacios contados a mano, asi que no
 * depende de que el visor evite el salto de linea (a diferencia de un
 * archivo de texto plano, donde ningun acolchado con espacios se sostiene
 * si el editor envuelve lineas largas). */
function bloqueTablaAHtml(filas: string[][], indiceSeparador: number): string {
  const filasEncabezado = filas.slice(0, indiceSeparador);
  const filasCuerpo = filas.slice(indiceSeparador + 1);

  const filaHtml = (celdas: string[], etiqueta: "th" | "td") =>
    "<tr>" + celdas.map((c) => `<${etiqueta}>${escapeHtml(c)}</${etiqueta}>`).join("") + "</tr>";

  return (
    `<table class="tabla-extraida">` +
    `<thead>${filasEncabezado.map((f) => filaHtml(f, "th")).join("")}</thead>` +
    `<tbody>${filasCuerpo.map((f) => filaHtml(f, "td")).join("")}</tbody>` +
    `</table>`
  );
}

/** Recorre el texto de una pagina y, cada vez que encuentra un bloque
 * Markdown "| ... |" que parece tabla real, lo reemplaza por un <table>
 * HTML; el resto del texto queda como <pre> normal. */
function renderTextoConTablasHtml(texto: string): string {
  const lineas = texto.split("\n");
  const partes: string[] = [];
  let bufferTexto: string[] = [];
  let i = 0;

  const flushTexto = () => {
    if (bufferTexto.length) {
      partes.push(`<pre>${escapeHtml(bufferTexto.join("\n"))}</pre>`);
      bufferTexto = [];
    }
  };

  while (i < lineas.length) {
    if (!lineas[i].trim().startsWith("|")) {
      bufferTexto.push(lineas[i]);
      i++;
      continue;
    }

    const inicioBloque = i;
    while (i < lineas.length && lineas[i].trim().startsWith("|")) i++;
    const bloque = lineas.slice(inicioBloque, i);

    const filas = bloque.map(partirFilaMarkdown);
    const nColumnas = Math.max(...filas.map((f) => f.length));
    const indiceSeparador = filas.findIndex((f) => esFilaSeparadoraMarkdown(f));

    if (bloque.length < 2 || nColumnas < 2 || indiceSeparador === -1) {
      bufferTexto.push(...bloque);
      continue;
    }

    flushTexto();
    for (const fila of filas) while (fila.length < nColumnas) fila.push("");
    partes.push(bloqueTablaAHtml(filas, indiceSeparador));
  }

  flushTexto();
  return partes.join("\n");
}

function construirReporteHtmlRevision(norma: any, paginas: any[]): string {
  const filas = paginas
    .map((p) => {
      const motivo = [
        `método: ${escapeHtml(p.extraction_method ?? "?")}`,
        p.ocr_confidence != null ? `confianza OCR: ${p.ocr_confidence}` : null,
        p.posible_formula ? "posible fórmula/notación técnica" : null,
      ].filter(Boolean).join(" · ");

      const muestra = renderTextoConTablasHtml(p.text_normalized ?? p.text_raw ?? "");

      return `
        <tr>
          <td>${p.page_number}</td>
          <td>${p.quality_score}</td>
          <td>${motivo}</td>
          <td>${muestra}</td>
        </tr>`;
    })
    .join("\n");

  return `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Revisión — ${escapeHtml(norma.document_key)}</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }
  h1 { font-size: 1.3rem; }
  table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
  th, td { border: 1px solid #ccc; padding: 0.5rem; vertical-align: top; text-align: left; }
  th { background: #f0f0f0; }
  pre { white-space: pre-wrap; word-break: break-word; margin: 0; font-size: 0.85rem; }
  .nota { background: #fff8e1; border: 1px solid #ffe082; padding: 0.75rem; border-radius: 6px; }
  /* Tablas extraidas de una pagina (dentro de la celda "Texto extraido"):
     scroll horizontal propio para que una tabla ancha no rompa el layout
     de la tabla de revision que la contiene. */
  td:has(table.tabla-extraida) { overflow-x: auto; display: block; max-width: 90vw; }
  table.tabla-extraida { width: auto; margin: 0.5rem 0; }
  table.tabla-extraida th, table.tabla-extraida td {
    border: 1px solid #999; padding: 0.35rem 0.6rem; font-size: 0.8rem;
    max-width: 420px; white-space: normal; word-break: break-word;
  }
  table.tabla-extraida th { background: #eef2ff; }
</style>
</head>
<body>
  <h1>📋 Revisión de baja confiabilidad — ${escapeHtml(norma.document_key)}</h1>
  <p>${escapeHtml(norma.titulo ?? "")}</p>
  <div class="nota">
    Corrige el texto en la plantilla .txt que te envié junto a este reporte,
    comparando con el PDF adjunto, y reenvía ese mismo archivo .txt a este
    chat cuando termines. No necesitas entrar a Supabase.
  </div>
  <table>
    <thead><tr><th>Página</th><th>Calidad</th><th>Motivo</th><th>Texto extraído (muestra)</th></tr></thead>
    <tbody>${filas}</tbody>
  </table>
</body>
</html>`;
}

function construirPlantillaTxtRevision(documentKey: string, paginas: any[]): string {
  const encabezado = [
    `# NORMA: ${documentKey}`,
    "# Instrucciones: corrige el texto de cada pagina comparando con el PDF adjunto.",
    "# No modifiques ni borres las lineas que empiezan con \"### PAGINA\".",
    "# Las tablas se muestran como \"Etiqueta: valor\" por fila (no como columnas con |).",
    "# Edita solo lo que va despues de los \":\" -- no cambies las etiquetas ni las lineas \"=====\".",
    "# Cuando termines, reenvia este mismo archivo (como documento, no como texto) a este chat.",
    "",
  ].join("\n");

  const bloques = paginas.map((p) => {
    const motivo = [
      `calidad actual: ${p.quality_score}`,
      `metodo: ${p.extraction_method ?? "?"}`,
      p.ocr_confidence != null ? `confianza ocr: ${p.ocr_confidence}` : null,
      p.posible_formula ? "posible formula/notacion tecnica" : null,
    ].filter(Boolean).join(", ");

    const texto = convertirTablasATextoEditable(p.text_normalized ?? p.text_raw ?? "");
    return `### PAGINA ${p.page_number} (${motivo})\n${texto}\n`;
  });

  return `${encabezado}\n${bloques.join("\n")}`;
}

/** Genera y envia el reporte HTML, el PDF y la plantilla .txt de una norma
 * con paginas de baja confiabilidad. Usada tanto por /normarevisar como por
 * el boton correspondiente en /normasrevisar. */
async function enviarRevisionNorma(chatId: string, documentKey: string) {
  try {
    const resultado = await getPaginasBajaCalidad(documentKey);

    if (!resultado) {
      return await sendMessage(chatId, `No encontré ninguna norma con document_key "${escapeHtml(documentKey)}".`);
    }

    const { norma, paginas } = resultado;

    if (!paginas.length) {
      return await sendMessage(chatId, `✅ "${escapeHtml(documentKey)}" no tiene páginas pendientes de revisión.`);
    }

    const html = construirReporteHtmlRevision(norma, paginas);
    await enviarDocumentoTexto(
      chatId,
      html,
      `revision_${documentKey}.html`,
      "text/html",
      `📋 Reporte de revisión — ${documentKey} (${paginas.length} página(s))`,
    );

    if (norma.pdf_url) {
      await telegram("sendDocument", {
        chat_id: chatId,
        document: norma.pdf_url,
        caption: `📄 PDF original — ${documentKey}`,
      });
    }

    const plantilla = construirPlantillaTxtRevision(documentKey, paginas);
    await enviarDocumentoTexto(
      chatId,
      plantilla,
      `plantilla_${documentKey}.txt`,
      "text/plain",
      "✏️ Corrige el texto de cada página comparando con el PDF y reenvía este mismo archivo (como documento) a este chat cuando termines.",
    );

    return new Response("OK", { status: 200 });
  } catch (error) {
    console.error("NORMAREVISAR_ERROR:", error);
    return await sendMessage(chatId, `⚠️ Error al generar la revisión: ${escapeHtml(String(error))}`);
  }
}

/** Igual que enviarRevisionNorma pero para paginas con tablas sin verificar
 * (texto legible, pero nadie confirmo que la tabla quedo bien reconstruida
 * columna por columna). Reutiliza el mismo reporte HTML y la misma
 * plantilla .txt: el admin compara la tabla en Markdown contra el PDF y, si
 * esta bien, reenvia el archivo sin cambios; si esta mal, corrige el bloque
 * de la tabla a mano antes de reenviarlo. */
async function enviarRevisionTablas(chatId: string, documentKey: string) {
  try {
    const resultado = await getPaginasConTablas(documentKey);

    if (!resultado) {
      return await sendMessage(chatId, `No encontré ninguna norma con document_key "${escapeHtml(documentKey)}".`);
    }

    const { norma, paginas } = resultado;

    if (!paginas.length) {
      return await sendMessage(chatId, `✅ "${escapeHtml(documentKey)}" no tiene tablas pendientes de verificación.`);
    }

    const html = construirReporteHtmlRevision(norma, paginas);
    await enviarDocumentoTexto(
      chatId,
      html,
      `tablas_${documentKey}.html`,
      "text/html",
      `📋 Reporte de tablas — ${documentKey} (${paginas.length} página(s))`,
    );

    if (norma.pdf_url) {
      await telegram("sendDocument", {
        chat_id: chatId,
        document: norma.pdf_url,
        caption: `📄 PDF original — ${documentKey}`,
      });
    }

    const plantilla = construirPlantillaTxtRevision(documentKey, paginas);
    await enviarDocumentoTexto(
      chatId,
      plantilla,
      `tablas_${documentKey}.txt`,
      "text/plain",
      "✏️ Compara el bloque \"Tabla:\" de cada página contra el PDF. Si la tabla está bien asignada (exportador/importador, montos, plazos, etc.), reenvía este mismo archivo sin cambios; si está mal, corrígela y reenvíalo.",
    );

    return new Response("OK", { status: 200 });
  } catch (error) {
    console.error("TABLAREVISAR_ERROR:", error);
    return await sendMessage(chatId, `⚠️ Error al generar la revisión de tablas: ${escapeHtml(String(error))}`);
  }
}

/** Parsea la plantilla .txt devuelta por un admin y aplica la correccion. */
async function aplicarRevisionManualNorma(
  contenido: string,
): Promise<{ ok: boolean; mensaje: string; documentKey?: string; paginasActualizadas?: number }> {
  const matchNorma = contenido.match(/^#\s*NORMA:\s*(.+)$/m);
  if (!matchNorma) {
    return { ok: false, mensaje: "No encontré la línea \"# NORMA: ...\" en el archivo. Usa la plantilla generada por /normarevisar sin borrar esa línea." };
  }

  const documentKey = matchNorma[1].trim();

  const { data: norma } = await supabase
    .from("digemid_normas")
    .select("id")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (!norma) {
    return { ok: false, mensaje: `No encontré ninguna norma con document_key "${documentKey}".` };
  }

  // Cada bloque empieza con "### PAGINA <n> (...)"; se parte por ese
  // marcador y se descarta lo anterior al primero (el encabezado con
  // instrucciones, que no es contenido de ninguna pagina).
  const partes = contenido.split(/^### PAGINA /m).slice(1);

  if (!partes.length) {
    return { ok: false, mensaje: "No encontré ningún bloque \"### PAGINA N (...)\" en el archivo." };
  }

  let actualizadas = 0;
  const ahora = new Date().toISOString();

  for (const parte of partes) {
    const match = parte.match(/^(\d+)\s*\([^)]*\)\s*\n([\s\S]*)$/);
    if (!match) continue;

    const pageNumber = parseInt(match[1], 10);
    const textoCorregido = reconstruirTablasDesdeTextoEditable(match[2].trim().normalize("NFC"));

    if (!textoCorregido) continue;

    const { error } = await supabase
      .from("digemid_norma_paginas")
      .update({
        text_raw: textoCorregido,
        text_normalized: textoCorregido,
        quality_score: 1,
        extraction_method: "revision_manual",
        revisado_manual: true,
        revisado_en: ahora,
        // Cualquier pagina que un admin reenvia via este flujo ya fue
        // comparada contra el PDF (venga de /normarevisar o /tablarevisar),
        // asi que tambien sale de la cola de tablas pendientes si aplicaba.
        tabla_verificada: true,
        tabla_verificada_en: ahora,
        updated_at: ahora,
      })
      .eq("norma_id", norma.id)
      .eq("page_number", pageNumber);

    if (!error) actualizadas += 1;
  }

  return { ok: true, mensaje: "ok", documentKey, paginasActualizadas: actualizadas };
}

const NORMATIVA_STORAGE_BUCKET = "digemid-documentos";

const NORMAPDF_PENDIENTE_MINUTOS = 30;

/** Envia las instrucciones para subir a mano el PDF de UNA norma puntual.
 * Ademas de la opcion de caption ("/normapdf DOCUMENT_KEY" adjunto al PDF),
 * deja registrado en digemid_normapdf_pendientes que este chat espera el PDF
 * de esta norma durante NORMAPDF_PENDIENTE_MINUTOS: asi, en Telegram movil
 * (donde adjuntar archivo y escribir caption a la vez es incomodo), basta con
 * mandar el PDF solo, sin nada mas, y se ata a la norma correcta igual. */
async function enviarInstruccionNormaPdf(chatId: string, documentKey: string): Promise<Response> {
  const { data: norma } = await supabase
    .from("digemid_normas")
    .select("document_key, titulo, pdf_url, process_status")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (!norma) {
    return await sendMessage(chatId, `No encontré ninguna norma con document_key "${escapeHtml(documentKey)}".`);
  }

  const expiraEn = new Date(Date.now() + NORMAPDF_PENDIENTE_MINUTOS * 60 * 1000).toISOString();
  await supabase
    .from("digemid_normapdf_pendientes")
    .upsert({ chat_id: chatId, document_key: documentKey, expira_en: expiraEn }, { onConflict: "chat_id" });

  return await sendMessage(
    chatId,
    `📄 Listo. Ahora mándame el PDF de <b>${escapeHtml(documentKey)}</b>` +
      `${norma.titulo ? ` (${escapeHtml(norma.titulo)})` : ""} como documento adjunto — ` +
      "no necesitas escribir nada más, ni como pie de foto ni aparte.\n\n" +
      `Tienes ${NORMAPDF_PENDIENTE_MINUTOS} minutos. Si prefieres, también puedes seguir usando el pie de foto ` +
      `<code>/normapdf ${escapeHtml(documentKey)}</code> al adjuntar el archivo, como antes.`,
  );
}

const ESTADO_VIGENCIA_POR_RELACION: Record<string, string> = {
  deroga: "derogada",
  deja_sin_efecto: "derogada",
  modifica: "modificada",
};

/** Genera un document_key para una norma que no existe en la base (solo se
 * conoce por mencion de otra norma que la deroga/modifica). Si no hay
 * tipo+numero+anio completos (norma citada de forma ambigua) cae a un hash
 * corto del texto para no bloquear la confirmacion del admin. */
function construirDocumentKeyStub(
  tipoNorma: string | null,
  numero: string | null,
  anio: number | null,
  descripcion: string,
): string {
  if (tipoNorma && numero && anio) {
    return `${tipoNorma.toUpperCase()}-${numero}-${anio}`;
  }
  const slug = descripcion.replace(/[^A-Za-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 40).toUpperCase();
  return `NORM-${slug || crypto.randomUUID().slice(0, 8).toUpperCase()}`;
}

/** Resuelve (confirma o rechaza) una relacion de derogacion/modificacion
 * propuesta por la IA (scripts/detectar_derogaciones_normativa.py). Nunca se
 * aplica sola: siempre pasa por este flujo manual via botones porque un
 * error de la IA aqui significa marcar mal una norma legal. Si la norma
 * afectada no existe todavia en la base, se crea un registro minimo (stub,
 * sin PDF) solo para dejar constancia de que quedo derogada/modificada. */
async function resolverRelacionDerogacion(
  chatId: string,
  messageId: number | undefined,
  relacionId: string,
  accion: "confirmar" | "rechazar",
): Promise<void> {
  const { data: relacion } = await supabase
    .from("digemid_norma_relaciones")
    .select("*")
    .eq("id", relacionId)
    .maybeSingle();

  if (!relacion) {
    await sendMessage(chatId, "⚠️ No encontré esa relación (puede que ya no exista).");
    return;
  }

  if (relacion.estado !== "pendiente") {
    await sendMessage(chatId, `Esta relación ya estaba marcada como "${escapeHtml(relacion.estado)}".`);
    return;
  }

  if (accion === "rechazar") {
    await supabase
      .from("digemid_norma_relaciones")
      .update({ estado: "rechazada", resuelto_por: chatId, resuelto_en: new Date().toISOString() })
      .eq("id", relacionId);

    if (messageId) {
      await editMessage(
        chatId,
        messageId,
        `❌ Rechazado: <b>${escapeHtml(relacion.norma_origen_document_key)}</b> → ${escapeHtml(relacion.descripcion_afectada)}`,
      );
    }
    return;
  }

  let normaAfectadaId: string | null = relacion.norma_afectada_id;
  const estadoVigencia = ESTADO_VIGENCIA_POR_RELACION[relacion.tipo_relacion] ?? "derogada";

  if (!normaAfectadaId) {
    const documentKeyStub = construirDocumentKeyStub(
      relacion.tipo_norma_afectada,
      relacion.numero_afectada,
      relacion.anio_afectada,
      relacion.descripcion_afectada,
    );

    const { data: normaExistente } = await supabase
      .from("digemid_normas")
      .select("id")
      .eq("document_key", documentKeyStub)
      .maybeSingle();

    if (normaExistente) {
      normaAfectadaId = normaExistente.id;
    } else {
      const { data: normaCreada, error: errorCreacion } = await supabase
        .from("digemid_normas")
        .insert({
          document_key: documentKeyStub,
          tipo_norma: relacion.tipo_norma_afectada,
          numero: relacion.numero_afectada,
          anio: relacion.anio_afectada,
          titulo: relacion.descripcion_afectada,
          has_file: false,
          process_status: "stub_derogada",
          estado_vigencia: estadoVigencia,
        })
        .select("id")
        .single();

      if (errorCreacion || !normaCreada) {
        await sendMessage(
          chatId,
          `⚠️ No pude crear el registro de "${escapeHtml(relacion.descripcion_afectada)}": ` +
            escapeHtml(errorCreacion?.message ?? "error desconocido"),
        );
        return;
      }

      normaAfectadaId = normaCreada.id;
    }
  } else {
    await supabase.from("digemid_normas").update({ estado_vigencia: estadoVigencia }).eq("id", normaAfectadaId);
  }

  await supabase
    .from("digemid_norma_relaciones")
    .update({
      estado: "confirmada",
      norma_afectada_id: normaAfectadaId,
      resuelto_por: chatId,
      resuelto_en: new Date().toISOString(),
    })
    .eq("id", relacionId);

  if (messageId) {
    const verbo = relacion.tipo_relacion === "modifica" ? "modificó" : "derogó";
    await editMessage(
      chatId,
      messageId,
      `✅ Confirmado: <b>${escapeHtml(relacion.norma_origen_document_key)}</b> ${verbo} a ` +
        `<b>${escapeHtml(relacion.descripcion_afectada)}</b>.`,
    );
  }
}

function porcentaje(n: number, total: number): string {
  if (!total) return "0%";
  return `${((n / total) * 100).toFixed(1)}%`;
}

/** Reporte de fidelidad para UNA norma: agrega las señales de calidad ya
 * guardadas por página (quality_score, ocr_confidence, posible_formula,
 * posible_grafico, has_tables, revisado_manual) en un veredicto legible.
 * posible_grafico es una heuristica de imagen embebida (no interpreta el
 * contenido) que solo corre en paginas procesadas de aqui en adelante. */
async function construirReporteEstadoNorma(documentKey: string): Promise<string> {
  const { data: norma } = await supabase
    .from("digemid_normas")
    .select("id, document_key, titulo")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (!norma) {
    return `⚠️ No encontré ninguna norma con document_key "${escapeHtml(documentKey)}".`;
  }

  const { data: paginas, error } = await supabase
    .from("digemid_norma_paginas")
    .select("page_number, quality_score, extraction_method, posible_formula, posible_grafico, revisado_manual, has_tables, tabla_verificada")
    .eq("norma_id", norma.id)
    .order("page_number");

  if (error) {
    return `⚠️ Error al consultar: ${escapeHtml(error.message)}`;
  }
  if (!paginas || !paginas.length) {
    return `⚠️ La norma ${escapeHtml(documentKey)} no tiene páginas registradas todavía.`;
  }

  const total = paginas.length;
  const alta = paginas.filter((p) => (p.quality_score ?? 0) >= 0.85).length;
  const media = paginas.filter((p) => (p.quality_score ?? 0) >= UMBRAL_BAJA_CALIDAD_NORMA && (p.quality_score ?? 0) < 0.85).length;
  const baja = paginas.filter((p) => (p.quality_score ?? 0) < UMBRAL_BAJA_CALIDAD_NORMA).length;
  const bajaSinRevisar = paginas.filter((p) => (p.quality_score ?? 0) < UMBRAL_BAJA_CALIDAD_NORMA && !p.revisado_manual).length;
  const formulas = paginas.filter((p) => p.posible_formula).length;
  const graficos = paginas.filter((p) => p.posible_grafico).length;
  const revisadas = paginas.filter((p) => p.revisado_manual).length;
  const tablas = paginas.filter((p) => p.has_tables).length;
  const tablasSinVerificar = paginas.filter((p) => p.has_tables && !p.tabla_verificada).length;
  const ocr = paginas.filter((p) => (p.extraction_method ?? "").toLowerCase().includes("ocr")).length;

  let veredicto: string;
  let detalle: string;
  if (bajaSinRevisar > 0 || formulas > 0 || graficos > 0) {
    veredicto = "⚠️ Necesita revisión manual";
    detalle = [
      bajaSinRevisar > 0 ? `${bajaSinRevisar} página(s) de calidad muy baja sin revisar` : null,
      formulas > 0 ? `${formulas} página(s) con posible fórmula/notación técnica` : null,
      graficos > 0 ? `${graficos} página(s) con posible gráfico/imagen sin revisar` : null,
    ].filter(Boolean).join(", ") + ".";
  } else if (tablasSinVerificar > 0 || ocr > 0 || media > 0) {
    veredicto = "🟡 Usar con precaución";
    detalle = [
      tablasSinVerificar > 0 ? `${tablasSinVerificar} página(s) con tabla sin verificar` : null,
      ocr > 0 ? `${ocr} página(s) vía OCR` : null,
      media > 0 ? `${media} página(s) de calidad media` : null,
    ].filter(Boolean).join(", ") + ". La IA ya avisa de esto en sus respuestas, pero ningún admin lo confirmó a mano.";
  } else {
    veredicto = "✅ Confiable";
    detalle = tablas > 0
      ? "Todas las tablas ya fueron verificadas a mano y el resto del texto es de calidad alta."
      : "Todo el texto es de calidad alta y no depende de tablas ni OCR. Aun así, ningún admin la ha confirmado con /normarevisar.";
  }

  const lineas = [
    `📊 <b>Estado de fidelidad — ${escapeHtml(documentKey)}</b>`,
    norma.titulo ? escapeHtml(norma.titulo) : "",
    "",
    `Páginas totales: <b>${total}</b>`,
    `Calidad alta (≥0.85): ${alta} (${porcentaje(alta, total)})`,
    `Calidad media (0.5–0.85): ${media} (${porcentaje(media, total)})`,
    `Calidad muy baja (<0.5): ${baja} (${porcentaje(baja, total)})`,
    `Con tablas: ${tablas} (${tablasSinVerificar} sin verificar)`,
    `Extraídas vía OCR: ${ocr} (${porcentaje(ocr, total)})`,
    `Con posible fórmula sin reconstruir: ${formulas}`,
    `Con posible gráfico/imagen sin revisar: ${graficos}`,
    `Revisadas manualmente por un admin: ${revisadas} de ${total} (${porcentaje(revisadas, total)})`,
    "",
    `<b>Veredicto: ${veredicto}</b>`,
    detalle,
    "",
    "⚠️ La detección de gráficos es una heurística nueva (imagen embebida de tamaño razonable, no interpreta el contenido) y solo corre en páginas procesadas de aquí en adelante — si esta norma no se ha reprocesado, el conteo de arriba puede salir en 0 aunque sí tenga gráficos.",
  ];

  if (bajaSinRevisar > 0) {
    lineas.push("", `Usa <code>/normarevisar ${escapeHtml(documentKey)}</code> para corregir las páginas pendientes.`);
  }
  if (tablasSinVerificar > 0) {
    lineas.push("", `Usa <code>/tablarevisar ${escapeHtml(documentKey)}</code> para verificar las tablas pendientes.`);
  }

  return lineas.join("\n");
}

/** Mismo reporte que construirReporteEstadoNorma pero agregado sobre toda la
 * base, usando conteos (head:true) en vez de traer todas las filas. */
async function construirReporteEstadoGlobal(): Promise<string> {
  async function contar(build: (q: any) => any): Promise<number> {
    const base = supabase.from("digemid_norma_paginas").select("id", { count: "exact", head: true });
    const { count, error } = await build(base);
    if (error) throw new Error(error.message);
    return count ?? 0;
  }

  const [total, alta, media, baja, bajaSinRevisar, formulas, graficos, revisadas, tablas, tablasSinVerificar, ocr] = await Promise.all([
    contar((q) => q),
    contar((q) => q.gte("quality_score", 0.85)),
    contar((q) => q.gte("quality_score", UMBRAL_BAJA_CALIDAD_NORMA).lt("quality_score", 0.85)),
    contar((q) => q.lt("quality_score", UMBRAL_BAJA_CALIDAD_NORMA)),
    contar((q) => q.lt("quality_score", UMBRAL_BAJA_CALIDAD_NORMA).eq("revisado_manual", false)),
    contar((q) => q.eq("posible_formula", true)),
    contar((q) => q.eq("posible_grafico", true)),
    contar((q) => q.eq("revisado_manual", true)),
    contar((q) => q.eq("has_tables", true)),
    contar((q) => q.eq("has_tables", true).eq("tabla_verificada", false)),
    contar((q) => q.ilike("extraction_method", "%ocr%")),
  ]);

  const lineas = [
    "📊 <b>Estado de fidelidad — toda la base</b>",
    "",
    `Páginas totales: <b>${total}</b>`,
    `Calidad alta (≥0.85): ${alta} (${porcentaje(alta, total)})`,
    `Calidad media (0.5–0.85): ${media} (${porcentaje(media, total)})`,
    `Calidad muy baja (<0.5): ${baja} (${porcentaje(baja, total)})`,
    `Con tablas: ${tablas} (<b>${tablasSinVerificar}</b> sin verificar)`,
    `Extraídas vía OCR: ${ocr} (${porcentaje(ocr, total)})`,
    `Con posible fórmula sin reconstruir: ${formulas}`,
    `Con posible gráfico/imagen sin revisar: ${graficos}`,
    `Revisadas manualmente por un admin: <b>${revisadas}</b> de ${total} (${porcentaje(revisadas, total)})`,
    `Calidad muy baja aún sin revisar: ${bajaSinRevisar}`,
    "",
    "⚠️ La detección de gráficos es una heurística nueva (imagen embebida de tamaño razonable) y solo corre en páginas procesadas de aquí en adelante — las páginas ya extraídas antes quedan en 0 hasta que se reprocesen. No interpreta el contenido del gráfico, solo avisa que hay una imagen para revisar a mano.",
    "",
    "Usa <code>/normaestado document_key</code> para el detalle de una norma puntual, <code>/reportenormas</code> para el reporte de todas las normas en un HTML, <code>/normasrevisar</code> para páginas de baja calidad, o <code>/tablasrevisar</code> para tablas sin verificar.",
  ];

  return lineas.join("\n");
}

type FilaResumenNorma = {
  id: string;
  document_key: string;
  titulo: string | null;
  anio: number | null;
  pdf_url: string | null;
  file_storage_path: string | null;
  process_status: string | null;
  total_paginas: number;
  calidad_alta: number;
  calidad_media: number;
  calidad_baja: number;
  calidad_baja_sin_revisar: number;
  tablas_total: number;
  tablas_sin_verificar: number;
  formulas: number;
  graficos: number;
  revisadas_manual: number;
  via_ocr: number;
};

type EstadoNorma = {
  prioridad: number;
  etiqueta: string;
  clase: string;
};

/** Calcula un veredicto de una sola etiqueta por norma para el reporte
 * maestro, en el mismo orden de urgencia con el que un admin deberia
 * atenderlas: primero lo que bloquea totalmente (sin PDF, sin procesar),
 * despues lo que necesita ojo humano (calidad baja, formulas, graficos),
 * despues lo que solo necesita precaucion (tablas/OCR sin verificar),
 * y al final lo ya confiable. Menor prioridad = mas urgente. */
function calcularEstadoNorma(fila: FilaResumenNorma): EstadoNorma {
  const sinPdf = !fila.pdf_url && !fila.file_storage_path;
  const sinProcesar = fila.total_paginas === 0;
  const necesitaRevision = fila.calidad_baja_sin_revisar > 0 || fila.formulas > 0 || fila.graficos > 0;
  const usarConPrecaucion = fila.tablas_sin_verificar > 0 || fila.via_ocr > 0 || fila.calidad_media > 0;

  if (sinPdf) return { prioridad: 0, etiqueta: "🔴 Falta subir PDF", clase: "mal" };
  if (sinProcesar) return { prioridad: 1, etiqueta: "🔴 Sin procesar", clase: "mal" };
  if ((fila.process_status ?? "").includes("error")) {
    return { prioridad: 1, etiqueta: `🔴 Error: ${fila.process_status}`, clase: "mal" };
  }
  if (necesitaRevision) return { prioridad: 2, etiqueta: "⚠️ Necesita revisión manual", clase: "regular" };
  if (usarConPrecaucion) return { prioridad: 3, etiqueta: "🟡 Usar con precaución", clase: "precaucion" };
  return { prioridad: 4, etiqueta: "✅ Confiable", clase: "bien" };
}

/** Reporte maestro de TODAS las normas/reglamentos en un solo HTML: estado
 * de PDF, calidad de texto, tablas, formulas y graficos por norma, para ir
 * levantando observaciones sin tener que consultar norma por norma. */
async function construirReporteMaestroNormas(): Promise<string> {
  const { data, error } = await supabase
    .from("digemid_normas_resumen")
    .select("*")
    .order("document_key");

  if (error) throw new Error(error.message);
  const filas = (data ?? []) as FilaResumenNorma[];

  const conEstado = filas
    .map((f) => ({ fila: f, estado: calcularEstadoNorma(f) }))
    .sort((a, b) => a.estado.prioridad - b.estado.prioridad || a.fila.document_key.localeCompare(b.fila.document_key));

  const resumen = {
    total: filas.length,
    sinPdf: conEstado.filter((x) => x.estado.prioridad === 0).length,
    sinProcesarOError: conEstado.filter((x) => x.estado.prioridad === 1).length,
    necesitaRevision: conEstado.filter((x) => x.estado.prioridad === 2).length,
    precaucion: conEstado.filter((x) => x.estado.prioridad === 3).length,
    confiable: conEstado.filter((x) => x.estado.prioridad === 4).length,
    conTablasPendientes: filas.filter((f) => f.tablas_sin_verificar > 0).length,
    conFormulas: filas.filter((f) => f.formulas > 0).length,
    conGraficos: filas.filter((f) => f.graficos > 0).length,
  };

  const filasHtml = conEstado
    .map(({ fila, estado }) => {
      const pdfTexto = fila.pdf_url || fila.file_storage_path ? "✅" : "❌";
      const calidadTexto = fila.total_paginas
        ? `${porcentaje(fila.calidad_alta, fila.total_paginas)} alta`
        : "—";
      return `
        <tr class="${estado.clase}">
          <td>${escapeHtml(fila.document_key)}</td>
          <td class="titulo">${escapeHtml(fila.titulo ?? "")}</td>
          <td>${fila.anio ?? "—"}</td>
          <td class="centro">${pdfTexto}</td>
          <td class="centro">${fila.total_paginas}</td>
          <td class="centro">${calidadTexto}</td>
          <td class="centro">${fila.tablas_total ? `${fila.tablas_total - fila.tablas_sin_verificar}/${fila.tablas_total}` : "—"}</td>
          <td class="centro">${fila.formulas || "—"}</td>
          <td class="centro">${fila.graficos || "—"}</td>
          <td>${estado.etiqueta}</td>
        </tr>`;
    })
    .join("\n");

  return `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Reporte maestro — normas DIGEMID</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }
  h1 { font-size: 1.3rem; }
  .tarjetas { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 1rem 0 1.5rem; }
  .tarjeta { border: 1px solid #ddd; border-radius: 8px; padding: 0.75rem 1rem; min-width: 140px; }
  .tarjeta .num { font-size: 1.4rem; font-weight: bold; display: block; }
  .tarjeta.mal { background: #ffebee; border-color: #ef9a9a; }
  .tarjeta.regular { background: #fff3e0; border-color: #ffcc80; }
  .tarjeta.precaucion { background: #fffde7; border-color: #fff59d; }
  .tarjeta.bien { background: #e8f5e9; border-color: #a5d6a7; }
  table { border-collapse: collapse; width: 100%; font-size: 0.85rem; }
  th, td { border: 1px solid #ddd; padding: 0.4rem 0.6rem; text-align: left; vertical-align: top; }
  th { background: #f5f5f5; position: sticky; top: 0; }
  td.centro { text-align: center; }
  td.titulo { max-width: 320px; }
  tr.mal { background: #fff5f5; }
  tr.regular { background: #fffaf0; }
  tr.precaucion { background: #fffef5; }
  .nota { background: #fff8e1; border: 1px solid #ffe082; padding: 0.75rem; border-radius: 6px; margin-bottom: 1.5rem; }
</style>
</head>
<body>
  <h1>📋 Reporte maestro — todas las normas y reglamentos DIGEMID</h1>
  <p>Generado ${escapeHtml(new Date().toISOString())} · ordenado de más a menos urgente.</p>

  <div class="tarjetas">
    <div class="tarjeta"><span class="num">${resumen.total}</span>normas totales</div>
    <div class="tarjeta mal"><span class="num">${resumen.sinPdf}</span>sin PDF subido</div>
    <div class="tarjeta mal"><span class="num">${resumen.sinProcesarOError}</span>sin procesar / con error</div>
    <div class="tarjeta regular"><span class="num">${resumen.necesitaRevision}</span>necesitan revisión manual</div>
    <div class="tarjeta precaucion"><span class="num">${resumen.precaucion}</span>usar con precaución</div>
    <div class="tarjeta bien"><span class="num">${resumen.confiable}</span>confiables</div>
    <div class="tarjeta"><span class="num">${resumen.conTablasPendientes}</span>con tablas sin verificar</div>
    <div class="tarjeta"><span class="num">${resumen.conFormulas}</span>con posible fórmula</div>
    <div class="tarjeta"><span class="num">${resumen.conGraficos}</span>con posible gráfico</div>
  </div>

  <div class="nota">
    <b>Cómo actuar según la columna "Estado":</b> "Falta subir PDF" → <code>/normapdf clave</code> (ver <code>/normassinpdf</code>).
    "Sin procesar" → espera la corrida automática o revisa <code>process_status</code>.
    "Necesita revisión manual" → <code>/normarevisar clave</code>.
    "Usar con precaución" (tablas sin verificar) → <code>/tablarevisar clave</code>.
    <br><br>
    <b>Sobre "Gráficos":</b> es una heurística de imagen embebida (tamaño razonable, ni logo ni escaneo de página completa) —
    NO interpreta el contenido del gráfico, solo avisa que hay una imagen para que un humano la revise contra el PDF.
    Solo corre en páginas procesadas de aquí en adelante: una norma ya procesada antes de este cambio puede tener
    gráficos reales y aun así mostrar 0 aquí, hasta que se reprocese.
  </div>

  <table>
    <thead>
      <tr>
        <th>Clave</th><th>Título</th><th>Año</th><th>PDF</th><th>Págs.</th>
        <th>Calidad alta</th><th>Tablas (verif/total)</th><th>Fórmulas</th><th>Gráficos</th><th>Estado</th>
      </tr>
    </thead>
    <tbody>${filasHtml}</tbody>
  </table>
</body>
</html>`;
}

/** Guarda el PDF que un admin subio a mano para UNA norma puntual: lo valida,
 * lo sube a Supabase Storage (mismo bucket que usa el respaldo automatico) y
 * deja la norma en cola para que la corrida horaria de
 * extract_normativa_text_simple.py la descargue y extraiga el texto. */
async function manejarPdfManual(
  chatId: string,
  documentKey: string,
  fileId: string,
  fileName: string,
): Promise<Response> {
  const { data: norma } = await supabase
    .from("digemid_normas")
    .select("id, document_key")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (!norma) {
    await sendMessage(
      chatId,
      `⚠️ No encontré ninguna norma con document_key "${escapeHtml(documentKey)}". ` +
        "Verifica el texto del pie de foto (caption) del PDF.",
    );
    return new Response("OK", { status: 200 });
  }

  try {
    const bytes = await descargarArchivoTelegram(fileId);

    const esPdfValido = bytes.length >= 100 &&
      bytes[0] === 0x25 && bytes[1] === 0x50 && bytes[2] === 0x44 && bytes[3] === 0x46; // %PDF

    if (!esPdfValido) {
      await sendMessage(
        chatId,
        `⚠️ El archivo no parece un PDF válido (no empieza con %PDF o es muy pequeño). ` +
          `No se guardó nada para "${escapeHtml(documentKey)}".`,
      );
      return new Response("OK", { status: 200 });
    }

    const nombreSeguro = (fileName || `${documentKey}.pdf`).replace(/[^A-Za-z0-9._-]+/g, "-");
    const rutaObjeto = `normas/${documentKey}/${nombreSeguro}`;

    const { error: errorSubida } = await supabase.storage
      .from(NORMATIVA_STORAGE_BUCKET)
      .upload(rutaObjeto, bytes, { contentType: "application/pdf", upsert: true });

    if (errorSubida) throw errorSubida;

    // URL firmada de 48h: sobra tiempo para que la corrida horaria (cada 1h)
    // la descargue y extraiga el texto; no necesita ser permanente porque
    // file_storage_path ya queda como respaldo durable una vez procesada.
    const { data: firmada, error: errorFirma } = await supabase.storage
      .from(NORMATIVA_STORAGE_BUCKET)
      .createSignedUrl(rutaObjeto, 60 * 60 * 48);

    if (errorFirma || !firmada?.signedUrl) {
      throw errorFirma ?? new Error("No se pudo generar la URL firmada del PDF.");
    }

    await supabase
      .from("digemid_normas")
      .update({
        pdf_url: firmada.signedUrl,
        file_storage_path: rutaObjeto,
        process_status: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", norma.id);

    await sendMessage(
      chatId,
      `✅ PDF recibido para <b>${escapeHtml(documentKey)}</b>. Quedó en cola: la próxima corrida ` +
        "horaria (máx. 1h) lo descargará, extraerá el texto y te avisará si detecta tablas o baja calidad.",
    );
  } catch (error) {
    console.error("NORMAPDF_UPLOAD_ERROR:", error);
    await sendMessage(
      chatId,
      `⚠️ No pude guardar el PDF de "${escapeHtml(documentKey)}": ${escapeHtml(String(error))}`,
    );
  }

  return new Response("OK", { status: 200 });
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
    "<b>🔊 Escuchar respuesta</b>",
    "En respuestas de /consulta (planes básico, consultoría y empresarial) aparece un botón para recibir la misma respuesta en audio.",
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
    "",
    "<b>/normasrevisar</b>",
    "Lista las normas con páginas de baja confiabilidad pendientes de revisión.",
    "",
    "<b>/normassinpdf</b>",
    "Lista las normas sin PDF confirmado, con botón para subirlo a mano una por una.",
    "",
    "<b>/derogacionespendientes</b>",
    "Vuelve a mandar las relaciones de derogación/modificación detectadas por IA que aún no confirmaste o rechazaste.",
    "",
    "<b>/normapdf document_key</b>",
    "Instrucciones para subir el PDF de una norma puntual (adjunta el PDF con ese mismo comando como caption).",
    "",
    "<b>/normarevisar document_key</b>",
    "Te mando un reporte HTML, el PDF original y una plantilla .txt para corregir el texto. Reenvía la plantilla editada a este chat para actualizar Supabase, sin entrar a la base de datos.",
    "",
    "<b>/tablasrevisar</b>",
    "Lista las normas con tablas cuya correspondencia fila-columna nadie confirmó contra el PDF (texto legible, pero tabla sin verificar).",
    "",
    "<b>/tablarevisar document_key</b>",
    "Igual que /normarevisar pero para tablas: te mando el PDF y la tabla en Markdown extraída. Si está bien, reenvía el archivo sin cambios; si está mal, corrígela y reenvíala.",
    "",
    "<b>/normaestado [document_key]</b>",
    "Reporte de fidelidad: calidad de texto, tablas, OCR, fórmulas y revisión manual. Sin argumento, resume toda la base; con document_key, el detalle de esa norma con un veredicto (confiable / usar con precaución / necesita revisión).",
    "",
    "<b>/reportenormas</b>",
    "Reporte maestro en HTML con TODAS las normas: PDF subido o no, páginas procesadas, calidad, tablas/fórmulas/gráficos pendientes, ordenado de más a menos urgente. Ábrelo en un navegador, no en un editor de texto.",
    "",
    "<b>/actualizarcomandos</b>",
    "Refresca el menú \"/\" nativo de Telegram (se actualiza solo en cada deploy, pero puedes forzarlo aquí).",
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

// /consulta busca por relevancia de texto (buscar_paginas_texto), no por
// fecha/orden/conteo: para preguntas tipo "cual es la ultima alerta",
// "que salio esta semana" o "cuantas alertas hay este mes" esa busqueda
// devuelve la pagina cuyo contenido calza mejor con esas palabras sueltas,
// no el dato real, y la IA narra ese resultado como si lo fuera. Estas
// preguntas se detectan aqui (por texto o por voz, ya que la voz reusa este
// mismo camino) y se resuelven con las mismas consultas ordenadas/contadas
// que usan /ultimas, /hoy, /semana y /mes, en vez de pasar por la IA.
function normalizarTexto(texto: string): string {
  return texto
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function ambitoTemporalAlertas(pregunta: string): "hoy" | "semana" | "mes" | null {
  const texto = normalizarTexto(pregunta);

  if (/\bhoy\b/.test(texto)) return "hoy";
  if (/\b(esta semana|semana)\b/.test(texto)) return "semana";
  if (/\b(este mes|del mes|\bmes\b)\b/.test(texto)) return "mes";

  return null;
}

function esConsultaDeUltimasAlertas(pregunta: string): boolean {
  const texto = normalizarTexto(pregunta);

  const mencionaAlertas = /alerta/.test(texto);
  const pideRecencia = /\b(ultima|ultimas|ultimo|ultimos|reciente|recientes|nueva|nuevas|nuevo|nuevos)\b/.test(texto);

  return mencionaAlertas && pideRecencia;
}

// Si la pregunta usa singular ("la ultima alerta", "una alerta reciente") se
// entiende que pide solo 1, no el listado completo de 5 que usa /ultimas;
// si menciona un numero explicito ("las 3 ultimas") se respeta ese numero.
function limiteAlertasSolicitado(pregunta: string): number {
  const texto = normalizarTexto(pregunta);

  const numeroExplicito = texto.match(/\b([1-9]|10)\b/);
  if (numeroExplicito) {
    return parseInt(numeroExplicito[1], 10);
  }

  const esPlural = /\b(ultimas|ultimos|recientes|nuevas|nuevos)\b/.test(texto);
  if (esPlural) return 5;

  const esSingular = /\b(ultima|ultimo|reciente|nueva|nuevo)\b/.test(texto);
  if (esSingular) return 1;

  return 5;
}

// Solo intercepta "cuantas alertas ..." cuando ademas trae un ambito de
// fecha explicito (hoy/semana/mes): sin eso, "cuantas alertas hay sobre
// tal medicamento" es una pregunta de contenido real que si necesita la
// busqueda semantica, no un conteo.
function esConsultaDeConteoAlertas(pregunta: string): boolean {
  const texto = normalizarTexto(pregunta);

  const preguntaCuantas = /\bcuant[oa]s?\b/.test(texto);
  const mencionaAlertas = /alerta/.test(texto);

  return preguntaCuantas && mencionaAlertas && ambitoTemporalAlertas(pregunta) !== null;
}

const UMBRAL_CONTEXTO_BAJA_CALIDAD = 0.5;
const UMBRAL_CONTEXTO_MEDIA_CALIDAD = 0.85;

/** Advertencias de confiabilidad para un bloque de contexto, a partir de las
 * senales que ya calcula la extraccion (quality_score, has_tables,
 * posible_formula, revisado_manual) pero que hasta ahora se quedaban solo en
 * el flujo de revision manual de admin y nunca llegaban a la IA que responde
 * /consulta. Sin esto, la IA citaba paginas OCR de baja confianza o tablas
 * aplanadas a texto corrido con la misma seguridad que contenido verificado. */
function advertenciasDelBloque(chunk: any): string[] {
  // La vigencia es independiente de si la transcripcion fue revisada: una
  // norma derogada sigue derogada aunque su OCR ya este verificado, asi que
  // esta advertencia va ANTES del corte por revisado_manual de abajo.
  const advertencias: string[] = [];

  if (chunk.estado_vigencia && chunk.estado_vigencia !== "vigente") {
    const etiqueta = chunk.estado_vigencia === "modificada" ? "MODIFICADA" : "DEROGADA / SIN EFECTO";
    advertencias.push(
      `⚠️ IMPORTANTE: esta norma fue marcada como ${etiqueta} por otra norma posterior. ` +
        "No la presentes como norma vigente: dilo explícitamente en tu respuesta.",
    );
  }

  if (chunk.revisado_manual) return advertencias;

  if (chunk.quality_score != null && chunk.quality_score < UMBRAL_CONTEXTO_BAJA_CALIDAD) {
    advertencias.push(
      "transcripcion de BAJA confiabilidad (posible error de OCR/lectura), no verificada por un humano",
    );
  } else if (chunk.quality_score != null && chunk.quality_score < UMBRAL_CONTEXTO_MEDIA_CALIDAD) {
    advertencias.push("transcripcion de confiabilidad media, no verificada por un humano");
  }

  if (chunk.has_tables) {
    advertencias.push(
      "esta pagina contiene una tabla; el texto de abajo esta aplanado y puede no reflejar bien la correspondencia fila-columna",
    );
  }

  if (chunk.posible_formula) {
    advertencias.push(
      "esta pagina puede contener una formula o notacion tecnica que la transcripcion no reconstruye con fidelidad",
    );
  }

  return advertencias;
}

function buildConsultaContext(chunks: any[]) {
  return chunks
    .map((chunk) => {
      const bloque = [
        `[Documento ${chunk.document_key} - ${chunk.title} - ${chunk.published_date} - pagina ${chunk.page_number}]`,
      ];

      const advertencias = advertenciasDelBloque(chunk);
      if (advertencias.length) {
        bloque.push(`ADVERTENCIA DE CONFIABILIDAD: ${advertencias.join("; ")}.`);
      }

      bloque.push(chunk.text_content);
      bloque.push(`Link oficial: ${chunk.detail_url}`);
      return bloque.join("\n");
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

/** Descarga cualquier archivo de Telegram (voz, documento) como bytes crudos. */
async function descargarArchivoTelegram(fileId: string): Promise<Uint8Array> {
  const fileInfoResponse = await fetch(`${TELEGRAM_API}/getFile?file_id=${fileId}`);
  const fileInfo = await fileInfoResponse.json();

  if (!fileInfo.ok) {
    throw new Error(`No se pudo obtener el archivo: ${JSON.stringify(fileInfo)}`);
  }

  const filePath = fileInfo.result.file_path as string;
  const fileResponse = await fetch(`https://api.telegram.org/file/bot${TELEGRAM_BOT_TOKEN}/${filePath}`);

  if (!fileResponse.ok) {
    throw new Error(`No se pudo descargar el archivo (status ${fileResponse.status}).`);
  }

  return new Uint8Array(await fileResponse.arrayBuffer());
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

  const audioBuffer = await descargarArchivoTelegram(fileId);
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

  if (trimmed === "/normasrevisar") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { data: paginas, error } = await supabase
      .from("digemid_norma_paginas")
      .select("norma_id, digemid_normas(document_key, titulo)")
      .lt("quality_score", UMBRAL_BAJA_CALIDAD_NORMA)
      .eq("revisado_manual", false);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al consultar: ${escapeHtml(error.message)}`);
    }

    if (!paginas || !paginas.length) {
      return await sendMessage(chatId, "✅ No hay páginas de baja confiabilidad pendientes de revisión.");
    }

    const porNorma = new Map<string, { titulo: string; count: number }>();
    for (const p of paginas as any[]) {
      const key = p.digemid_normas?.document_key ?? "?";
      const previo = porNorma.get(key);
      porNorma.set(key, {
        titulo: p.digemid_normas?.titulo ?? "",
        count: (previo?.count ?? 0) + 1,
      });
    }

    const lineas = ["📋 <b>Normas con páginas de baja confiabilidad</b>", "", "Toca una para revisarla:"];
    const botones = [...porNorma.entries()].map(([documentKey, info]) => [
      { text: `${documentKey} — ${info.count} página(s)`, callback_data: `normarevisar:${documentKey}` },
    ]);

    return await sendMessage(chatId, lineas.join("\n"), { inline_keyboard: botones });
  }

  if (trimmed.startsWith("/normarevisar")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const documentKey = trimmed.replace("/normarevisar", "").trim();

    if (!documentKey) {
      return await sendMessage(
        chatId,
        "Escribe el document_key de la norma.\n\nEjemplo:\n<code>/normarevisar RM-100-2024</code>\n\nUsa <code>/normasrevisar</code> para ver cuáles tienen páginas pendientes.",
      );
    }

    return await enviarRevisionNorma(chatId, documentKey);
  }

  if (trimmed === "/tablasrevisar") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { data: paginas, error } = await supabase
      .from("digemid_norma_paginas")
      .select("norma_id, digemid_normas(document_key, titulo)")
      .eq("has_tables", true)
      .eq("tabla_verificada", false);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al consultar: ${escapeHtml(error.message)}`);
    }

    if (!paginas || !paginas.length) {
      return await sendMessage(chatId, "✅ No hay tablas pendientes de verificación.");
    }

    const porNorma = new Map<string, { titulo: string; count: number }>();
    for (const p of paginas as any[]) {
      const key = p.digemid_normas?.document_key ?? "?";
      const previo = porNorma.get(key);
      porNorma.set(key, {
        titulo: p.digemid_normas?.titulo ?? "",
        count: (previo?.count ?? 0) + 1,
      });
    }

    const lineas = [
      "📋 <b>Normas con tablas sin verificar</b>",
      "",
      "El texto ya es legible, pero nadie confirmó que las columnas de la tabla (exportador/importador, montos, plazos, etc.) quedaron bien asignadas. Toca una para revisarla:",
    ];
    const botones = [...porNorma.entries()].map(([documentKey, info]) => [
      { text: `${documentKey} — ${info.count} página(s)`, callback_data: `tablarevisar:${documentKey}` },
    ]);

    return await sendMessage(chatId, lineas.join("\n"), { inline_keyboard: botones });
  }

  if (trimmed.startsWith("/tablarevisar")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const documentKey = trimmed.replace("/tablarevisar", "").trim();

    if (!documentKey) {
      return await sendMessage(
        chatId,
        "Escribe el document_key de la norma.\n\nEjemplo:\n<code>/tablarevisar DS-13-2022</code>\n\nUsa <code>/tablasrevisar</code> para ver cuáles tienen tablas pendientes.",
      );
    }

    return await enviarRevisionTablas(chatId, documentKey);
  }

  if (trimmed === "/normassinpdf") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { data: normas, error } = await supabase
      .from("digemid_normas")
      .select("document_key, titulo, process_status, pdf_url")
      .or("pdf_url.is.null,pdf_url.eq.,process_status.eq.pdf_download_error,process_status.eq.text_extraction_error")
      .order("anio", { ascending: false })
      .limit(30);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al consultar: ${escapeHtml(error.message)}`);
    }

    if (!normas || !normas.length) {
      return await sendMessage(chatId, "✅ No hay normas pendientes de PDF en este momento.");
    }

    const lineas = [
      "📄 <b>Normas sin PDF confirmado</b>",
      "",
      "Toca una para ver cómo subir su PDF manualmente (una por una, sin riesgo de mezclarlas):",
    ];
    const botones = normas.map((n: any) => [
      {
        text: `${n.document_key}${n.process_status ? ` (${n.process_status})` : " (sin PDF)"}`,
        callback_data: `normapdf:${n.document_key}`,
      },
    ]);

    return await sendMessage(chatId, lineas.join("\n"), { inline_keyboard: botones });
  }

  if (trimmed === "/derogacionespendientes") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const { data: relaciones, error } = await supabase
      .from("digemid_norma_relaciones")
      .select("id, norma_origen_document_key, tipo_relacion, descripcion_afectada, fragmento_fuente")
      .eq("estado", "pendiente")
      .order("created_at", { ascending: true })
      .limit(15);

    if (error) {
      return await sendMessage(chatId, `⚠️ Error al consultar: ${escapeHtml(error.message)}`);
    }

    if (!relaciones || !relaciones.length) {
      return await sendMessage(chatId, "✅ No hay relaciones de derogación/modificación pendientes de confirmar.");
    }

    const verbos: Record<string, string> = {
      deroga: "derogaría",
      deja_sin_efecto: "dejaría sin efecto",
      modifica: "modificaría",
    };

    for (const relacion of relaciones) {
      const verbo = verbos[relacion.tipo_relacion] ?? relacion.tipo_relacion;
      const texto =
        `⚠️ <b>Posible derogación/modificación</b>\n\n` +
        `<b>${escapeHtml(relacion.norma_origen_document_key)}</b> ${verbo} a:\n` +
        `<b>${escapeHtml(relacion.descripcion_afectada)}</b>` +
        (relacion.fragmento_fuente
          ? `\n\nFragmento: <i>"${escapeHtml(relacion.fragmento_fuente)}"</i>`
          : "") +
        "\n\n¿Confirmas esta relación?";

      await sendMessage(chatId, texto, {
        inline_keyboard: [[
          { text: "✅ Confirmar", callback_data: `derog:confirmar:${relacion.id}` },
          { text: "❌ Rechazar", callback_data: `derog:rechazar:${relacion.id}` },
        ]],
      });
    }

    return new Response("OK", { status: 200 });
  }

  if (trimmed.startsWith("/normapdf")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const documentKey = trimmed.replace("/normapdf", "").trim();

    if (!documentKey) {
      return await sendMessage(
        chatId,
        "Escribe el document_key de la norma.\n\nEjemplo:\n<code>/normapdf RM-100-2024</code>\n\n" +
          "Usa <code>/normassinpdf</code> para ver cuáles no tienen PDF.",
      );
    }

    return await enviarInstruccionNormaPdf(chatId, documentKey);
  }

  if (trimmed === "/normaestado" || trimmed.startsWith("/normaestado ")) {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    const documentKey = trimmed.replace("/normaestado", "").trim();

    try {
      const reporte = documentKey
        ? await construirReporteEstadoNorma(documentKey)
        : await construirReporteEstadoGlobal();
      return await sendMessage(chatId, reporte);
    } catch (error) {
      return await sendMessage(chatId, `⚠️ Error al calcular el estado: ${escapeHtml(String(error))}`);
    }
  }

  if (trimmed === "/reportenormas") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    try {
      const html = await construirReporteMaestroNormas();
      await enviarDocumentoTexto(
        chatId,
        html,
        `reporte_normas_${getLimaDateParts().isoDate}.html`,
        "text/html",
        "📋 Reporte maestro de todas las normas — ábrelo en un navegador para verlo como tabla.",
      );
      return new Response("OK", { status: 200 });
    } catch (error) {
      return await sendMessage(chatId, `⚠️ Error al generar el reporte: ${escapeHtml(String(error))}`);
    }
  }

  if (trimmed === "/actualizarcomandos") {
    if (!isAdmin(chatId)) {
      return await sendMessage(chatId, "⛔ Comando solo disponible para administradores.");
    }

    try {
      await actualizarComandosTelegram();
      return await sendMessage(
        chatId,
        "✅ Menú \"/\" actualizado. Si Telegram no lo refresca solo, cierra y vuelve a abrir el chat (o reinicia la app).",
      );
    } catch (error) {
      return await sendMessage(chatId, `⚠️ Error al actualizar el menú: ${escapeHtml(String(error))}`);
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

    if (esConsultaDeConteoAlertas(question)) {
      const ambito = ambitoTemporalAlertas(question)!;
      const { total, etiqueta } = ambito === "hoy"
        ? { total: (await getTodayAlerts()).length, etiqueta: "hoy" }
        : ambito === "semana"
        ? { total: (await getAlertasSemana(10)).total, etiqueta: "esta semana" }
        : { total: (await getMonthAlerts()).length, etiqueta: "este mes" };

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: total,
        status: "ok_redirigido_conteo",
      });

      return await sendMessage(
        chatId,
        `🔢 Hay <b>${total}</b> ${total === 1 ? "alerta" : "alertas"} registradas ${etiqueta}.`,
        alertasMenu(),
      );
    }

    const ambito = ambitoTemporalAlertas(question);

    if (ambito === "hoy") {
      const rows = await getTodayAlerts();

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: rows.length,
        status: "ok_redirigido_hoy",
      });

      await sendMessage(chatId, formatAlertList("📅 <b>Alertas DIGEMID de hoy</b>", rows), alertasMenu());
      return await enviarPdfsAlertas(chatId, rows);
    }

    if (ambito === "semana") {
      const { rows, total } = await getAlertasSemana(10);

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: rows.length,
        status: "ok_redirigido_semana",
      });

      await sendMessage(chatId, formatWeekAlertList(rows, total, 10), alertasMenu());
      return await enviarPdfsAlertas(chatId, rows);
    }

    if (ambito === "mes") {
      const rows = await getMonthAlerts();

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: rows.length,
        status: "ok_redirigido_mes",
      });

      await sendMessage(chatId, formatAlertList("🗓️ <b>Alertas DIGEMID del mes</b>", rows), alertasMenu());
      return await enviarPdfsAlertas(chatId, rows);
    }

    if (esConsultaDeUltimasAlertas(question)) {
      const limite = limiteAlertasSolicitado(question);
      const rows = await getLatestAlerts(limite);
      const titulo = limite === 1 ? "🆕 <b>Última alerta DIGEMID</b>" : "🆕 <b>Últimas alertas DIGEMID</b>";

      await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: rows.length,
        status: "ok_redirigido_ultimas",
      });

      await sendMessage(
        chatId,
        formatAlertList(titulo, rows),
        alertasMenu(),
      );
      return await enviarPdfsAlertas(chatId, rows);
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

      const consultaId = await logConsulta({
        chatId,
        userId,
        command: "/consulta",
        queryText: question,
        resultCount: sources.length,
        status: "ok",
        raw: { answerText: answer },
      });

      const filasBotones: Array<Array<{ text: string; url?: string; callback_data?: string }>> = sources
        .slice(0, 3)
        .map((source) => [
          { text: `📄 ${source.documentKey}${source.page ? " (pág. " + source.page + ")" : ""}`, url: source.url },
        ]);

      const puedeEscucharAudio = isAdmin(chatId) || nivel !== "gratis";
      if (consultaId && puedeEscucharAudio) {
        filasBotones.push([{ text: "🔊 Escuchar respuesta", callback_data: `tts:${consultaId}` }]);
      }

      const sourceButtons = filasBotones.length ? { inline_keyboard: filasBotones } : undefined;

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

  if (data.startsWith("normarevisar:")) {
    if (!isAdmin(chatId)) {
      return;
    }

    const documentKey = data.slice("normarevisar:".length);
    return await enviarRevisionNorma(chatId, documentKey);
  }

  if (data.startsWith("tablarevisar:")) {
    if (!isAdmin(chatId)) {
      return;
    }

    const documentKey = data.slice("tablarevisar:".length);
    return await enviarRevisionTablas(chatId, documentKey);
  }

  if (data.startsWith("normapdf:")) {
    if (!isAdmin(chatId)) {
      return;
    }

    const documentKey = data.slice("normapdf:".length);
    return await enviarInstruccionNormaPdf(chatId, documentKey);
  }

  if (data.startsWith("derog:confirmar:") || data.startsWith("derog:rechazar:")) {
    if (!isAdmin(chatId)) {
      return;
    }

    const accion = data.startsWith("derog:confirmar:") ? "confirmar" : "rechazar";
    const relacionId = data.slice(`derog:${accion}:`.length);
    await resolverRelacionDerogacion(chatId, callback.message?.message_id, relacionId, accion);
    return;
  }

  if (data.startsWith("tts:")) {
    const consultaId = data.slice(4);

    try {
      const { data: row } = await supabase
        .from("digemid_bot_consultas")
        .select("raw")
        .eq("id", consultaId)
        .maybeSingle();

      const answerText = (row?.raw as Record<string, unknown> | null)?.answerText;

      if (!answerText || typeof answerText !== "string") {
        return await sendMessage(chatId, "⚠️ Ya no tengo guardado el texto de esa respuesta. Vuelve a hacer la consulta.");
      }

      await sendMessage(chatId, "🎙️ Generando audio...");
      const wavBytes = await generarAudioRespuesta(answerText);
      return await enviarAudioRespuesta(chatId, wavBytes, "🔊 Respuesta en audio");
    } catch (error) {
      console.error("TTS_ERROR:", error);
      return await sendMessage(chatId, "⚠️ No pude generar el audio en este momento. Intenta de nuevo en unos minutos.");
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

// Fire-and-forget: refresca el menu "/" de Telegram en cada arranque en frio
// de la funcion (cada deploy), sin bloquear ni poder tumbar el bot si falla.
actualizarComandosTelegram().catch((error) =>
  console.error("SET_MY_COMMANDS_ERROR:", error)
);

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
      if (!isAdmin(chatId)) {
        const nivelVoz = await getNivelUsuario(chatId);
        const limiteSegundos = NIVEL_LIMITE_VOZ_SEGUNDOS[nivelVoz] ?? NIVEL_LIMITE_VOZ_SEGUNDOS.gratis;
        const duracion = update.message.voice.duration ?? 0;

        if (duracion > limiteSegundos) {
          await sendMessage(
            chatId,
            `⚠️ Tu plan <b>${escapeHtml(nivelVoz)}</b> permite notas de voz de hasta ` +
              `<b>${limiteSegundos} segundos</b> (la tuya duró ${duracion}s). ` +
              "Intenta de nuevo más corto, o escribe tu pregunta con /consulta.",
          );
          return new Response("OK", { status: 200 });
        }
      }

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

    if (update.message?.document) {
      if (!isAdmin(chatId)) {
        return new Response("OK", { status: 200 });
      }

      const documentoRecibido = update.message.document;
      const esPdf = documentoRecibido.mime_type === "application/pdf" ||
        (documentoRecibido.file_name ?? "").toLowerCase().endsWith(".pdf");

      if (esPdf) {
        const caption = (update.message.caption ?? "").trim();
        const matchCaption = caption.match(/^\/normapdf\s+(\S+)/i);

        if (matchCaption) {
          return await manejarPdfManual(
            chatId,
            matchCaption[1].trim(),
            documentoRecibido.file_id,
            documentoRecibido.file_name ?? "",
          );
        }

        const { data: pendiente } = await supabase
          .from("digemid_normapdf_pendientes")
          .select("document_key, expira_en")
          .eq("chat_id", chatId)
          .maybeSingle();

        if (pendiente && new Date(pendiente.expira_en).getTime() > Date.now()) {
          await supabase.from("digemid_normapdf_pendientes").delete().eq("chat_id", chatId);
          return await manejarPdfManual(
            chatId,
            pendiente.document_key,
            documentoRecibido.file_id,
            documentoRecibido.file_name ?? "",
          );
        }

        await sendMessage(
          chatId,
          "⚠️ Para subir el PDF de una norma, primero escribe <code>/normapdf DOCUMENT_KEY</code> y después " +
            "mándame el archivo (sin nada más), o adjunta el PDF con <code>/normapdf DOCUMENT_KEY</code> como pie " +
            "de foto (caption).\n\n" +
            "Ejemplo: <code>/normapdf RM-100-2024</code>\n\n" +
            "Usa <code>/normassinpdf</code> para ver qué normas lo necesitan.",
        );
        return new Response("OK", { status: 200 });
      }

      try {
        const bytes = await descargarArchivoTelegram(update.message.document.file_id);
        const contenido = new TextDecoder("utf-8").decode(bytes);
        const resultado = await aplicarRevisionManualNorma(contenido);

        if (!resultado.ok) {
          await sendMessage(chatId, `⚠️ ${resultado.mensaje}`);
        } else {
          await sendMessage(
            chatId,
            `✅ Norma <b>${escapeHtml(resultado.documentKey ?? "")}</b> actualizada: ` +
              `<b>${resultado.paginasActualizadas}</b> página(s) corregida(s).`,
          );
        }
      } catch (error) {
        console.error("DOCUMENTO_REVISION_ERROR:", error);
        await sendMessage(
          chatId,
          "⚠️ No pude procesar ese archivo. Verifica que sea la plantilla generada por /normarevisar o /tablarevisar, sin modificar los encabezados de página.",
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
