/** Formato de salida para WhatsApp.
 *
 * Telegram usa HTML (<b>texto</b>); WhatsApp usa su propio marcado
 * (*negrita*, _cursiva_, ```monoespaciado```) y muestra cualquier etiqueta
 * HTML como texto literal. La logica regulatoria (que datos se muestran y
 * con que trazabilidad) NO vive aqui: este modulo solo decide como se ve.
 */

import { formatCreatedAtSimple } from "../_shared/fechas-lima.ts";
import { tipoNormativa } from "../_shared/digemid-datos.ts";

export function negrita(texto: string): string {
  return `*${texto}*`;
}

/** Convierte a formato WhatsApp un texto que viene con marcado de Telegram.
 * Se usa para la respuesta de la IA: aunque el prompt de WhatsApp ya pide
 * asteriscos, un modelo puede devolver <b> o **negrita** igual, y esas
 * etiquetas no deben llegar visibles al usuario (§15). */
export function aFormatoWhatsApp(texto: string): string {
  return (texto ?? "")
    .replace(/<\s*br\s*\/?\s*>/gi, "\n")
    .replace(/<\s*b\s*>([\s\S]*?)<\s*\/\s*b\s*>/gi, "*$1*")
    .replace(/<\s*strong\s*>([\s\S]*?)<\s*\/\s*strong\s*>/gi, "*$1*")
    .replace(/<\s*i\s*>([\s\S]*?)<\s*\/\s*i\s*>/gi, "_$1_")
    .replace(/<\s*em\s*>([\s\S]*?)<\s*\/\s*em\s*>/gi, "_$1_")
    .replace(/<\s*code\s*>([\s\S]*?)<\s*\/\s*code\s*>/gi, "`$1`")
    .replace(/<\s*pre\s*>([\s\S]*?)<\s*\/\s*pre\s*>/gi, "`$1`")
    // Cualquier otra etiqueta se elimina en vez de mostrarse literal.
    .replace(/<\/?[a-z][^>]*>/gi, "")
    // Markdown de doble asterisco -> negrita de WhatsApp (un asterisco).
    .replace(/\*\*(.+?)\*\*/g, "*$1*")
    // Entidades HTML que pudieran venir escapadas desde el lado de Telegram.
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .trim();
}

export const TEXTO_MENU = [
  negrita("DIGEMID RegAlert"),
  "",
  "¿Qué deseas consultar? Responde con el número o el nombre de la opción:",
  "",
  "1️⃣ 🚨 Últimas alertas",
  "2️⃣ 📅 Alertas de hoy",
  "3️⃣ 📚 Normativa",
  "4️⃣ 🔎 Buscar",
  "5️⃣ 🤖 Consulta con IA",
  "6️⃣ 👤 Mi perfil",
  "7️⃣ ℹ️ Ayuda",
].join("\n");

export const TEXTO_AYUDA = [
  negrita("ℹ️ Cómo usar RegAlert por WhatsApp"),
  "",
  "Puedes escribir el comando con o sin barra (" + "`/ultimas`" + " o " + "`ultimas`" + ").",
  "",
  negrita("Alertas DIGEMID"),
  "• `ultimas` — últimas alertas registradas",
  "• `hoy` — alertas publicadas hoy",
  "• `semana` — alertas publicadas esta semana",
  "• `mes` — alertas publicadas este mes",
  "• `recientes` — alertas registradas en los últimos 7 días",
  "• `detalle 75-2026` — ver una alerta por su número",
  "",
  negrita("Normativa"),
  "• `normas` — últimas leyes, decretos y resoluciones",
  "",
  negrita("Búsqueda y consulta"),
  "• `buscar paracetamol` — busca alertas por palabra clave",
  "• `consulta qué establece la Ley 29459` — pregunta con IA, citando la norma fuente",
  "",
  negrita("Tu cuenta"),
  "• `miperfil` — tu plan y consultas disponibles",
  "",
  "📌 Las respuestas son informativas y citan el documento fuente. No reemplazan al " +
  "Químico Farmacéutico Director Técnico ni a DIGEMID.",
].join("\n");

export const TEXTO_NO_RECONOCIDO = [
  "No reconocí esa opción.",
  "",
  TEXTO_MENU,
].join("\n");

export const TEXTO_PRUEBA_VENCIDA = [
  negrita("⏰ Tu prueba gratuita de RegAlert DIGEMID terminó"),
  "",
  "Para seguir consultando alertas, normativa y hacer preguntas con IA, activa un plan.",
  "",
  "Escríbenos a través de nuestros canales de contacto para coordinar tu suscripción.",
].join("\n");

export const TEXTO_SOLO_TEXTO = [
  "Por ahora solo puedo leer mensajes de *texto*.",
  "",
  "Escríbeme tu consulta en texto, por ejemplo:",
  "`buscar paracetamol`",
  "",
  "Escribe `menu` para ver todas las opciones.",
].join("\n");

// deno-lint-ignore no-explicit-any
export function formatAlertListWhatsApp(titulo: string, rows: any[]): string {
  if (!rows.length) {
    return `${negrita(titulo)}\n\n📭 No encontré alertas para esta consulta.`;
  }

  const lineas = [negrita(titulo), ""];

  for (const row of rows) {
    lineas.push(`🚨 ${negrita(String(row.alert_number ?? row.document_key ?? ""))}`);
    if (row.alert_title ?? row.title) {
      lineas.push(`📌 ${row.alert_title ?? row.title}`);
    }
    lineas.push(`📅 ${row.published_date_display ?? row.published_date ?? "Sin fecha"}`);
    if (row.detail_url) lineas.push(`🔗 ${row.detail_url}`);
    lineas.push("");
  }

  lineas.push(`✅ Total mostrado: ${rows.length}`);
  return lineas.join("\n");
}

// deno-lint-ignore no-explicit-any
export function formatWeekAlertListWhatsApp(rows: any[], total: number): string {
  const titulo = "📅 Alertas DIGEMID de esta semana";

  if (!rows.length) {
    return [
      negrita(titulo),
      "",
      "No se encontraron alertas publicadas esta semana.",
      "",
      "Puedes probar con `ultimas`.",
    ].join("\n");
  }

  const lineas = [negrita(titulo), ""];

  rows.forEach((row, indice) => {
    lineas.push(`${indice + 1}. ${negrita(`Alerta DIGEMID N° ${row.document_key}`)}`);
    lineas.push(`Publicada: ${row.published_date_display ?? row.published_date ?? "Sin fecha"}`);
    if (row.title) lineas.push(`Título: ${row.title}`);
    if (row.detail_url) lineas.push(`Detalle: ${row.detail_url}`);
    lineas.push("");
  });

  lineas.push(
    total > rows.length
      ? `Mostrando ${rows.length} de ${total} alertas de esta semana.`
      : `Total: ${rows.length} ${rows.length === 1 ? "alerta" : "alertas"}.`,
  );

  return lineas.join("\n");
}

/** /recientes usa created_at (cuando RegAlert registro el documento) y lo
 * muestra SEPARADO de published_date (fecha oficial): son dos hechos
 * distintos y mezclarlos induce a error juridico. */
// deno-lint-ignore no-explicit-any
export function formatRecentAlertListWhatsApp(rows: any[]): string {
  const titulo = "🕒 Alertas registradas recientemente";

  if (!rows.length) {
    return [
      negrita(titulo),
      "",
      "No se encontraron alertas registradas en los últimos 7 días.",
      "",
      "Puedes probar con `ultimas` o `semana`.",
    ].join("\n");
  }

  const lineas = [negrita(titulo), ""];

  rows.forEach((row, indice) => {
    lineas.push(`${indice + 1}. ${negrita(`Alerta DIGEMID N° ${row.document_key}`)}`);
    lineas.push(`Fecha publicada: ${row.published_date_display ?? row.published_date ?? "Sin fecha"}`);
    lineas.push(`Registrada: ${formatCreatedAtSimple(row.created_at)}`);
    if (row.title) lineas.push(`Título: ${row.title}`);
    if (row.detail_url) lineas.push(`Detalle: ${row.detail_url}`);
    lineas.push("");
  });

  lineas.push(`Total: ${rows.length} ${rows.length === 1 ? "alerta encontrada." : "alertas encontradas."}`);
  return lineas.join("\n");
}

// deno-lint-ignore no-explicit-any
export function formatAlertDetailWhatsApp(row: any): string {
  const pdfUrl = row.drive_file_url || row.drive_download_url || row.pdf_source_url;

  const lineas = [
    negrita(`🚨 Alerta DIGEMID N.° ${row.alert_number}`),
    "",
    negrita("📌 Título:"),
    String(row.alert_title ?? "Sin título"),
    "",
    `📅 ${negrita("Publicación:")} ${row.published_date_display ?? row.published_date ?? "Sin fecha"}`,
    `📋 ${negrita("Estado:")} ${row.process_status ?? "Registrada"}`,
  ];

  if (row.detail_url) lineas.push("", `🔗 Ver alerta: ${row.detail_url}`);
  if (pdfUrl) lineas.push(`⬇️ PDF: ${pdfUrl}`);
  if (!pdfUrl) lineas.push("", "📎 PDF aún no registrado en el sistema.");

  return lineas.join("\n");
}

// deno-lint-ignore no-explicit-any
export function formatNormativaListWhatsApp(titulo: string, rows: any[]): string {
  if (!rows.length) {
    return `${negrita(titulo)}\n\n📭 No encontré normativa para esta consulta.`;
  }

  const lineas = [negrita(titulo), ""];

  for (const row of rows) {
    lineas.push(`📜 ${negrita(`${tipoNormativa(row)} ${row.document_key}`)}`);
    if (row.title) lineas.push(String(row.title));
    lineas.push(`📅 ${row.published_date_display ?? row.published_date ?? "Sin fecha"}`);
    if (row.detail_url) lineas.push(`🔗 ${row.detail_url}`);
    if (row.file_url) lineas.push(`📄 PDF: ${row.file_url}`);
    lineas.push("");
  }

  lineas.push(`✅ Total mostrado: ${rows.length}`);
  lineas.push(
    "ℹ️ Detección automática por título/metadata; el contenido aún no pasa por el proceso de verificación de fidelidad.",
  );

  return lineas.join("\n");
}

/** Respuesta de /consulta: el texto de la IA ya viene con las citas que
 * exige el prompt; aqui solo se adapta el marcado y se agregan los links
 * oficiales de las fuentes (que en Telegram van en botones). */
export function formatConsultaWhatsApp(
  answer: string,
  fuentes: { documentKey: string; url: string }[],
): string {
  const partes = [aFormatoWhatsApp(answer)];

  if (fuentes.length) {
    partes.push("");
    partes.push(negrita("🔗 Documentos:"));
    for (const fuente of fuentes) {
      partes.push(`• ${fuente.documentKey}: ${fuente.url}`);
    }
  }

  return partes.join("\n");
}
