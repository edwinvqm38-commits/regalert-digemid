/** Consulta IA (RAG) sobre la base documental de DIGEMID.
 *
 * NO es un segundo sistema RAG: usa la misma busqueda (RPC
 * buscar_paginas_texto), el mismo contexto por pagina, las mismas
 * advertencias de confiabilidad y los mismos proveedores (DeepSeek con
 * respaldo Gemini) que /consulta en Telegram. Lo unico que varia por canal
 * es el marcado de negrita del prompt, porque Telegram usa HTML (<b>) y
 * WhatsApp usa asteriscos (*).
 *
 * La trazabilidad juridica es parte del contrato de este modulo: el prompt
 * obliga a citar documento, fecha y pagina, y a decir explicitamente cuando
 * el contexto no alcanza, en vez de completar con conocimiento propio.
 */

import { getLatestNormativa, tipoNormativa } from "./digemid-datos.ts";

// deno-lint-ignore no-explicit-any
export type SupabaseLike = any;

export type EstiloNegrita = "html" | "whatsapp";

const UMBRAL_CONTEXTO_BAJA_CALIDAD = 0.5;
const UMBRAL_CONTEXTO_MEDIA_CALIDAD = 0.85;

function negritaPorEstilo(estilo: EstiloNegrita, texto: string): string {
  return estilo === "html" ? `<b>${texto}</b>` : `*${texto}*`;
}

function normalizarParaPatron(texto: string): string {
  return (texto ?? "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "");
}

/** Detecta preguntas sobre "cual es la ultima/mas reciente norma", que la
 * busqueda de texto NUNCA puede responder bien: el dato que se pide (que es
 * lo mas reciente) no esta escrito dentro de ningun documento, es un hecho
 * sobre el propio catalogo. Para esas se responde con datos ordenados por
 * fecha en vez de dejar que el modelo adivine o se rinda sin sustento. */
export function pareceConsultaSobreUltimaNorma(pregunta: string): boolean {
  const normalizada = normalizarParaPatron(pregunta);
  const patronRecienciaAntes = /(ultima|mas reciente|recien|nueva)s?\s+(norma|normativa|ley|decreto|resolucion|alerta)/;
  const patronRecienciaDespues = /(norma|normativa|ley|decreto|resolucion|alerta).{0,25}(ultima|mas reciente|recien|nueva)/;
  return patronRecienciaAntes.test(normalizada) || patronRecienciaDespues.test(normalizada);
}

// deno-lint-ignore no-explicit-any
function formatUltimaNormativaComoRespuesta(rows: any[], estilo: EstiloNegrita): string {
  const b = (t: string) => negritaPorEstilo(estilo, t);

  if (!rows.length) {
    return "No encontré normativa registrada en la base de datos.";
  }

  const lineas = [`${b("Últimas normas registradas por RegAlert")}:`, ""];
  rows.forEach((row, indice) => {
    lineas.push(
      `${indice + 1}. ${b(`${tipoNormativa(row)} ${row.document_key}`)} — ` +
        `${row.published_date_display ?? row.published_date ?? "sin fecha"}`,
    );
    if (row.title) lineas.push(String(row.title));
  });
  lineas.push(
    "",
    "Esto es lo más reciente que RegAlert tiene registrado; no equivale necesariamente al instante " +
      "en que DIGEMID lo subió a su propia página web.",
  );

  return lineas.join("\n");
}

/** Prefijos de tipo de norma reconocidos en el texto libre de una pregunta,
 * hacia el mismo formato de document_key que usa digemid_normas ("RM-727-2025").
 * Las claves de mas de una palabra deben probarse antes que sus abreviaturas
 * (extraerReferenciaNormativa las ordena por longitud) para que "resolucion
 * ministerial" no quede capturado a medias por una entrada mas corta. */
const PREFIJOS_NORMA: Record<string, string> = {
  "ley": "LEY",
  "decreto supremo": "DS",
  "decreto legislativo": "DL",
  "decreto de urgencia": "DU",
  "resolucion ministerial": "RM",
  "resolucion directoral": "RD",
  "resolucion suprema": "RS",
  "ds": "DS",
  "dl": "DL",
  "du": "DU",
  "rm": "RM",
  "rd": "RD",
  "rs": "RS",
};

export type ReferenciaNormativa = { documentKey: string };

/** Detecta que el usuario esta preguntando por UNA norma puntual, citada por
 * su tipo + numero + año ("Resolución Ministerial 727-2025/MINSA", "DS
 * 020-2024", "Ley 29459"). Para estas preguntas, buscar por texto libre es
 * poco confiable (puede traer una norma distinta que "suena" parecida); es
 * mejor ir directo al documento por su identificador exacto. */
export function extraerReferenciaNormativa(pregunta: string): ReferenciaNormativa | null {
  const texto = normalizarParaPatron(pregunta);
  const claves = Object.keys(PREFIJOS_NORMA).sort((a, b) => b.length - a.length);

  for (const clave of claves) {
    const escapado = clave.replace(/ /g, "\\s+");
    const regex = new RegExp(`\\b${escapado}\\b\\s*(?:n[°ºo.]?\\s*)?(\\d{1,4})[\\s\\-/]+(\\d{4})\\b`);
    const match = texto.match(regex);
    if (match) {
      return { documentKey: `${PREFIJOS_NORMA[clave]}-${match[1]}-${match[2]}` };
    }
  }

  return null;
}

/** Tope de paginas que se envian al modelo para UNA norma puntual: evita que
 * una ley larga (varias decenas de paginas) dispare un costo/latencia
 * absurdos en una sola consulta. Si la norma tiene mas, se avisa en vez de
 * cortar en silencio. */
const MAX_PAGINAS_NORMA_PUNTUAL = 25;

/** Trae una norma puntual por su document_key exacto, con su contenido
 * (digemid_norma_paginas) y sus relaciones normativas conocidas
 * (digemid_norma_relaciones, en ambos sentidos: a que otras normas afecta, y
 * que otras normas la afectaron a ella) para que el modelo pueda responder
 * tambien "que mas debo tener en cuenta" y no solo "que dice este articulo". */
async function getNormaConContenido(
  supabase: SupabaseLike,
  documentKey: string,
  // deno-lint-ignore no-explicit-any
): Promise<{ norma: any; paginas: any[]; relaciones: any[] } | null> {
  const { data: norma, error: errorNorma } = await supabase
    .from("digemid_normas")
    .select("id, document_key, titulo, fecha_publicacion, source_url, pdf_url, estado_vigencia")
    .eq("document_key", documentKey)
    .maybeSingle();

  if (errorNorma) throw errorNorma;
  if (!norma) return null;

  const { data: paginas, error: errorPaginas } = await supabase
    .from("digemid_norma_paginas")
    .select("page_number, text_normalized, text_raw, quality_score, revisado_manual, has_tables, posible_formula")
    .eq("norma_id", norma.id)
    .order("page_number")
    .limit(MAX_PAGINAS_NORMA_PUNTUAL);

  if (errorPaginas) throw errorPaginas;

  const { data: relaciones, error: errorRelaciones } = await supabase
    .from("digemid_norma_relaciones")
    .select(
      "tipo_relacion, tipo_norma_afectada, numero_afectada, anio_afectada, descripcion_afectada, estado, norma_origen_id, norma_afectada_id, norma_origen_document_key",
    )
    .or(`norma_origen_id.eq.${norma.id},norma_afectada_id.eq.${norma.id}`);

  if (errorRelaciones) throw errorRelaciones;

  return { norma, paginas: paginas ?? [], relaciones: relaciones ?? [] };
}

// deno-lint-ignore no-explicit-any
function paginaComoChunk(pagina: any, norma: any) {
  return {
    document_key: norma.document_key,
    title: norma.titulo,
    published_date: norma.fecha_publicacion,
    page_number: pagina.page_number,
    text_content: pagina.text_normalized ?? pagina.text_raw ?? "",
    detail_url: norma.source_url ?? norma.pdf_url ?? "",
    quality_score: pagina.quality_score,
    revisado_manual: pagina.revisado_manual,
    has_tables: pagina.has_tables,
    posible_formula: pagina.posible_formula,
    estado_vigencia: norma.estado_vigencia,
  };
}

/** Convierte las relaciones normativas en un bloque de texto adicional para
 * el modelo: deroga/modifica/prorroga detectados automaticamente, en las dos
 * direcciones (esta norma afecta a otra vs. otra norma afecto a esta). Nunca
 * se presenta como confirmado por un humano — eso lo dice `estado`. */
// deno-lint-ignore no-explicit-any
function formatRelacionesComoContexto(relaciones: any[], norma: any): string {
  if (!relaciones.length) return "";

  const lineas = [
    "",
    `[Relaciones normativas de ${norma.document_key} detectadas automaticamente, estado de verificacion segun cada una]`,
  ];

  for (const r of relaciones) {
    if (r.norma_origen_id === norma.id) {
      lineas.push(
        `- ${norma.document_key} ${r.tipo_relacion} a ${r.tipo_norma_afectada ?? "norma"} ` +
          `${r.numero_afectada ?? "?"}-${r.anio_afectada ?? "?"} (${r.descripcion_afectada ?? "sin descripción"}). ` +
          `Estado: ${r.estado}.`,
      );
    } else {
      lineas.push(
        `- ${norma.document_key} fue afectada (${r.tipo_relacion}) por ${r.norma_origen_document_key ?? "otra norma"}. ` +
          `Estado: ${r.estado}.`,
      );
    }
  }

  return lineas.join("\n");
}

/** Version legible para el USUARIO (no para el modelo) de las relaciones
 * normativas de una norma: mismo contenido que formatRelacionesComoContexto
 * pero en negrita segun el estilo del canal, para que el usuario tambien
 * vea "a que otras normas afecta o lo afectaron" sin tener que abrir el
 * link ni pedirlo explicitamente. */
// deno-lint-ignore no-explicit-any
function formatRelacionesParaUsuario(relaciones: any[], norma: any, estilo: EstiloNegrita): string {
  if (!relaciones.length) return "";

  const b = (t: string) => negritaPorEstilo(estilo, t);
  const lineas = [
    "",
    `${b("Otras normas a tener en cuenta")} (relación detectada automáticamente, verifica antes de asumirla como definitiva):`,
  ];

  for (const r of relaciones) {
    const etiquetaEstado = r.estado === "verificada" ? "verificada" : "sin verificar por un humano";
    if (r.norma_origen_id === norma.id) {
      lineas.push(
        `- ${b(norma.document_key)} ${r.tipo_relacion} a ${r.tipo_norma_afectada ?? "norma"} ` +
          `${r.numero_afectada ?? "?"}-${r.anio_afectada ?? "?"}` +
          (r.descripcion_afectada ? ` (${r.descripcion_afectada})` : "") +
          ` — ${etiquetaEstado}.`,
      );
    } else {
      lineas.push(
        `- ${b(norma.document_key)} fue afectada (${r.tipo_relacion}) por ` +
          `${b(r.norma_origen_document_key ?? "otra norma")} — ${etiquetaEstado}.`,
      );
    }
  }

  return lineas.join("\n");
}

/** Responde sobre UNA norma puntual citada por numero exacto, usando su
 * contenido real (no busqueda difusa) y sus relaciones conocidas. Devuelve
 * null si no se encontro esa norma exacta en digemid_normas: quien llama
 * decide si sigue con la busqueda de texto como respaldo. */
async function responderNormaPuntual(
  supabase: SupabaseLike,
  documentKey: string,
  question: string,
  config: ConfigConsultaIa,
): Promise<RespuestaConsulta | null> {
  const resultado = await getNormaConContenido(supabase, documentKey);
  if (!resultado) return null;

  const { norma, paginas, relaciones } = resultado;

  if (!paginas.length) {
    return {
      answer: `Encontré ${negritaPorEstilo(config.estiloNegrita, norma.document_key)} en la base, pero su ` +
        "contenido todavía no fue extraído/revisado, así que no puedo interpretarla todavía. " +
        "Verifica directamente con el PDF oficial.",
      sources: [{ documentKey: norma.document_key, url: norma.source_url ?? norma.pdf_url ?? "" }],
      sinEvidencia: true,
    };
  }

  const chunks = paginas.map((pagina) => paginaComoChunk(pagina, norma));
  const systemPrompt = construirSystemPrompt(config.estiloNegrita);
  const userContent = `Contexto:\n\n${buildConsultaContext(chunks)}${formatRelacionesComoContexto(relaciones, norma)}` +
    `\n\nPregunta: ${question}`;
  const sources = consultaSources(chunks);
  const relacionesTexto = formatRelacionesParaUsuario(relaciones, norma, config.estiloNegrita);

  // Si preguntan puntualmente por una tabla/cuadro/grafico, el texto ya
  // extraido (aplanado por OCR/pdfplumber) es justo el menos confiable para
  // eso: se complementa con una lectura del PDF real via un modelo con
  // vision. Es un extra opcional (nunca lanza, nunca bloquea la respuesta
  // de texto normal si falla o no hay proveedor configurado).
  const necesitaVision = preguntaPideVerificacionVisual(question);
  const verificacionVisual = necesitaVision ? await intentarVerificacionVisual(norma, question, config) : null;
  const b = (t: string) => negritaPorEstilo(config.estiloNegrita, t);
  const bloqueVisual = verificacionVisual
    ? `\n\n${b("🔎 Verificación visual del PDF (tabla/gráfico)")}:\n${verificacionVisual}`
    : "";

  if (config.deepseekApiKey) {
    try {
      const interpretacion = await callDeepseek(config.deepseekApiKey, systemPrompt, userContent);
      return { answer: `${interpretacion}${bloqueVisual}${relacionesTexto}`, sources, sinEvidencia: false };
    } catch (error) {
      console.error("DeepSeek falló, probando respaldo Gemini:", error);
    }
  }

  if (config.geminiApiKey) {
    const interpretacion = await callGemini(config.geminiApiKey, config.geminiModel, systemPrompt, userContent);
    return { answer: `${interpretacion}${bloqueVisual}${relacionesTexto}`, sources, sinEvidencia: false };
  }

  throw new Error("Falta configurar DEEPSEEK_API_KEY (principal) o GEMINI_API_KEY (respaldo)");
}

export function construirSystemPrompt(estilo: EstiloNegrita): string {
  const instruccionNegrita = estilo === "html"
    ? "usa negrita en formato HTML de Telegram: <b>texto</b>. No uses markdown (**texto**)."
    : "usa negrita de WhatsApp: *texto* (un solo asterisco a cada lado). " +
      "No uses HTML (<b>) ni markdown de doble asterisco (**texto**).";

  const ejemploResumen = estilo === "html"
    ? "<b>[resumen de la respuesta en una sola linea, en negrita]</b>"
    : "*[resumen de la respuesta en una sola linea, en negrita]*";

  const ejemploDetalle = estilo === "html"
    ? "[2 a 4 lineas de detalle de apoyo, con terminos clave en <b>negrita</b>]"
    : "[2 a 4 lineas de detalle de apoyo, con terminos clave en *negrita*]";

  const ejemploFuente = estilo === "html"
    ? "📌 Fuente: <b>[numero de alerta o codigo de norma]</b> — [fecha], pag. [numero de pagina]"
    : "📌 Fuente: *[numero de alerta o codigo de norma]* — [fecha], pag. [numero de pagina]";

  return `Eres un asistente que responde preguntas sobre alertas y \
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
${instruccionNegrita}

Estructura SIEMPRE tu respuesta en este formato exacto, pensado para leerse \
rapido en un celular:

${ejemploResumen}

${ejemploDetalle}

${ejemploFuente}

[SOLO si el bloque usado trae "ADVERTENCIA DE CONFIABILIDAD": una linea final \
"⚠️ Verificar con el PDF original: [motivo breve]". Omite esta linea por completo \
si no aplica.]

No agregues secciones adicionales ni encabezados fuera de esta estructura.`;
}

export async function searchConsultaChunks(
  supabase: SupabaseLike,
  query: string,
  limit = 4,
) {
  // buscar_paginas_texto filtra palabras vacias y ordena por relevancia;
  // websearch_to_tsquery exigiria que aparezcan todas las palabras, lo cual
  // falla con preguntas en lenguaje natural.
  const { data, error } = await supabase.rpc("buscar_paginas_texto", {
    query_texto: query,
    limite: limit,
  });

  if (error) throw error;

  return data ?? [];
}

export async function suggestSimilarAlerts(
  supabase: SupabaseLike,
  question: string,
  limit = 3,
) {
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

/** Advertencias de confiabilidad de un bloque de contexto, a partir de las
 * senales que ya calcula la extraccion documental (estado_vigencia,
 * quality_score, has_tables, posible_formula, revisado_manual). Sin esto la
 * IA citaria una pagina OCR de baja confianza o una tabla aplanada con la
 * misma seguridad que contenido ya verificado por un humano. */
// deno-lint-ignore no-explicit-any
export function advertenciasDelBloque(chunk: any): string[] {
  const advertencias: string[] = [];

  // La vigencia es independiente de si la transcripcion fue revisada: una
  // norma derogada sigue derogada aunque su OCR ya este verificado.
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

// deno-lint-ignore no-explicit-any
export function buildConsultaContext(chunks: any[]): string {
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

export type FuenteConsulta = { documentKey: string; url: string; page?: number };

// deno-lint-ignore no-explicit-any
export function consultaSources(chunks: any[]): FuenteConsulta[] {
  const seen = new Set<string>();
  const sources: FuenteConsulta[] = [];

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

async function callDeepseek(
  apiKey: string,
  systemPrompt: string,
  userContent: string,
): Promise<string> {
  const response = await fetch("https://api.deepseek.com/chat/completions", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-chat",
      messages: [
        { role: "system", content: systemPrompt },
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

async function callGemini(
  apiKey: string,
  model: string,
  systemPrompt: string,
  userContent: string,
): Promise<string> {
  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        system_instruction: { parts: [{ text: systemPrompt }] },
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
  // deno-lint-ignore no-explicit-any
  return parts.map((p: any) => p.text ?? "").join("");
}

/** Detecta que la pregunta pide especificamente el contenido de una tabla,
 * cuadro, anexo, grafico o imagen: son justo los casos donde el texto ya
 * extraido (aplanado por OCR/pdfplumber) es menos confiable, asi que ahi
 * conviene el costo extra de mandarle el PDF real a un modelo con vision en
 * vez de conformarse con el texto plano. */
export function preguntaPideVerificacionVisual(pregunta: string): boolean {
  const normalizada = normalizarParaPatron(pregunta);
  return /\b(tabla|cuadro|anexo|grafico|imagen|figura|diagrama|escala de (infraccion|sancion))/.test(normalizada);
}

/** Tope de tamaño del PDF para mandarlo inline a Gemini: la API de Gemini
 * acepta datos inline hasta ~20MB en la request; se deja margen para el
 * resto del payload (prompt, contexto) en vez de pegarle justo al limite. */
const MAX_BYTES_PDF_INLINE = 18 * 1024 * 1024;

/** Codifica un ArrayBuffer a base64 en bloques: hacerlo de una sola pasada
 * con String.fromCharCode(...bytes) revienta el limite de argumentos de la
 * funcion para un PDF de varios MB. */
function arrayBufferABase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  const TAMANO_BLOQUE = 8192;
  let binario = "";
  for (let i = 0; i < bytes.length; i += TAMANO_BLOQUE) {
    binario += String.fromCharCode(...bytes.subarray(i, i + TAMANO_BLOQUE));
  }
  return btoa(binario);
}

/** Le manda el PDF real (no el texto ya extraido) a Gemini como documento
 * nativo: a diferencia del OCR/pdfplumber de la extraccion, un modelo con
 * vision SI puede leer una tabla con celdas combinadas o describir un
 * grafico, porque esta viendo el render real de la pagina. Devuelve null
 * (nunca lanza) si el PDF no esta disponible, es muy grande, o la llamada
 * falla — esto es un complemento opcional a la respuesta de texto, no un
 * requisito; si falla, quien llama sigue con la respuesta normal. */
async function intentarVerificacionVisual(
  // deno-lint-ignore no-explicit-any
  norma: any,
  question: string,
  config: ConfigConsultaIa,
): Promise<string | null> {
  const pdfUrl = norma.pdf_url || norma.source_url;
  if (!pdfUrl || !config.geminiApiKey) return null;

  try {
    const respuestaPdf = await fetch(pdfUrl);
    if (!respuestaPdf.ok) return null;

    const buffer = await respuestaPdf.arrayBuffer();
    if (buffer.byteLength === 0 || buffer.byteLength > MAX_BYTES_PDF_INLINE) return null;

    const base64Pdf = arrayBufferABase64(buffer);
    const systemPrompt =
      "Eres un asistente que responde preguntas sobre tablas, cuadros, anexos o graficos de un " +
      "documento oficial de DIGEMID (Peru), mirando el PDF real adjunto. Cita la pagina exacta. " +
      "Si la tabla tiene celdas combinadas, describe la combinacion en vez de fingir una grilla " +
      "simple. Si no encuentras lo que se pregunta en el documento, dilo explicitamente.";

    const response = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/${config.geminiModel}:generateContent?key=${config.geminiApiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          system_instruction: { parts: [{ text: systemPrompt }] },
          contents: [{
            role: "user",
            parts: [
              { inline_data: { mime_type: "application/pdf", data: base64Pdf } },
              { text: `Documento: ${norma.document_key}\n\nPregunta: ${question}` },
            ],
          }],
          generationConfig: { maxOutputTokens: 1024 },
        }),
      },
    );

    if (!response.ok) return null;

    const data = await response.json();
    const parts = data.candidates?.[0]?.content?.parts ?? [];
    // deno-lint-ignore no-explicit-any
    const texto = parts.map((p: any) => p.text ?? "").join("").trim();
    return texto || null;
  } catch (error) {
    console.error("Verificacion visual del PDF falló (se sigue con la respuesta de texto):", error);
    return null;
  }
}

export type ConfigConsultaIa = {
  deepseekApiKey: string;
  geminiApiKey: string;
  geminiModel: string;
  estiloNegrita: EstiloNegrita;
};

export type RespuestaConsulta = {
  answer: string;
  sources: FuenteConsulta[];
  /** true cuando no hubo contexto documental suficiente y la respuesta NO
   * proviene de la base: el canal debe presentarla como tal, nunca como una
   * conclusion regulatoria. */
  sinEvidencia: boolean;
};

const PROMPT_REFORMULACION =
  "Eres un asistente que reformula preguntas de usuarios en una consulta de " +
  "busqueda de texto breve y precisa. Responde UNICAMENTE con la consulta " +
  "reformulada (maximo 12 palabras, terminos legales/tecnicos concretos en " +
  "español), sin explicaciones, sin comillas, sin texto adicional.";

/** Reescribe la pregunta del usuario en una consulta mas apta para la
 * busqueda de texto (RPC buscar_paginas_texto), para que preguntas vagas o
 * mal formuladas ("esa cosa de los precios raros de las boticas") encuentren
 * paginas que una busqueda literal de esas palabras no encontraria. Si la
 * reformulacion falla (o no hay proveedor configurado), se sigue con la
 * pregunta original: esta reescritura es una ayuda, no un requisito. */
export async function reformularConsultaParaBusqueda(
  question: string,
  config: ConfigConsultaIa,
): Promise<string> {
  try {
    if (config.deepseekApiKey) {
      const reformulada = await callDeepseek(config.deepseekApiKey, PROMPT_REFORMULACION, question);
      return reformulada.trim() || question;
    }
    if (config.geminiApiKey) {
      const reformulada = await callGemini(
        config.geminiApiKey,
        config.geminiModel,
        PROMPT_REFORMULACION,
        question,
      );
      return reformulada.trim() || question;
    }
  } catch (error) {
    console.error("No se pudo reformular la consulta, se usa la pregunta original:", error);
  }

  return question;
}

export async function answerConsulta(
  supabase: SupabaseLike,
  question: string,
  config: ConfigConsultaIa,
): Promise<RespuestaConsulta> {
  // "¿cuál es la última norma...?" no lo responde ninguna busqueda de texto:
  // lo que se pide no esta escrito en ningun documento, es un hecho sobre el
  // catalogo. Se resuelve con datos ordenados por fecha, sin gastar ni una
  // llamada al modelo.
  if (pareceConsultaSobreUltimaNorma(question)) {
    const rows = await getLatestNormativa(supabase, 5);
    return {
      answer: formatUltimaNormativaComoRespuesta(rows, config.estiloNegrita),
      sources: rows.map((row) => ({ documentKey: row.document_key, url: row.detail_url })),
      sinEvidencia: false,
    };
  }

  // Si la pregunta cita una norma puntual por numero exacto ("Resolucion
  // Ministerial 727-2025"), no se usa busqueda difusa: se va directo a esa
  // norma en digemid_normas, con su contenido real y sus relaciones. La
  // busqueda por relevancia puede devolver una norma distinta que "suena"
  // parecida, que es justo lo que se quiere evitar aqui.
  const referencia = extraerReferenciaNormativa(question);
  if (referencia) {
    const respuesta = await responderNormaPuntual(supabase, referencia.documentKey, question, config);
    if (respuesta) return respuesta;
    // No se encontro esa norma exacta en digemid_normas: se cae al flujo
    // normal de busqueda de texto como respaldo, en vez de fallar.
  }

  const consultaBusqueda = await reformularConsultaParaBusqueda(question, config);
  const chunks = await searchConsultaChunks(supabase, consultaBusqueda);

  if (!chunks.length) {
    const suggestions = await suggestSimilarAlerts(supabase, consultaBusqueda);

    if (!suggestions.length) {
      return {
        answer: "No encontré documentos relacionados con esa consulta en la base de datos.",
        sources: [],
        sinEvidencia: true,
      };
    }

    return {
      answer:
        "No encontré una coincidencia exacta para tu pregunta. ¿Quizás te refieres a alguna de estas alertas?",
      sources: suggestions.map((s) => ({ documentKey: s.document_key, url: s.detail_url })),
      sinEvidencia: true,
    };
  }

  const systemPrompt = construirSystemPrompt(config.estiloNegrita);
  const userContent = `Contexto:\n\n${buildConsultaContext(chunks)}\n\nPregunta: ${question}`;
  const sources = consultaSources(chunks);

  if (config.deepseekApiKey) {
    try {
      return {
        answer: await callDeepseek(config.deepseekApiKey, systemPrompt, userContent),
        sources,
        sinEvidencia: false,
      };
    } catch (error) {
      console.error("DeepSeek falló, probando respaldo Gemini:", error);
    }
  }

  if (config.geminiApiKey) {
    return {
      answer: await callGemini(config.geminiApiKey, config.geminiModel, systemPrompt, userContent),
      sources,
      sinEvidencia: false,
    };
  }

  throw new Error("Falta configurar DEEPSEEK_API_KEY (principal) o GEMINI_API_KEY (respaldo)");
}
