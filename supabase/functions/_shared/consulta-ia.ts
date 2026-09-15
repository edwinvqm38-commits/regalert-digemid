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
