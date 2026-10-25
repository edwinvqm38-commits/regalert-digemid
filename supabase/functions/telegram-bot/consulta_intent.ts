export const CONSULTA_INTENTS = [
  "document_summary",
  "article_lookup",
  "regulatory_question",
  "alert_search",
  "product_lookup",
  "comparison",
  "botica_impact",
  "table_lookup",
  "general_search",
] as const;

export type ConsultaIntentName = typeof CONSULTA_INTENTS[number];

export type ConsultaIntent = {
  intent: ConsultaIntentName;
  document_key: string | null;
  article: string | null;
  alert_number: string | null;
  product: string | null;
  lot: string | null;
  search_query: string;
  needs_structured_table: boolean;
};

export type ConsultaIntentConfig = {
  deepseekApiKey?: string;
  geminiApiKey?: string;
  geminiModel?: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
};

const INTENT_PROMPT =
  `Clasifica una consulta DIGEMID. No respondas su contenido.
Devuelve SOLO JSON con: intent, document_key, article, alert_number, product, lot, search_query, needs_structured_table.
intent: document_summary|article_lookup|regulatory_question|alert_search|product_lookup|comparison|botica_impact|table_lookup|general_search.
Usa null si no aplica. needs_structured_table=true si pide tabla, fila, columna, celda o correspondencia. search_query debe ser breve.`;

const NUMBER_WORDS: Record<string, string> = {
  uno: "1",
  un: "1",
  primero: "1",
  dos: "2",
  segundo: "2",
  tres: "3",
  tercero: "3",
  cuatro: "4",
  cuarto: "4",
  cinco: "5",
  quinto: "5",
  seis: "6",
  sexto: "6",
  siete: "7",
  septimo: "7",
  ocho: "8",
  octavo: "8",
  nueve: "9",
  noveno: "9",
  diez: "10",
  decimo: "10",
};

function plain(value: string): string {
  return value.toLowerCase().normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function nullableString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return normalized ? normalized.slice(0, 160) : null;
}

function explicitTableRequest(question: string): boolean {
  const text = plain(question);
  return /\b(tabla|fila|columna|celda|casilla|cuadro|matriz)\b/.test(text) ||
    /\b(valor|sancion|multa|obligacion)\b.*\b(corresponde|segun|fila|columna)\b/
      .test(text) ||
    /\b(compara|comparar|comparacion)\b.*\b(columnas?|filas?|celdas?)\b/.test(
      text,
    );
}

function extractArticle(question: string): string | null {
  const match = plain(question).match(
    /\bart(?:iculo)?\.?\s+(\d+[a-z]?|uno|un|primero|dos|segundo|tres|tercero|cuatro|cuarto|cinco|quinto|seis|sexto|siete|septimo|ocho|octavo|nueve|noveno|diez|decimo)\b/,
  );
  if (!match) return null;
  return NUMBER_WORDS[match[1]] ?? match[1];
}

function extractDocumentKey(question: string): string | null {
  const text = plain(question);
  const patterns: [RegExp, string][] = [
    [
      /\bley\s+(?:n(?:ro|umero)?\.?\s*[°ºo]?\s*)?(\d{1,6})(?:[-/]?(\d{4}))?\b/,
      "LEY",
    ],
    [
      /\b(?:decreto\s+supremo|d\.?\s*s\.?)\s+(?:n(?:ro|umero)?\.?\s*[°ºo]?\s*)?(\d{1,4})(?:[-/](\d{4}))?(?:[-/]([a-z]+))?\b/,
      "DS",
    ],
    [
      /\b(?:resolucion\s+ministerial|r\.?\s*m\.?)\s+(?:n(?:ro|umero)?\.?\s*[°ºo]?\s*)?(\d{1,4})(?:[-/](\d{4}))?(?:[-/]([a-z]+))?\b/,
      "RM",
    ],
  ];
  for (const [pattern, type] of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    return [type, String(Number(match[1])), match[2], match[3]?.toUpperCase()]
      .filter(Boolean).join("-");
  }
  return null;
}

function extractAlertNumber(question: string): string | null {
  const match = plain(question).match(
    /\balerta(?:\s+(?:n(?:ro|umero)?\.?))?\s*[°º]?\s*(\d{1,4}(?:[-/]\d{4})?)\b/,
  );
  return match?.[1]?.replace("/", "-") ?? null;
}

function extractAfter(question: string, label: string): string | null {
  const match = question.match(
    new RegExp(`\\b${label}\\s+([A-Za-z0-9][A-Za-z0-9._/-]{1,79})`, "i"),
  );
  return match?.[1] ?? null;
}

export function deterministicConsultaIntent(question: string): ConsultaIntent {
  const text = plain(question);
  const article = extractArticle(question);
  const documentKey = extractDocumentKey(question);
  const alertNumber = extractAlertNumber(question);
  const lot = extractAfter(question, "lote");
  const product = extractAfter(question, "producto");
  const structured = explicitTableRequest(question);
  let intent: ConsultaIntentName = "general_search";
  if (structured) intent = "table_lookup";
  else if (/\b(compara|comparar|comparacion|diferencia)\b/.test(text)) {
    intent = "comparison";
  } else if (article) intent = "article_lookup";
  else if (/\b(botica|farmacia)\b/.test(text)) intent = "botica_impact";
  else if (lot || product) intent = "product_lookup";
  else if (alertNumber || /\balertas?\b/.test(text)) intent = "alert_search";
  else if (
    documentKey && /\b(resume|resumen|sintesis|de que trata)\b/.test(text)
  ) intent = "document_summary";
  else if (
    documentKey ||
    /\b(norma|ley|reglamento|obligacion|almacenamiento)\b/.test(text)
  ) {
    intent = "regulatory_question";
  }

  const focusedSearch = article && documentKey
    ? `artículo ${article} ${documentKey}`
    : [product, lot ? `lote ${lot}` : null, alertNumber, documentKey]
      .filter(Boolean).join(" ") || question.trim();
  return {
    intent,
    document_key: documentKey,
    article,
    alert_number: alertNumber,
    product,
    lot,
    search_query: focusedSearch.slice(0, 200),
    needs_structured_table: structured,
  };
}

function parseProviderIntent(raw: string, question: string): ConsultaIntent {
  const fallback = deterministicConsultaIntent(question);
  const fenced = raw.trim().replace(/^```(?:json)?\s*/i, "").replace(
    /\s*```$/,
    "",
  );
  const start = fenced.indexOf("{");
  const end = fenced.lastIndexOf("}");
  if (start < 0 || end <= start) throw new Error("INTENT_JSON_INVALID");
  const value = JSON.parse(fenced.slice(start, end + 1));
  if (
    !value || typeof value !== "object" ||
    !CONSULTA_INTENTS.includes(value.intent)
  ) {
    throw new Error("INTENT_SCHEMA_INVALID");
  }
  const forcedStructured = explicitTableRequest(question);
  const searchQuery = nullableString(value.search_query) ??
    fallback.search_query;
  return {
    intent: forcedStructured
      ? "table_lookup"
      : fallback.article
      ? "article_lookup"
      : value.intent,
    // Lo explícito en la pregunta prevalece sobre una normalización del modelo.
    document_key: fallback.document_key ?? nullableString(value.document_key),
    article: fallback.article ?? nullableString(value.article),
    alert_number: fallback.alert_number ?? nullableString(value.alert_number),
    product: fallback.product ?? nullableString(value.product),
    lot: fallback.lot ?? nullableString(value.lot),
    search_query: searchQuery.slice(0, 200),
    needs_structured_table: forcedStructured ||
      value.needs_structured_table === true,
  };
}

async function callDeepSeekIntent(
  question: string,
  apiKey: string,
  fetchImpl: typeof fetch,
  timeoutMs: number,
): Promise<string> {
  const response = await fetchImpl(
    "https://api.deepseek.com/chat/completions",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      signal: AbortSignal.timeout(timeoutMs),
      body: JSON.stringify({
        model: "deepseek-chat",
        messages: [
          { role: "system", content: INTENT_PROMPT },
          { role: "user", content: question },
        ],
        temperature: 0,
        max_tokens: 220,
        response_format: { type: "json_object" },
      }),
    },
  );
  if (!response.ok) throw new Error(`INTENT_DEEPSEEK_${response.status}`);
  const data = await response.json();
  return data.choices?.[0]?.message?.content ?? "";
}

async function callGeminiIntent(
  question: string,
  apiKey: string,
  model: string,
  fetchImpl: typeof fetch,
  timeoutMs: number,
): Promise<string> {
  const response = await fetchImpl(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: AbortSignal.timeout(timeoutMs),
      body: JSON.stringify({
        system_instruction: { parts: [{ text: INTENT_PROMPT }] },
        contents: [{ role: "user", parts: [{ text: question }] }],
        generationConfig: {
          temperature: 0,
          maxOutputTokens: 220,
          responseMimeType: "application/json",
        },
      }),
    },
  );
  if (!response.ok) throw new Error(`INTENT_GEMINI_${response.status}`);
  const data = await response.json();
  return (data.candidates?.[0]?.content?.parts ?? [])
    .map((part: { text?: string }) => part.text ?? "").join("");
}

/** Solo clasifica la pregunta. No recibe evidencia ni genera una respuesta regulatoria. */
export async function interpretConsultaIntent(
  question: string,
  config: ConsultaIntentConfig,
): Promise<ConsultaIntent> {
  const fetchImpl = config.fetchImpl ?? fetch;
  const timeoutMs = config.timeoutMs ?? 5000;
  if (config.deepseekApiKey) {
    try {
      return parseProviderIntent(
        await callDeepSeekIntent(
          question,
          config.deepseekApiKey,
          fetchImpl,
          timeoutMs,
        ),
        question,
      );
    } catch {
      console.error("CONSULTA_INTENT_DEEPSEEK_FALLBACK");
    }
  }
  if (config.geminiApiKey) {
    try {
      return parseProviderIntent(
        await callGeminiIntent(
          question,
          config.geminiApiKey,
          config.geminiModel ?? "gemini-flash-latest",
          fetchImpl,
          timeoutMs,
        ),
        question,
      );
    } catch {
      console.error("CONSULTA_INTENT_DETERMINISTIC_FALLBACK");
    }
  }
  return deterministicConsultaIntent(question);
}
