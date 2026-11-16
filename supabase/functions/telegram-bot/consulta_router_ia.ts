/**
 * Router de IA para /consulta.
 *
 * Una sola llamada barata interpreta la pregunta ANTES de las reglas por
 * palabras clave, para no depender de suposiciones. Devuelve una ruta y, si la
 * pregunta es un listado/conteo, una frase canónica que las rutas existentes
 * ya reconocen. Ante cualquier fallo devuelve null y el flujo anterior sigue
 * intacto (fail-open hacia el comportamiento previo, nunca hacia inventar).
 */

export type RutaConsulta =
  | "listar_alertas"
  | "contar_alertas"
  | "listar_normativa"
  | "pregunta_documental"
  | "ambigua";

export type PeriodoConsulta = "ultimas" | "hoy" | "semana" | "mes";

export type RouterConsultaResult = {
  route: RutaConsulta;
  period: PeriodoConsulta | null;
  limit: number | null;
  clarification: string | null;
  provider: "deepseek" | "gemini";
};

export type RouterConsultaOptions = {
  deepseekApiKey?: string;
  geminiApiKey?: string;
  geminiModel?: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
};

export const ROUTER_PROMPT = `Eres el intérprete de preguntas de un bot de alertas y normativa de DIGEMID (Perú).
Clasifica la pregunta del usuario. Responde SOLO JSON:
{"route":"...","period":"ultimas|hoy|semana|mes|null","limit":número|null,"clarification":"texto|null"}

Rutas:
- listar_alertas: quiere ver alertas sanitarias (últimas, de hoy, de la semana, del mes).
- contar_alertas: quiere saber CUÁNTAS alertas hay (period hoy|semana|mes).
- listar_normativa: quiere ver las normas/leyes/decretos/resoluciones MÁS RECIENTES o recién publicadas, sin un tema concreto (ej. "qué dice la última norma que ha salido").
- pregunta_documental: pregunta sobre el CONTENIDO de una norma, tema, medicamento, requisito o artículo concreto.
- ambigua: no se entiende qué quiere. Escribe en clarification UNA pregunta corta en español para aclarar.

Reglas: no inventes; si menciona un tema, medicamento, número de norma o artículo concreto es pregunta_documental. limit solo si pide una cantidad (1-10).`;

const ROUTES: RutaConsulta[] = [
  "listar_alertas",
  "contar_alertas",
  "listar_normativa",
  "pregunta_documental",
  "ambigua",
];
const PERIODS: PeriodoConsulta[] = ["ultimas", "hoy", "semana", "mes"];

export function parseRouterJson(
  content: string,
  provider: "deepseek" | "gemini",
): RouterConsultaResult | null {
  let data: Record<string, unknown>;
  try {
    const clean = content.replace(/^```(?:json)?\s*|\s*```$/g, "").trim();
    data = JSON.parse(clean);
  } catch {
    return null;
  }
  const route = data.route as RutaConsulta;
  if (!ROUTES.includes(route)) return null;

  let period = PERIODS.includes(data.period as PeriodoConsulta)
    ? (data.period as PeriodoConsulta)
    : null;
  if (route === "listar_alertas" && !period) period = "ultimas";
  if (route === "contar_alertas" && (!period || period === "ultimas")) {
    return null; // un conteo sin periodo no es fiable
  }

  const limit = Number.isInteger(data.limit) && (data.limit as number) >= 1 &&
      (data.limit as number) <= 10
    ? (data.limit as number)
    : null;

  const clarification = typeof data.clarification === "string"
    ? data.clarification.trim().slice(0, 300) || null
    : null;
  if (route === "ambigua" && !clarification) return null;

  return {
    route,
    period: route === "listar_alertas" || route === "contar_alertas"
      ? period
      : null,
    limit: route === "listar_alertas" ? limit : null,
    clarification: route === "ambigua" ? clarification : null,
    provider,
  };
}

async function callWithTimeout(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit,
  timeoutMs: number,
): Promise<unknown | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetchImpl(url, { ...init, signal: controller.signal });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

export async function routeConsultaConIA(
  question: string,
  options: RouterConsultaOptions,
): Promise<RouterConsultaResult | null> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs ?? 5000;
  const text = question.trim().slice(0, 500);
  if (!text) return null;

  if (options.deepseekApiKey) {
    const json = await callWithTimeout(
      fetchImpl,
      "https://api.deepseek.com/chat/completions",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${options.deepseekApiKey}`,
        },
        body: JSON.stringify({
          model: "deepseek-chat",
          temperature: 0,
          max_tokens: 160,
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: ROUTER_PROMPT },
            { role: "user", content: text },
          ],
        }),
      },
      timeoutMs,
    ) as { choices?: { message?: { content?: string } }[] } | null;
    const parsed = parseRouterJson(
      json?.choices?.[0]?.message?.content ?? "",
      "deepseek",
    );
    if (parsed) return parsed;
  }

  if (options.geminiApiKey) {
    const model = options.geminiModel ?? "gemini-flash-latest";
    const json = await callWithTimeout(
      fetchImpl,
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${options.geminiApiKey}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          systemInstruction: { parts: [{ text: ROUTER_PROMPT }] },
          contents: [{ role: "user", parts: [{ text }] }],
          generationConfig: {
            temperature: 0,
            maxOutputTokens: 160,
            responseMimeType: "application/json",
          },
        }),
      },
      timeoutMs,
    ) as
      | { candidates?: { content?: { parts?: { text?: string }[] } }[] }
      | null;
    const parsed = parseRouterJson(
      json?.candidates?.[0]?.content?.parts?.[0]?.text ?? "",
      "gemini",
    );
    if (parsed) return parsed;
  }

  return null;
}

/** Frase canónica que las rutas por reglas de index.ts ya reconocen.
 * null = la pregunta sigue tal cual (documental) o requiere aclaración. */
export function frasePorRuta(r: RouterConsultaResult): string | null {
  switch (r.route) {
    case "listar_normativa":
      return "últimas normativas";
    case "contar_alertas":
      return r.period === "hoy"
        ? "cuántas alertas hoy"
        : r.period === "semana"
        ? "cuántas alertas esta semana"
        : "cuántas alertas este mes";
    case "listar_alertas":
      if (r.period === "hoy") return "alertas de hoy";
      if (r.period === "semana") return "alertas de esta semana";
      if (r.period === "mes") return "alertas de este mes";
      return r.limit === 1
        ? "la última alerta"
        : `últimas ${r.limit ?? 5} alertas`;
    default:
      return null;
  }
}
