import {
  type ConsultaEvidence,
  evaluateConsultaSafety,
  executeConsultaWithSafetyGate,
} from "./consulta_safety.ts";
import type { NormaResolution } from "./norm_identity.ts";
import {
  type ConsultaIntent,
  deterministicConsultaIntent,
} from "./consulta_intent.ts";

// Orquestación exclusiva de Telegram, sin credenciales ni arranque del webhook.
// Las reglas siguen teniendo una sola autoridad: consulta_safety/revision_safety.
export type ConsultaChunk = ConsultaEvidence & Record<string, any>;
export type ConsultaRespuesta = {
  answer: string;
  sources: { documentKey: string; url: string; page?: number }[];
  reportHtml?: string;
  reportFileName?: string;
  blocked?: boolean;
};

export function hasRecoverableText(chunk: ConsultaChunk): boolean {
  return typeof chunk.text_content === "string" && chunk.text_content.trim().length > 0;
}

function isUnreviewedOcr(chunk: ConsultaChunk): boolean {
  return chunk.revisado_manual !== true &&
    (chunk.ocr_used === true ||
      String(chunk.extraction_method ?? "").toLowerCase().includes("ocr"));
}

function unreviewedOcrPages(chunks: ConsultaChunk[]): number[] {
  return [
    ...new Set(
      chunks.filter((chunk) => isUnreviewedOcr(chunk)).map((chunk) =>
        Number(chunk.page_number)
      ).filter(Number.isInteger),
    ),
  ]
    .sort((a, b) => a - b);
}

function normalizedWarningText(value: string): string {
  return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
}

/** El modelo puede obedecer el prompt y cerrar con un aviso OCR. El runtime
 * es la autoridad para mostrarlo una sola vez y con texto determinístico.
 * Solo se retira un aviso OCR final equivalente; tabla/fórmula/estructura se
 * preservan porque representan riesgos distintos. */
function stripTrailingEquivalentOcrWarning(answer: string): string {
  const marker = answer.lastIndexOf("⚠️");
  if (marker < 0) return answer.trimEnd();
  const warning = normalizedWarningText(answer.slice(marker));
  const isOcrEquivalent = warning.includes("ocr") ||
    warning.includes("transcripcion no verificada") ||
    (warning.includes("encabezado") && warning.includes("articulo") &&
      warning.includes("deform"));
  const hasIndependentRisk =
    /\b(tabla|formula|estructura(?:da)?|fila|columna)\b/.test(
      warning,
    );
  return isOcrEquivalent && !hasIndependentRisk
    ? answer.slice(0, marker).trimEnd()
    : answer.trimEnd();
}

function consolidateOcrWarning(
  answer: string,
  chunks: ConsultaChunk[],
): string {
  const ocrPages = unreviewedOcrPages(chunks);
  if (!ocrPages.length) return answer;
  const hasContextualRecovery = chunks.some((chunk) =>
    isUnreviewedOcr(chunk) &&
    chunk.article_match_method === "ocr_contextual_recovery"
  );
  const warning = hasContextualRecovery
    ? "⚠️ Texto obtenido por OCR y no revisado manualmente; el encabezado del artículo presenta una deformación OCR. Verificar con el PDF oficial."
    : `⚠️ La evidencia de la(s) página(s) ${
      ocrPages.join(", ")
    } proviene de OCR y aún no fue revisada por una persona.`;
  const withoutEquivalent = stripTrailingEquivalentOcrWarning(answer);
  return `${withoutEquivalent}\n\n${warning}`;
}

export type ConsultaRuntimeDependencies = {
  interpretIntent?: (question: string) => Promise<ConsultaIntent>;
  searchChunks: (question: string) => Promise<ConsultaChunk[]>;
  suggestSimilarAlerts: (question: string) => Promise<any[]>;
  resolverNormaPuntual: (
    question: string,
  ) => Promise<NormaResolution>;
  getNormaConContenido: (key: string, intent?: ConsultaIntent) => Promise<
    {
      norma: any;
      paginas: any[];
      relaciones: any[];
    } | null
  >;
  paginaComoChunk: (pagina: any, norma: any) => ConsultaChunk;
  buildContext: (chunks: ConsultaChunk[]) => string;
  sources: (chunks: ConsultaChunk[]) => ConsultaRespuesta["sources"];
  formatRelacionesComoContexto: (relaciones: any[], norma: any) => string;
  formatRelacionesParaUsuario: (relaciones: any[], norma: any) => string;
  construirReporte: (
    norma: any,
    paginas: any[],
    relaciones: any[],
    answer: string,
  ) => string;
  deepseek?: (content: string) => Promise<string>;
  gemini?: (content: string) => Promise<string>;
};

export function normalizeConsultaEvidence(chunk: ConsultaChunk): ConsultaChunk {
  const metadata = chunk.metadata && typeof chunk.metadata === "object"
    ? chunk.metadata
    : {};
  // Ausencia de señal no equivale a una constatación de ausencia de tablas.
  // Se marca riesgo posible en memoria; nunca se inventa una verificación.
  const unknownTablePresence = typeof chunk.has_tables !== "boolean" &&
    typeof chunk.possible_table !== "boolean" &&
    typeof metadata.possible_table !== "boolean";
  return {
    ...chunk,
    possible_table:
      chunk.possible_table === true || metadata.possible_table === true ||
        unknownTablePresence
        ? true
        : chunk.possible_table,
    table_requires_review: chunk.table_requires_review === true ||
        metadata.table_requires_review === true
      ? true
      : chunk.table_requires_review,
    table_quality_score: chunk.table_quality_score ??
      (typeof metadata.table_quality_score === "number"
        ? metadata.table_quality_score
        : undefined),
    tables_markdown: chunk.tables_markdown ??
      (typeof metadata.tables_markdown === "string"
        ? metadata.tables_markdown
        : undefined),
    metadata,
  };
}

/** El RPC histórico omite metadata y coalesce oculta flags nulos. Recuperamos
 * la página original por identidad, solo mediante lecturas existentes.
 * Un resultado ausente/inconsistente conserva el texto pero queda pendiente. */
export async function hydrateConsultaChunks(
  db: { from: (table: string) => any },
  chunks: ConsultaChunk[],
): Promise<ConsultaChunk[]> {
  return await Promise.all(chunks.map(async (chunk) => {
    const unknown = () => ({
      ...chunk,
      possible_table: true,
      table_requires_review: true,
    });
    if (!chunk.document_key || !Number.isInteger(chunk.page_number)) {
      return unknown();
    }
    const { data: norma, error: normaError } = await db.from("digemid_normas")
      .select("id").eq("document_key", chunk.document_key).maybeSingle();
    if (normaError) return unknown();
    let document = norma;
    if (!document) {
      const { data, error } = await db.from("digemid_documentos")
        .select("id").eq("document_key", chunk.document_key).maybeSingle();
      if (error || !data) return unknown();
      document = data;
    }
    const { data: page, error } = await db
      .from(norma ? "digemid_norma_paginas" : "digemid_documento_paginas")
      // Alertas tienen variantes de columnas legacy; no presuponer metadata.
      .select(
        norma
          ? "page_number, text_normalized, text_raw, has_tables, revisado_manual, tabla_verificada, extraction_method, ocr_used, metadata"
          : "*",
      )
      .eq(norma ? "norma_id" : "document_id", document.id)
      .eq("page_number", chunk.page_number).maybeSingle();
    if (error || !page) return unknown();
    const text = norma
      ? page.text_normalized ?? page.text_raw
      : page.text_content;
    // No unir texto de una versión con la verificación de otra.
    if (text !== chunk.text_content) return unknown();
    const hasTables = norma
      ? page.has_tables
      : page.contiene_tablas === true || page.has_table === true
      ? true
      : page.contiene_tablas ?? page.has_table;
    return normalizeConsultaEvidence({
      ...chunk,
      has_tables: chunk.has_tables === true ? true : hasTables,
      possible_table:
        chunk.possible_table === true || chunk.metadata?.possible_table === true
          ? true
          : page.possible_table,
      table_requires_review: chunk.table_requires_review === true ||
          chunk.metadata?.table_requires_review === true
        ? true
        : page.table_requires_review,
      tabla_verificada: page.tabla_verificada,
      revisado_manual: page.revisado_manual,
      metadata: page.metadata,
      extraction_method: page.extraction_method,
      ocr_used: page.ocr_used,
    });
  }));
}

export async function answerConsultaRuntime(
  question: string,
  deps: ConsultaRuntimeDependencies,
): Promise<ConsultaRespuesta> {
  let intent: ConsultaIntent;
  try {
    intent = deps.interpretIntent
      ? await deps.interpretIntent(question)
      : deterministicConsultaIntent(question);
  } catch {
    // Un router externo defectuoso nunca rompe /consulta ni evita el parser local.
    intent = deterministicConsultaIntent(question);
  }
  const resolution = await deps.resolverNormaPuntual(question);
  const unavailable = (): ConsultaRespuesta => ({
    answer:
      "No pude verificar la norma exacta por un problema técnico. Inténtalo nuevamente más tarde.",
    sources: [],
  });
  if (resolution.status === "error") return unavailable();
  if (resolution.status === "ambiguous") {
    return {
      answer: resolution.reason === "incomplete"
        ? "Indica una sola norma con su tipo y, si corresponde, año y sector; por ejemplo RM 554-2022/MINSA."
        : resolution.reason === "multiple"
        ? "Encontré más de una norma que coincide con la referencia. Indica el tipo, año y sector para precisar la norma solicitada."
        : "La identidad de esa norma está incompleta o tiene un registro histórico pendiente de aclarar en la biblioteca. Necesito una referencia inequívoca antes de interpretar su contenido.",
      sources: [],
    };
  }
  const notFound = (): ConsultaRespuesta => ({
    answer:
      "No encontré la norma exacta en la biblioteca normativa. No usaré documentos que solo la mencionan como sustituto de la norma solicitada.",
    sources: [],
  });
  if (resolution.status === "not_found") return notFound();
  let exact = null;
  if (resolution.status === "resolved") {
    try {
      exact = await deps.getNormaConContenido(resolution.documentKey, intent);
    } catch {
      return unavailable();
    }
    // Una eliminación o inconsistencia entre las dos lecturas nunca habilita RPC.
    if (!exact) return notFound();
    if (exact.norma.document_key !== resolution.documentKey) {
      return unavailable();
    }
  }
  const chunks = (
    (exact
      ? exact.paginas.map((page) => deps.paginaComoChunk(page, exact.norma))
      : await deps.searchChunks(intent.search_query || question)).map(normalizeConsultaEvidence)
  ).filter(hasRecoverableText);

  if (!chunks.length) {
    if (exact) {
      return {
        answer: intent.intent === "article_lookup"
          ? `Encontré <b>${exact.norma.document_key}</b>, pero no pude localizar con suficiente confianza ` +
            `los límites del artículo ${intent.article ?? "solicitado"}. No enviaré la norma completa al modelo; verifica directamente con el PDF oficial.`
          : `Encontré <b>${exact.norma.document_key}</b> en la base, pero su contenido todavía no fue ` +
            "extraído/revisado, así que no puedo interpretarla todavía. Verifica directamente con el PDF oficial.",
        sources: [{
          documentKey: exact.norma.document_key,
          url: exact.norma.pdf_url ?? exact.norma.source_url ?? "",
        }],
      };
    }
    const suggestions = await deps.suggestSimilarAlerts(question);
    return {
      answer: suggestions.length
        ? "No encontré una coincidencia exacta para tu pregunta. ¿Quizás te refieres a alguna de estas alertas?"
        : "No encontré documentos relacionados con esa consulta en la base de datos.",
      sources: suggestions.map((s) => ({
        documentKey: s.document_key,
        url: s.detail_url,
      })),
    };
  }

  const safetyQuestion = intent.needs_structured_table
    ? `${question} tabla`
    : question;
  const result = await executeConsultaWithSafetyGate(
    safetyQuestion,
    chunks,
    async (permitted) => {
      // No cerrar sobre chunks/paginas para construir contexto ni reintroducirlos
      // a través del PDF completo o relaciones sin procedencia por página.
      const safeChunks = permitted as ConsultaChunk[];
      const allPermitted = safeChunks.length === chunks.length;
      const relations = exact && allPermitted && intent.intent !== "article_lookup"
        ? exact.relaciones
        : [];
      const context = deps.buildContext(safeChunks);
      const userContent = `Contexto:\n\n${context}${
        exact ? deps.formatRelacionesComoContexto(relations, exact.norma) : ""
      }\n\nPregunta: ${question}`;
      let answer: string | undefined;
      if (deps.deepseek) {
        try {
          answer = await deps.deepseek(userContent);
        } catch {
          // No imprimir respuestas de proveedores, URLs con keys ni prompts.
          console.error("CONSULTA_DEEPSEEK_FALLBACK");
        }
      }
      if (answer === undefined && deps.gemini) {
        answer = await deps.gemini(
          userContent,
        );
      }
      if (answer === undefined) {
        throw new Error(
          "Falta configurar DEEPSEEK_API_KEY (principal) o GEMINI_API_KEY (respaldo)",
        );
      }
      const answerWithOcrWarning = consolidateOcrWarning(answer, safeChunks);
      const response: ConsultaRespuesta = {
        answer: answerWithOcrWarning,
        sources: deps.sources(safeChunks),
        blocked: false,
      };
      if (exact) {
        const permittedPages = new Set(
          safeChunks.map((chunk) => chunk.page_number),
        );
        response.reportHtml = deps.construirReporte(
          exact.norma,
          exact.paginas.filter((page) => permittedPages.has(page.page_number)),
          relations,
          answerWithOcrWarning,
        );
        response.reportFileName = `reporte_${exact.norma.document_key}.html`;
        response.answer += deps.formatRelacionesParaUsuario(
          relations,
          exact.norma,
        );
      }
      return response;
    },
  );
  if (!result.blocked) return result.value;

  const decision = evaluateConsultaSafety(safetyQuestion, chunks);
  for (const chunk of result.evidence.length ? result.evidence : chunks) {
    console.info("CONSULTA_SAFETY_BLOCK", {
      reason: decision.reason,
      document_key: chunk.document_key,
      page_number: chunk.page_number,
    });
  }
  return {
    blocked: true,
    answer: result.answer,
    sources: deps.sources(result.evidence),
  };
}
