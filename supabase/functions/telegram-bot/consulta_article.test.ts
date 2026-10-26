import assert from "node:assert/strict";
import {
  type ArticlePage,
  articleSearchIdentifiers,
  MAX_ARTICLE_EVIDENCE_PAGES,
  selectArticleEvidence,
} from "./consulta_article.ts";
import {
  answerConsultaRuntime,
  type ConsultaChunk,
  type ConsultaRuntimeDependencies,
} from "./consulta_runtime.ts";
import { deterministicConsultaIntent } from "./consulta_intent.ts";

function page(
  pageNumber: number,
  text: string,
  extra: Partial<ArticlePage> = {},
): ArticlePage {
  return {
    page_number: pageNumber,
    source_asset_id: "asset-ley",
    text_normalized: text,
    text_raw: text,
    has_tables: false,
    ...extra,
  };
}

Deno.test("article_lookup: artículo de una sola página se recorta por encabezados", () => {
  const result = selectArticleEvidence([
    page(
      1,
      "Preámbulo\nArtículo 5.- Texto solicitado.\nArtículo 6º Texto ajeno.",
    ),
  ], "5");

  assert.equal(result.status, "found");
  assert.equal(result.pages.length, 1);
  assert.equal(result.pages[0].page_number, 1);
  assert.equal(result.pages[0].source_asset_id, "asset-ley");
  assert.match(String(result.pages[0].text_normalized), /^Artículo 5/);
  assert.doesNotMatch(String(result.pages[0].text_normalized), /Artículo 6/);
});

Deno.test("article_lookup: artículo atraviesa páginas 2 a 4", () => {
  const result = selectArticleEvidence([
    page(2, "Cabecera\nArtículo 5° Inicio al final de la página."),
    page(3, "Continuación literal del artículo."),
    page(4, "Fin del artículo.\nArtículo 6.- Siguiente."),
  ], "5");

  assert.equal(result.status, "found");
  assert.deepEqual(result.pages.map((item) => item.page_number), [2, 3, 4]);
  assert.match(String(result.pages[2].text_normalized), /^Fin del artículo/);
  assert.doesNotMatch(String(result.pages[2].text_normalized), /Artículo 6/);
});

Deno.test("article_lookup: siguiente artículo en página compartida no se incluye", () => {
  const result = selectArticleEvidence([
    page(2, "Art1culo 5º Inicio."),
    page(3, "Último párrafo.\nArticulo 6 Texto del siguiente artículo."),
  ], "5");

  assert.equal(result.status, "found");
  assert.equal(result.pages.length, 2);
  assert.equal(result.pages[1].text_normalized, "Último párrafo.");
});

Deno.test("article_lookup: sin encabezado siguiente conserva hasta fin documental", () => {
  const result = selectArticleEvidence([
    page(7, "Artículo 5 Inicio."),
    page(8, "Continuación y final del documento."),
  ], "5");

  assert.equal(result.status, "found");
  assert.deepEqual(result.pages.map((item) => item.page_number), [7, 8]);
  assert.match(String(result.pages[1].text_normalized), /final del documento/);
});

Deno.test("article_lookup: artículo inexistente falla cerrado", () => {
  const result = selectArticleEvidence([
    page(1, "Artículo 4 Texto."),
    page(2, "Artículo 6 Texto."),
  ], "5");

  assert.deepEqual(result, { status: "not_found", pages: [] });
});

Deno.test("article_lookup: búsqueda backend incluye solicitado y vecinos", () => {
  assert.deepEqual(articleSearchIdentifiers("5"), ["5", "4", "6"]);
});

Deno.test("article_lookup: artículo alfanumérico 5-A usa solo coincidencia exacta", () => {
  const result = selectArticleEvidence([
    page(
      2,
      "Artículo 5-A.- Texto alfanumérico.\nArtículo 5-B.- Texto siguiente.",
    ),
  ], "5a");

  assert.deepEqual(articleSearchIdentifiers("5a"), ["5a", "5-a"]);
  assert.equal(result.status, "found");
  assert.equal(result.match_method, "exact");
  assert.match(String(result.pages[0].text_normalized), /^Artículo 5-A/);
  assert.doesNotMatch(String(result.pages[0].text_normalized), /Artículo 5-B/);
});

Deno.test("OCR contextual: Artículo 4 / -59 / 6 recupera el artículo 5", () => {
  const result = selectArticleEvidence([
    page(
      2,
      "Artículo 4.- Texto anterior.\nArtículo -59.- Texto literal recuperado.\nArtículo 6.- Texto siguiente.",
    ),
  ], "5");

  assert.equal(result.status, "found");
  assert.equal(result.match_method, "ocr_contextual_recovery");
  assert.equal(result.pages[0].article_match_method, "ocr_contextual_recovery");
  assert.match(String(result.pages[0].text_normalized), /^Artículo -59/);
  assert.doesNotMatch(String(result.pages[0].text_normalized), /Artículo 6/);
});

Deno.test("OCR contextual: 58 / 59 / 60 nunca convierte 59 en artículo 5", () => {
  const result = selectArticleEvidence([
    page(
      20,
      "Artículo 58.- Texto.\nArtículo 59.- Texto real.\nArtículo 60.- Texto.",
    ),
  ], "5");

  assert.deepEqual(result, { status: "not_found", pages: [] });
});

Deno.test("OCR contextual: Artículo S se recupera solo entre 4 y 6", () => {
  const result = selectArticleEvidence([
    page(
      2,
      "Artículo 4.- Texto anterior.\nArtículo S.- Texto literal.\nArtículo 6.- Texto siguiente.",
    ),
  ], "5");

  assert.equal(result.status, "found");
  assert.equal(result.match_method, "ocr_contextual_recovery");
  assert.match(String(result.pages[0].text_normalized), /^Artículo S/);
});

Deno.test("OCR contextual: símbolos degradados alrededor de 5 se manejan conservadoramente", () => {
  const recovered = selectArticleEvidence([
    page(
      2,
      "Artículo 4.- Anterior.\nArtículo -5°- Texto literal.\nArtículo 6.- Siguiente.",
    ),
  ], "5");
  const exact = selectArticleEvidence([
    page(2, "Artículo 5* Texto literal.\nArtículo 6.- Siguiente."),
  ], "5");

  assert.equal(recovered.match_method, "ocr_contextual_recovery");
  assert.match(String(recovered.pages[0].text_normalized), /^Artículo -5°-/);
  assert.equal(exact.match_method, "exact");
  assert.match(String(exact.pages[0].text_normalized), /^Artículo 5\*/);
});

Deno.test("OCR contextual: candidato ambiguo sin ambos vecinos falla cerrado", () => {
  for (
    const text of [
      "Artículo -59.- Sin vecinos.",
      "Artículo 4.- Anterior.\nArtículo -59.- Sin siguiente.",
      "Artículo -59.- Sin anterior.\nArtículo 6.- Siguiente.",
    ]
  ) {
    assert.deepEqual(
      selectArticleEvidence([page(2, text)], "5"),
      { status: "not_found", pages: [] },
    );
  }
});

Deno.test("OCR contextual: coincidencia exacta conserva prioridad", () => {
  const result = selectArticleEvidence([
    page(
      2,
      "Artículo 4.- Texto.\nArtículo -59.- Candidato.\nArtículo 6.- Texto.\nArtículo 5.- Coincidencia exacta.\nArtículo 7.- Fin.",
    ),
  ], "5");

  assert.equal(result.status, "found");
  assert.equal(result.match_method, "exact");
  assert.match(String(result.pages[0].text_normalized), /^Artículo 5/);
  assert.doesNotMatch(String(result.pages[0].text_normalized), /Candidato/);
});

Deno.test("regresión LEY-29459: 40 / -59 / 6* recupera texto OCR literal", () => {
  const result = selectArticleEvidence([
    page(
      2,
      "Artículo 40. Definiciones\n...\n: Artículo -59.- De:la Autoridad Nacional de Salud\nTexto del artículo.\nArtículo 6*.- De la clasificación",
      { extraction_method: "ocr_tesseract", ocr_used: true },
    ),
  ], "5");

  assert.equal(result.status, "found");
  assert.equal(result.match_method, "ocr_contextual_recovery");
  assert.match(
    String(result.pages[0].text_normalized),
    /^: Artículo -59\.- De:la Autoridad Nacional de Salud/,
  );
  assert.doesNotMatch(String(result.pages[0].text_normalized), /clasificación/);
  assert.equal(result.pages[0].extraction_method, "ocr_tesseract");
});

Deno.test("article_lookup: límite defensivo no entrega evidencia parcial", () => {
  const pages = Array.from(
    { length: MAX_ARTICLE_EVIDENCE_PAGES + 1 },
    (_, index) =>
      page(
        index + 1,
        index === 0 ? "Artículo 5 Inicio." : `Continuación ${index}.`,
      ),
  );
  const result = selectArticleEvidence(pages, "5");

  assert.deepEqual(result, { status: "limit_exceeded", pages: [] });
});

Deno.test("article_lookup: ventana backend truncada no simula fin documental", () => {
  const result = selectArticleEvidence(
    [page(2, "Artículo 5 Inicio."), page(3, "Continuación.")],
    "5",
    MAX_ARTICLE_EVIDENCE_PAGES,
    true,
  );

  assert.deepEqual(result, { status: "limit_exceeded", pages: [] });
});

function runtime(
  documentPages: ArticlePage[],
): { deps: ConsultaRuntimeDependencies; answerCalls: { value: number } } {
  const answerCalls = { value: 0 };
  const selection = selectArticleEvidence(documentPages, "5");
  const deps: ConsultaRuntimeDependencies = {
    interpretIntent: async (question) => deterministicConsultaIntent(question),
    searchChunks: async () => [],
    suggestSimilarAlerts: async () => [],
    resolverNormaPuntual: async () => ({
      status: "resolved",
      documentKey: "LEY-29459",
    }),
    getNormaConContenido: async () => ({
      norma: {
        document_key: "LEY-29459",
        pdf_url: "https://example.invalid/ley.pdf",
      },
      paginas: selection.pages,
      relaciones: [],
    }),
    paginaComoChunk: (item): ConsultaChunk => ({
      ...item,
      document_key: "LEY-29459",
      detail_url: "https://example.invalid/ley.pdf",
      text_content: item.text_normalized,
    }),
    buildContext: (chunks) =>
      chunks.map((item) => item.text_content).join("\n"),
    sources: (chunks) =>
      chunks.map((item) => ({
        documentKey: item.document_key!,
        url: item.detail_url!,
        page: item.page_number!,
      })),
    formatRelacionesComoContexto: () => "",
    formatRelacionesParaUsuario: () => "",
    construirReporte: () => "reporte",
    deepseek: async () => {
      answerCalls.value++;
      return "Respuesta sustentada";
    },
  };
  return { deps, answerCalls };
}

Deno.test("article_lookup: tabla pendiente ajena al intervalo no bloquea", async () => {
  const { deps, answerCalls } = runtime([
    page(2, "Artículo 5 Texto narrativo seguro."),
    page(3, "Artículo 6.- Tabla ajena.", {
      has_tables: true,
      tabla_verificada: false,
    }),
  ]);
  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459",
    deps,
  );

  assert.equal(result.blocked, false);
  assert.equal(answerCalls.value, 1);
  assert.deepEqual(result.sources.map((source) => source.page), [2]);
});

Deno.test("article_lookup: artículo inexistente no invoca modelo", async () => {
  const { deps, answerCalls } = runtime([
    page(1, "Artículo 4 Texto."),
    page(2, "Artículo 6 Texto."),
  ]);
  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459",
    deps,
  );

  assert.equal(answerCalls.value, 0);
  assert.match(result.answer, /no pude localizar con suficiente confianza/);
  assert.match(result.answer, /No enviaré la norma completa al modelo/);
});

Deno.test("article_lookup: tabla pendiente dentro del intervalo mantiene safety", async () => {
  const { deps, answerCalls } = runtime([
    page(2, "Artículo 4.- Anterior.\nArtículo -59.- Inicio narrativo."),
    page(3, "Tabla relevante del artículo.", {
      has_tables: true,
      tabla_verificada: false,
    }),
    page(4, "Artículo 6.- Texto ajeno."),
  ]);
  const result = await answerConsultaRuntime(
    "Qué valor corresponde según la tabla del artículo 5 de la Ley 29459",
    deps,
  );

  assert.equal(result.blocked, true);
  assert.equal(answerCalls.value, 0);
  assert.deepEqual(result.sources.map((source) => source.page), [3]);
});
