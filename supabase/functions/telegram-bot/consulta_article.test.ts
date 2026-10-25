import assert from "node:assert/strict";
import {
  type ArticlePage,
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
    page(2, "Artículo 5 Inicio narrativo."),
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
