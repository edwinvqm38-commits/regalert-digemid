import assert from "node:assert/strict";
import {
  type ConsultaIntent,
  deterministicConsultaIntent,
  interpretConsultaIntent,
} from "./consulta_intent.ts";
import {
  answerConsultaRuntime,
  type ConsultaChunk,
  type ConsultaRuntimeDependencies,
} from "./consulta_runtime.ts";

const cases: [string, ConsultaIntent["intent"], Partial<ConsultaIntent>][] = [
  [
    "Resume el artículo 5 de la Ley 29459",
    "article_lookup",
    { document_key: "LEY-29459", article: "5" },
  ],
  [
    "Explícame el artículo cinco de la Ley 29459",
    "article_lookup",
    { document_key: "LEY-29459", article: "5" },
  ],
  [
    "Qué dice la Ley 29459 sobre productos sanitarios",
    "regulatory_question",
    { document_key: "LEY-29459" },
  ],
  [
    "Qué obligaciones tiene una botica según la Ley 29459",
    "botica_impact",
    { document_key: "LEY-29459" },
  ],
  [
    "Hay una alerta del producto Opdivo",
    "product_lookup",
    { product: "Opdivo" },
  ],
  ["Qué pasó con el lote ABC123", "product_lookup", { lot: "ABC123" }],
  ["Qué norma habla de almacenamiento", "regulatory_question", {}],
];

for (const [question, expectedIntent, expectedFields] of cases) {
  Deno.test(`parser determinístico: ${question}`, () => {
    const result = deterministicConsultaIntent(question);
    assert.equal(result.intent, expectedIntent);
    for (const [key, value] of Object.entries(expectedFields)) {
      assert.equal(result[key as keyof ConsultaIntent], value);
    }
    assert.ok(result.search_query);
  });
}

for (
  const question of [
    "qué valor corresponde según la tabla",
    "qué sanción corresponde según la fila",
    "compara las columnas",
  ]
) {
  Deno.test(`consulta tabular se marca estructurada: ${question}`, () => {
    const result = deterministicConsultaIntent(question);
    assert.equal(result.intent, "table_lookup");
    assert.equal(result.needs_structured_table, true);
  });
}

function providerJson(overrides: Partial<ConsultaIntent> = {}): string {
  return JSON.stringify({
    intent: "article_lookup",
    document_key: "LEY-29459",
    article: "5",
    alert_number: null,
    product: null,
    lot: null,
    search_query: "artículo 5 LEY-29459",
    needs_structured_table: false,
    ...overrides,
  });
}

function deepSeekResponse(content: string): Response {
  return Response.json({ choices: [{ message: { content } }] });
}

function geminiResponse(content: string): Response {
  return Response.json({
    candidates: [{ content: { parts: [{ text: content }] } }],
  });
}

Deno.test("DeepSeek es principal, recibe solo la pregunta y usa temperatura cero", async () => {
  const calls: { url: string; body: any }[] = [];
  const result = await interpretConsultaIntent(
    "Resume el artículo 5 de la Ley 29459",
    {
      deepseekApiKey: "deep-key",
      geminiApiKey: "gemini-key",
      fetchImpl: (async (input, init) => {
        calls.push({
          url: String(input),
          body: JSON.parse(String(init?.body)),
        });
        return deepSeekResponse(providerJson());
      }) as typeof fetch,
    },
  );
  assert.equal(result.intent, "article_lookup");
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /deepseek/);
  assert.equal(calls[0].body.temperature, 0);
  assert.equal(
    calls[0].body.messages[1].content,
    "Resume el artículo 5 de la Ley 29459",
  );
  assert.doesNotMatch(
    JSON.stringify(calls[0].body),
    /text_content|pdf_url|paginas/,
  );
});

Deno.test("JSON inválido de DeepSeek usa Gemini", async () => {
  const urls: string[] = [];
  const result = await interpretConsultaIntent(
    "Hay una alerta del producto Opdivo",
    {
      deepseekApiKey: "deep-key",
      geminiApiKey: "gemini-key",
      fetchImpl: (async (input) => {
        const url = String(input);
        urls.push(url);
        return url.includes("deepseek")
          ? deepSeekResponse("esto no es JSON")
          : geminiResponse(providerJson({
            intent: "product_lookup",
            document_key: null,
            article: null,
            product: "Opdivo",
            search_query: "Opdivo alerta",
          }));
      }) as typeof fetch,
    },
  );
  assert.deepEqual(urls.map((url) => url.includes("deepseek")), [true, false]);
  assert.equal(result.intent, "product_lookup");
  assert.equal(result.product, "Opdivo");
});

Deno.test("timeout de DeepSeek usa Gemini", async () => {
  let calls = 0;
  const result = await interpretConsultaIntent(
    "Qué norma habla de almacenamiento",
    {
      deepseekApiKey: "deep-key",
      geminiApiKey: "gemini-key",
      timeoutMs: 10,
      fetchImpl: (async (input) => {
        calls++;
        if (String(input).includes("deepseek")) {
          throw new DOMException("timeout", "AbortError");
        }
        return geminiResponse(providerJson({
          intent: "regulatory_question",
          document_key: null,
          article: null,
          search_query: "norma almacenamiento",
        }));
      }) as typeof fetch,
    },
  );
  assert.equal(calls, 2);
  assert.equal(result.intent, "regulatory_question");
});

Deno.test("DeepSeek no configurado va directamente a Gemini", async () => {
  const urls: string[] = [];
  const result = await interpretConsultaIntent("Qué pasó con el lote ABC123", {
    geminiApiKey: "gemini-key",
    fetchImpl: (async (input) => {
      urls.push(String(input));
      return geminiResponse(providerJson({
        intent: "product_lookup",
        document_key: null,
        article: null,
        lot: "ABC123",
        search_query: "lote ABC123",
      }));
    }) as typeof fetch,
  });
  assert.equal(urls.length, 1);
  assert.match(urls[0], /generativelanguage/);
  assert.equal(result.lot, "ABC123");
});

Deno.test("proveedores inválidos terminan en parser determinístico", async () => {
  const result = await interpretConsultaIntent(
    "Explícame el artículo cinco de la Ley 29459",
    {
      deepseekApiKey: "deep-key",
      geminiApiKey: "gemini-key",
      fetchImpl:
        (async (input) =>
          String(input).includes("deepseek")
            ? deepSeekResponse("{")
            : geminiResponse("[]")) as typeof fetch,
    },
  );
  assert.equal(result.intent, "article_lookup");
  assert.equal(result.article, "5");
  assert.equal(result.document_key, "LEY-29459");
});

const safeArticle: ConsultaChunk = {
  document_key: "LEY-29459",
  page_number: 3,
  detail_url: "https://example.invalid/ley.pdf",
  has_tables: false,
  text_content: "Artículo 5. Los productos sanitarios se rigen por esta Ley.",
};
const unrelatedPendingTable: ConsultaChunk = {
  ...safeArticle,
  page_number: 18,
  has_tables: true,
  tabla_verificada: false,
  text_content: "TABLA_PENDIENTE_AJENA",
};

function runtimeDeps(): {
  deps: ConsultaRuntimeDependencies;
  calls: { answer: number; loadedIntent: ConsultaIntent | undefined };
} {
  const calls = {
    answer: 0,
    loadedIntent: undefined as ConsultaIntent | undefined,
  };
  return {
    calls,
    deps: {
      interpretIntent: async (question) =>
        deterministicConsultaIntent(question),
      searchChunks: async () => [unrelatedPendingTable],
      suggestSimilarAlerts: async () => [],
      resolverNormaPuntual: async () => ({
        status: "resolved",
        documentKey: "LEY-29459",
      }),
      getNormaConContenido: async (_key, intent) => {
        calls.loadedIntent = intent;
        return {
          norma: {
            document_key: "LEY-29459",
            pdf_url: safeArticle.detail_url,
          },
          // Simula la selección backend: la página tabular ajena no se recupera.
          paginas: intent?.intent === "article_lookup"
            ? [safeArticle]
            : [safeArticle, unrelatedPendingTable],
          relaciones: [],
        };
      },
      paginaComoChunk: (page) => page,
      buildContext: (chunks) =>
        chunks.map((chunk) => chunk.text_content).join("\n"),
      sources: (chunks) =>
        chunks.map((chunk) => ({
          documentKey: chunk.document_key!,
          url: chunk.detail_url!,
          page: chunk.page_number!,
        })),
      formatRelacionesComoContexto: () => "",
      formatRelacionesParaUsuario: () => "",
      construirReporte: () => "reporte",
      deepseek: async (prompt) => {
        calls.answer++;
        assert.doesNotMatch(prompt, /TABLA_PENDIENTE_AJENA/);
        return "Respuesta sustentada";
      },
    },
  };
}

Deno.test("regresión LEY-29459: artículo 5 no se bloquea por tabla de otra página", async () => {
  const { deps, calls } = runtimeDeps();
  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459",
    deps,
  );
  assert.equal(calls.loadedIntent?.intent, "article_lookup");
  assert.equal(calls.loadedIntent?.article, "5");
  assert.equal(calls.answer, 1);
  assert.equal(result.blocked, false);
  assert.deepEqual(result.sources.map((source) => source.page), [3]);
});

for (
  const question of [
    "qué valor corresponde según la tabla",
    "qué sanción corresponde según la fila",
    "compara las columnas",
  ]
) {
  Deno.test(`gate sigue bloqueando intención tabular: ${question}`, async () => {
    const { deps, calls } = runtimeDeps();
    deps.resolverNormaPuntual = async () => ({ status: "not_a_specific_norm" });
    deps.searchChunks = async () => [unrelatedPendingTable];
    const result = await answerConsultaRuntime(question, deps);
    assert.equal(result.blocked, true);
    assert.equal(calls.answer, 0);
  });
}
