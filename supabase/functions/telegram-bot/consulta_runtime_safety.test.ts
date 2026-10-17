import assert from "node:assert/strict";
import {
  answerConsultaRuntime,
  type ConsultaChunk,
  type ConsultaRuntimeDependencies,
  hydrateConsultaChunks,
} from "./consulta_runtime.ts";
import {
  ORDINAL_COLUMN_IDENTITY_METHOD,
  TABLE_VERIFICATION_METHOD,
} from "./revision_safety.ts";

const safe: ConsultaChunk = {
  document_key: "DS-20-2024",
  page_number: 1,
  detail_url: "https://example.invalid/oficial.pdf",
  has_tables: false,
  text_content: "El objeto de la norma es establecer disposiciones sanitarias.",
};
const pending: ConsultaChunk = {
  ...safe,
  page_number: 5,
  has_tables: true,
  text_content: "CONTENIDO_PENDIENTE_NO_ENVIAR",
  tabla_verificada: false,
};

function setup(chunks: ConsultaChunk[], exact = false, deepseekFails = false) {
  const calls = {
    deepseek: 0,
    gemini: 0,
    search: [] as string[],
    prompts: [] as string[],
    contexts: [] as ConsultaChunk[][],
    reports: [] as any[][],
  };
  const deps: ConsultaRuntimeDependencies = {
    searchChunks: async (question) => {
      calls.search.push(question);
      return chunks;
    },
    suggestSimilarAlerts: async () => [],
    extraerReferenciaNormativa: () =>
      exact ? { documentKey: safe.document_key! } : null,
    getNormaConContenido: async () => ({
      norma: { document_key: safe.document_key, pdf_url: safe.detail_url },
      paginas: chunks,
      relaciones: [{ text: "RELACION_SIN_PAGINA_NO_REINTRODUCIR" }],
    }),
    paginaComoChunk: (page) => page,
    buildContext: (permitted) => {
      calls.contexts.push(permitted);
      return permitted.map((page) => page.text_content).join("\n");
    },
    sources: (pages) =>
      pages.map((page) => ({
        documentKey: page.document_key!,
        url: page.detail_url!,
        page: page.page_number!,
      })),
    formatRelacionesComoContexto: (relations) =>
      relations.map((r) => r.text).join("\n"),
    formatRelacionesParaUsuario: (relations) =>
      relations.map((r) => r.text).join("\n"),
    construirReporte: (_norma, pages, relations, answer) => {
      calls.reports.push(pages);
      return JSON.stringify({ pages, relations, answer });
    },
    deepseek: async (prompt) => {
      calls.deepseek++;
      calls.prompts.push(prompt);
      if (deepseekFails) throw new Error("fallo simulado");
      return "Respuesta sustentada";
    },
    gemini: async (prompt) => {
      calls.gemini++;
      calls.prompts.push(prompt);
      return "Respuesta de respaldo";
    },
  };
  return { deps, calls };
}

// Se prueba la misma orquestación que invoca answerConsulta() en index.ts.
// Ningún permiso de red/env: todos los proveedores y lecturas son dobles.
for (const exact of [false, true]) {
  const route = exact ? "norma puntual" : "RPC";
  for (
    const [name, question] of [
      [
        "A: asociación multa-infracción",
        "¿Qué multa corresponde a la infracción 5?",
      ],
      ["B: columna ordinal", "¿Qué valor corresponde a C3?"],
      ["sanción-categoría", "¿Cuál es la sanción asociada a esta categoría?"],
      ["obligación-fila", "¿Qué obligación corresponde a la fila 4?"],
      [
        "plazo-supuesto",
        "Según la tabla, ¿qué plazo corresponde a este supuesto?",
      ],
      ["celda-infracción", "¿Qué dice C3 para esta infracción?"],
    ]
  ) {
    Deno.test(`${route} / ${name}: bloquea antes de cualquier proveedor`, async () => {
      const { deps, calls } = setup([pending], exact);
      const result = await answerConsultaRuntime(question, deps);
      assert.equal(result.blocked, true);
      assert.match(result.answer, /NO CONFIRMADO AUTOMÁTICAMENTE/);
      assert.equal(calls.deepseek, 0);
      assert.equal(calls.gemini, 0);
      assert.equal(calls.contexts.length, 0);
      assert.equal(calls.reports.length, 0);
      assert.equal(result.sources[0].page, 5);
    });
  }

  Deno.test(`${route} / C: evidencia segura llama exactamente una vez`, async () => {
    const { deps, calls } = setup([safe], exact);
    const question = "¿Cuál es el objeto de la norma?";
    const result = await answerConsultaRuntime(question, deps);
    assert.equal(result.blocked, false);
    assert.equal(calls.deepseek, 1);
    assert.equal(calls.gemini, 0);
    assert.equal(result.answer.startsWith("Respuesta sustentada"), true);
    if (!exact) assert.deepEqual(calls.search, [question]);
  });

  Deno.test(`${route} / D: mezcla narrativa excluye página insegura de contexto, fuentes y reporte`, async () => {
    const { deps, calls } = setup([pending, safe], exact);
    const result = await answerConsultaRuntime(
      "¿Cuál es el objeto de la norma?",
      deps,
    );
    assert.equal(result.blocked, false);
    assert.equal(calls.deepseek, 1);
    assert.equal(calls.gemini, 0);
    assert.deepEqual(calls.contexts[0].map((page) => page.page_number), [1]);
    assert.equal(calls.contexts[0][0].text_content, safe.text_content);
    assert.doesNotMatch(
      calls.prompts[0],
      /CONTENIDO_PENDIENTE|RELACION_SIN_PAGINA/,
    );
    assert.deepEqual(result.sources.map((source) => source.page), [1]);
    if (exact) {
      assert.deepEqual(calls.reports[0].map((page) => page.page_number), [1]);
      assert.doesNotMatch(
        result.reportHtml!,
        /CONTENIDO_PENDIENTE|RELACION_SIN_PAGINA/,
      );
    }
  });

  Deno.test(`${route} / E: DeepSeek fallaría y Gemini está disponible; bloqueo impide ambos`, async () => {
    const { deps, calls } = setup([pending], exact, true);
    const result = await answerConsultaRuntime(
      "¿Qué valor corresponde a C3?",
      deps,
    );
    assert.equal(result.blocked, true);
    assert.equal(calls.deepseek, 0);
    assert.equal(calls.gemini, 0);
  });

  Deno.test(`${route}: fallback permitido conserva exactamente el contexto filtrado`, async () => {
    const { deps, calls } = setup([pending, safe], exact, true);
    const result = await answerConsultaRuntime("Resume el artículo 5.", deps);
    assert.equal(result.blocked, false);
    assert.equal(calls.deepseek, 1);
    assert.equal(calls.gemini, 1);
    assert.equal(calls.prompts[0], calls.prompts[1]);
    assert.doesNotMatch(
      calls.prompts[1],
      /CONTENIDO_PENDIENTE|RELACION_SIN_PAGINA/,
    );
  });

  Deno.test(`${route}: narrativa con única página pendiente también bloquea`, async () => {
    const { deps, calls } = setup([pending], exact);
    const result = await answerConsultaRuntime(
      "¿Qué dice literalmente el numeral 3?",
      deps,
    );
    assert.equal(result.blocked, true);
    assert.equal(calls.deepseek + calls.gemini, 0);
  });

  Deno.test(`${route}: metadata pendiente prevalece sobre flags legacy`, async () => {
    const { deps, calls } = setup([{
      ...safe,
      tabla_verificada: true,
      revisado_manual: true,
      metadata: { possible_table: true, table_requires_review: true },
    }], exact);
    const result = await answerConsultaRuntime(
      "¿Qué valor corresponde a C3?",
      deps,
    );
    assert.equal(result.blocked, true);
    assert.equal(calls.deepseek + calls.gemini, 0);
  });
}

Deno.test("ausencia de flags de tabla falla cerrada, sin inventar false", async () => {
  const { deps, calls } = setup([{ ...safe, has_tables: undefined }]);
  const result = await answerConsultaRuntime(
    "¿Cuál es el objeto de la norma?",
    deps,
  );
  assert.equal(result.blocked, true);
  assert.equal(calls.deepseek + calls.gemini, 0);
});

function database(
  page: any,
  options: { alert?: boolean; error?: boolean } = {},
) {
  const reads: { table: string; filters: Record<string, unknown> }[] = [];
  return {
    reads,
    from(table: string) {
      const filters: Record<string, unknown> = {};
      const query = {
        select: (_columns: string) => query,
        eq: (key: string, value: unknown) => {
          filters[key] = value;
          return query;
        },
        async maybeSingle() {
          reads.push({ table, filters });
          if (options.error) {
            return { data: null, error: { message: "error simulado" } };
          }
          if (table === "digemid_normas") {
            return { data: options.alert ? null : { id: "norma-1" } };
          }
          if (table === "digemid_documentos") {
            return { data: { id: "alerta-1" } };
          }
          return { data: page };
        },
      };
      return query;
    },
  };
}

Deno.test("RPC sin metadata: recupera pendiente por document_key y página antes del modelo", async () => {
  const db = database({
    page_number: 1,
    has_tables: false,
    text_normalized: safe.text_content,
    metadata: {
      possible_table: true,
      table_requires_review: true,
      table_quality_score: 0.2,
    },
  });
  const { deps, calls } = setup([]);
  deps.searchChunks = () => hydrateConsultaChunks(db, [safe]);
  const result = await answerConsultaRuntime(
    "¿Qué multa corresponde a la infracción 5?",
    deps,
  );
  assert.equal(result.blocked, true);
  assert.equal(calls.deepseek + calls.gemini, 0);
  assert.deepEqual(db.reads, [
    { table: "digemid_normas", filters: { document_key: safe.document_key } },
    {
      table: "digemid_norma_paginas",
      filters: { norma_id: "norma-1", page_number: 1 },
    },
  ]);
});

for (
  const [name, page, options] of [
    ["página desaparecida", null, {}],
    ["error de lectura", null, { error: true }],
    ["texto cambió entre RPC y lectura", {
      has_tables: false,
      text_normalized: "otra versión",
    }, {}],
    ["alerta legacy sin señales", { text_content: safe.text_content }, {
      alert: true,
    }],
    ["alerta con flags contradictorios", {
      text_content: safe.text_content,
      contiene_tablas: false,
      has_table: true,
    }, { alert: true }],
  ] as const
) {
  Deno.test(`hidratación fail-closed: ${name}`, async () => {
    const { deps, calls } = setup([]);
    deps.searchChunks = () =>
      hydrateConsultaChunks(database(page, options), [safe]);
    const result = await answerConsultaRuntime(
      "¿Qué valor corresponde a C3?",
      deps,
    );
    assert.equal(result.blocked, true);
    assert.equal(calls.deepseek + calls.gemini, 0);
  });
}

Deno.test("hidratación conserva texto y flags de página narrativa segura", async () => {
  const db = database({
    has_tables: false,
    text_normalized: safe.text_content,
    metadata: {},
  });
  const { deps, calls } = setup([]);
  deps.searchChunks = () => hydrateConsultaChunks(db, [safe]);
  const result = await answerConsultaRuntime(
    "¿Cuál es el objeto de la norma?",
    deps,
  );
  assert.equal(result.blocked, false);
  assert.equal(calls.deepseek, 1);
  assert.equal(calls.contexts[0][0].text_content, safe.text_content);
});

Deno.test("hidratación nunca borra una señal pendiente del RPC con false de la página", async () => {
  const db = database({
    has_tables: false,
    text_normalized: safe.text_content,
    possible_table: false,
    table_requires_review: false,
    metadata: {},
  });
  const { deps, calls } = setup([]);
  deps.searchChunks = () =>
    hydrateConsultaChunks(db, [{
      ...safe,
      metadata: { table_requires_review: true },
    }]);
  const result = await answerConsultaRuntime(
    "¿Qué valor corresponde a C3?",
    deps,
  );
  assert.equal(result.blocked, true);
  assert.equal(calls.deepseek + calls.gemini, 0);
});

Deno.test("tabla con verificación moderna completa sigue habilitada tras hidratación", async () => {
  const sha = "a".repeat(64);
  const table = "| Categoría | Valor |\n| --- | --- |\n| A | 1 |";
  const metadata = {
    source_pdf_sha256: sha,
    tables_markdown: table,
    table_regions: {
      detected_regions: 1,
      accepted_regions: 1,
      unresolved_regions: 0,
    },
    manual_review: {
      table_verified: true,
      table_confirmation_requested: true,
      table_structure_present: true,
      table_verification: {
        confirmation_explicit: true,
        verification_method: TABLE_VERIFICATION_METHOD,
        verified_at: "2026-09-01T00:00:00.000Z",
        source_pdf_sha256: sha,
        ordinal_columns: {
          column_identity: ORDINAL_COLUMN_IDENTITY_METHOD,
          editable_table_count: 1,
          invalid_block_count: 0,
          cardinality_preserved: true,
          source_headers_matched: true,
          synthetic_headers_present: false,
          empty_headers_present: false,
          duplicate_header_ambiguity: false,
          duplicate_headers: [],
        },
        region_coverage: {
          detected_regions: 1,
          verified_regions: 1,
          unresolved_regions: 0,
          coverage_unknown: false,
        },
      },
    },
  };
  const db = database({
    has_tables: true,
    text_normalized: table,
    tabla_verificada: true,
    revisado_manual: true,
    metadata,
  });
  const { deps, calls } = setup([]);
  deps.searchChunks = () =>
    hydrateConsultaChunks(db, [{
      ...safe,
      has_tables: true,
      text_content: table,
    }]);
  const result = await answerConsultaRuntime(
    "¿Qué valor corresponde a C2?",
    deps,
  );
  assert.equal(result.blocked, false);
  assert.equal(calls.deepseek, 1);
  assert.equal(calls.gemini, 0);
  assert.deepEqual(calls.contexts[0][0].metadata, metadata);
});

Deno.test("norma exacta inexistente cae al RPC y conserva el bloqueo", async () => {
  const { deps, calls } = setup([pending], true);
  deps.getNormaConContenido = async () => null;
  const result = await answerConsultaRuntime(
    "¿Qué valor corresponde a C3?",
    deps,
  );
  assert.equal(result.blocked, true);
  assert.equal(calls.search.length, 1);
  assert.equal(calls.deepseek + calls.gemini, 0);
});

Deno.test("Gemini como único proveedor sigue detrás del gate", async () => {
  const { deps, calls } = setup([pending]);
  deps.deepseek = undefined;
  const result = await answerConsultaRuntime(
    "¿Qué valor corresponde a C3?",
    deps,
  );
  assert.equal(result.blocked, true);
  assert.equal(calls.gemini, 0);
});

Deno.test("sin evidencia devuelve sugerencias sin IA", async () => {
  const { deps, calls } = setup([]);
  deps.suggestSimilarAlerts = async () => [{
    document_key: "ALERTA",
    detail_url: "https://example.invalid",
  }];
  const result = await answerConsultaRuntime("consulta sin resultados", deps);
  assert.match(result.answer, /Quizás/);
  assert.equal(result.sources[0].documentKey, "ALERTA");
  assert.equal(calls.deepseek + calls.gemini, 0);
});

Deno.test("log de bloqueo solo contiene motivo e identidad de página", async () => {
  const originalInfo = console.info;
  const logs: unknown[][] = [];
  console.info = (...args: unknown[]) => {
    logs.push(args);
  };
  try {
    const { deps } = setup([pending]);
    await answerConsultaRuntime(
      "¿Qué multa corresponde a la infracción 5?",
      deps,
    );
    assert.deepEqual(logs, [["CONSULTA_SAFETY_BLOCK", {
      reason: "unverified_table_structure",
      document_key: pending.document_key,
      page_number: 5,
    }]]);
  } finally {
    console.info = originalInfo;
  }
});
