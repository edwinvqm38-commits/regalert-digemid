import assert from "node:assert/strict";
import {
  extraerReferenciaNormativa,
  resolverNormaPuntual,
} from "./norm_identity.ts";
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
    resolverNormaPuntual: async () =>
      exact
        ? { status: "resolved", documentKey: safe.document_key! }
        : { status: "not_a_specific_norm" },
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

Deno.test("norma exacta desaparecida no cae al RPC ni llama modelos", async () => {
  const { deps, calls } = setup([pending], true);
  deps.getNormaConContenido = async () => null;
  const result = await answerConsultaRuntime(
    "¿Qué valor corresponde a C3?",
    deps,
  );
  assert.match(result.answer, /No encontré la norma exacta/);
  assert.equal(calls.search.length, 0);
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

Deno.test("norma exacta con filas pending_ocr vacías no invoca IA", async () => {
  const placeholder: ConsultaChunk = {
    ...safe,
    text_content: "   ",
    extraction_method: "pending_ocr",
    ocr_used: false,
  };
  const { deps, calls } = setup([placeholder], true);

  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459",
    deps,
  );

  assert.match(result.answer, /contenido todavía no fue extraído\/revisado/);
  assert.equal(calls.deepseek + calls.gemini, 0);
  assert.equal(calls.contexts.length, 0);
});

Deno.test("evidencia OCR no revisada añade advertencia determinística", async () => {
  const ocr: ConsultaChunk = {
    ...safe,
    page_number: 7,
    extraction_method: "tesseract_pymupdf_render",
    ocr_used: true,
    revisado_manual: false,
  };
  const { deps, calls } = setup([ocr], true);

  const result = await answerConsultaRuntime("Resume el artículo 5.", deps);

  assert.equal(calls.deepseek, 1);
  assert.match(result.answer, /página\(s\) 7 proviene de OCR/);
  assert.match(result.answer, /aún no fue revisada por una persona/);
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

// Identidades observadas en producción. El contenido es sintético: LEY-29459
// y los dos DS todavía no tienen páginas; ese caso se verifica por separado.
const catalog = [
  { document_key: "LEY-29459", tipo_norma: "Ley", numero: "29459", anio: 2009 },
  { document_key: "LEY-32033", tipo_norma: "Ley", numero: "32033", anio: 2024 },
  {
    document_key: "DS-014-2011-SA",
    tipo_norma: "Decreto Supremo",
    numero: "014-2011-SA",
    anio: 2011,
  },
  {
    document_key: "DS-016-2011-SA",
    tipo_norma: "Decreto Supremo",
    numero: "016-2011-SA",
    anio: 2011,
  },
  {
    document_key: "RM-554-2022-MINSA",
    tipo_norma: "Resolución Ministerial",
    numero: "554-2022-MINSA",
    anio: 2022,
  },
  {
    document_key: "RM-810-2024-MINSA",
    tipo_norma: "Resolución Ministerial",
    numero: "810-2024-MINSA",
    anio: 2024,
  },
  {
    document_key: "NORM-LEY-29459-LEY-DE-LOS-PRODUCTOS-FARMACEUT",
    tipo_norma: "LEY",
    numero: "29459",
    anio: null,
    process_status: "stub_derogada_reconciliada",
    reconciliado_con_document_key: "LEY-29459",
  },
];

function identitySetup(
  rows: Record<string, unknown>[] = catalog,
  pages: ConsultaChunk[] = [safe],
  options: {
    error?: boolean;
    pageSize?: number;
    failSecondPage?: boolean;
    deepseekFails?: boolean;
  } = {},
) {
  const { deps, calls } = setup(
    [{
      ...safe,
      document_key: "OTRO-DOCUMENTO",
      text_content: "MENCION_AJENA_NO_USAR",
    }],
    false,
    options.deepseekFails,
  );
  const reads: string[] = [];
  const loaded: string[] = [];
  let suggestions = 0;
  const db = {
    from(table: string) {
      assert.equal(table, "digemid_normas");
      let number = "";
      const query = {
        select(columns: string, config: { count: string }) {
          assert.equal(config.count, "exact");
          assert.match(columns, /raw->>reconciliado_con_document_key/);
          return query;
        },
        or(filter: string) {
          assert.match(
            filter,
            /^numero\.ilike\.%\d+%,document_key\.ilike\.%\d+%$/,
          );
          number = filter.split("%")[1];
          return query;
        },
        order(column: string) {
          assert.equal(column, "id");
          return query;
        },
        async range(start: number, end: number) {
          reads.push(number);
          if (options.error || (options.failSecondPage && start > 0)) {
            return { error: { message: "simulado" }, data: null, count: null };
          }
          const selected = rows.map((row, i): Record<string, unknown> => ({
            id: String(i),
            ...row,
          }))
            .filter((row) =>
              String(row.numero).includes(number) ||
              String(row.document_key).includes(number)
            );
          return {
            data: selected.slice(
              start,
              Math.min(end + 1, start + (options.pageSize ?? 500)),
            ),
            count: selected.length,
            error: null,
          };
        },
      };
      return query;
    },
  };
  deps.resolverNormaPuntual = (question) => resolverNormaPuntual(db, question);
  deps.getNormaConContenido = async (key) => {
    loaded.push(key);
    return {
      norma: { document_key: key, pdf_url: safe.detail_url },
      paginas: pages.map((page) => ({ ...page, document_key: key })),
      relaciones: [],
    };
  };
  deps.suggestSimilarAlerts = async () => {
    suggestions++;
    return [];
  };
  return { deps, calls, reads, loaded, suggestions: () => suggestions };
}

for (
  const [reference, expected] of [
    ["Ley 29459", "LEY-29459"],
    ["Ley N° 29459", "LEY-29459"],
    ["Ley Nº 29459", "LEY-29459"],
    ["Ley 32033", "LEY-32033"],
    ["DS 014-2011-SA", "DS-014-2011-SA"],
    ["D.S. 014-2011-SA", "DS-014-2011-SA"],
    ["D.S. N° 014-2011-SA", "DS-014-2011-SA"],
    ["RM 554-2022/MINSA", "RM-554-2022-MINSA"],
    ["DS 016-2011-SA", "DS-016-2011-SA"],
    ["RM 810-2024/MINSA", "RM-810-2024-MINSA"],
    ["Ley 29459-2009", "LEY-29459"],
    ["Decreto Supremo 14-2011/SA", "DS-014-2011-SA"],
  ]
) {
  Deno.test(`identidad integrada: ${reference}`, async () => {
    const { deps, calls, loaded } = identitySetup();
    const result = await answerConsultaRuntime(
      `¿Cuál es el objeto de la ${reference}?`,
      deps,
    );
    assert.deepEqual(loaded, [expected]);
    assert.deepEqual(calls.search, []);
    assert.equal(calls.deepseek, 1);
    assert.equal(calls.gemini, 0);
    assert.deepEqual(calls.contexts[0].map((page) => page.document_key), [
      expected,
    ]);
    assert.deepEqual(result.sources.map((source) => source.documentKey), [
      expected,
    ]);
    assert.doesNotMatch(
      JSON.stringify([calls.prompts, result]),
      /MENCION_AJENA/,
    );
    assert.match(result.reportFileName!, new RegExp(expected));
  });
}

Deno.test("parseo conserva cinco dígitos, año ausente y sufijo; no fabrica document_key", () => {
  assert.deepEqual(extraerReferenciaNormativa("Ley Nº 29459").references, [
    { tipo: "LEY", numero: "29459", anio: null, sufijo: null },
  ]);
  assert.deepEqual(
    extraerReferenciaNormativa("D.S. N° 014-2011-SA").references,
    [
      { tipo: "DS", numero: "14", anio: 2011, sufijo: "SA" },
    ],
  );
});

for (
  const [name, question, rows, expected] of [
    ["varios años", "RM 554", [catalog[4], {
      ...catalog[4],
      document_key: "RM-554-2023-MINSA",
      numero: "554-2023-MINSA",
      anio: 2023,
    }], /más de una/],
    ["duplicado no reconciliado", "Ley 29459", [catalog[0], {
      ...catalog[6],
      process_status: "stub_derogada",
    }], /más de una/],
    ["alias sin destino", "Ley 29459", [catalog[6]], /histórico/],
    ["dos documentos aunque uno tenga clave canónica", "Ley 29459", [
      catalog[0],
      { ...catalog[0], document_key: "LEY-29459-2009" },
    ], /más de una/],
    ["tipo incompleto", "Resolución 554", catalog, /Indica una sola norma/],
    [
      "dos normas en la pregunta",
      "Compara Ley 29459 y Ley 32033",
      catalog,
      /Indica una sola norma/,
    ],
    ["año mal formado", "DS 014-201", catalog, /Indica una sola norma/],
    ["no encontrada", "Ley 99999", catalog, /No encontré la norma exacta/],
    [
      "sin coincidencia parcial de número",
      "Ley 2945",
      catalog,
      /No encontré la norma exacta/,
    ],
    [
      "tipo distinto",
      "RD 554-2022/MINSA",
      catalog,
      /No encontré la norma exacta/,
    ],
    [
      "año distinto",
      "Ley 29459-2010",
      [catalog[0]],
      /No encontré la norma exacta/,
    ],
    [
      "sector distinto",
      "RM 554-2022/MEF",
      catalog,
      /No encontré la norma exacta/,
    ],
    ["sector desconocido legacy", "DS 20-2024-SA", [{
      document_key: "DS-20-2024",
      tipo_norma: "DS",
      numero: "20",
      anio: 2024,
    }], /incompleta/],
    ["campos contradictorios", "DS 14-2011-SA", [{
      ...catalog[2],
      numero: "014-2012-SA",
    }], /incompleta/],
  ] as const
) {
  Deno.test(`identidad cerrada: ${name}`, async () => {
    const { deps, calls, loaded, suggestions } = identitySetup([...rows]);
    const result = await answerConsultaRuntime(question, deps);
    assert.match(result.answer, expected);
    assert.deepEqual(calls.search, []);
    assert.deepEqual(loaded, []);
    assert.equal(suggestions(), 0);
    assert.equal(calls.deepseek + calls.gemini, 0);
    assert.deepEqual(result.sources, []);
    assert.equal(result.reportHtml, undefined);
  });
}

Deno.test("identidad: paginación no oculta una segunda candidata", async () => {
  const { deps, calls, reads } = identitySetup(
    [
      catalog[0],
      { ...catalog[0], document_key: "LEY-29459-2009" },
    ],
    [safe],
    { pageSize: 1 },
  );
  const result = await answerConsultaRuntime("Ley 29459", deps);
  assert.equal(reads.length, 2);
  assert.match(result.answer, /más de una/);
  assert.equal(calls.search.length + calls.deepseek + calls.gemini, 0);
});

for (
  const options of [{ error: true }, { pageSize: 1, failSecondPage: true }]
) {
  Deno.test(`identidad: error técnico separado ${JSON.stringify(options)}`, async () => {
    const { deps, calls, loaded } = identitySetup(catalog, [safe], options);
    const result = await answerConsultaRuntime("Ley 29459", deps);
    assert.match(result.answer, /problema técnico/);
    assert.doesNotMatch(result.answer, /No encontré|más de una/);
    assert.deepEqual(loaded, []);
    assert.equal(calls.search.length + calls.deepseek + calls.gemini, 0);
  });
}

Deno.test("identidad integrada: pregunta general mantiene búsqueda", async () => {
  const { deps, calls, reads, loaded } = identitySetup();
  await answerConsultaRuntime(
    "¿Qué disposiciones hay sobre medicamentos genéricos?",
    deps,
  );
  assert.equal(calls.search.length, 1);
  assert.equal(calls.deepseek, 1);
  assert.deepEqual(reads, []);
  assert.deepEqual(loaded, []);
});

Deno.test("LEY-29459 sin páginas: identidad resuelta no sustituye contenido", async () => {
  const { deps, calls, loaded } = identitySetup(catalog, []);
  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459.",
    deps,
  );
  assert.deepEqual(loaded, ["LEY-29459"]);
  assert.match(result.answer, /contenido todavía no fue/);
  assert.equal(result.sources[0].documentKey, "LEY-29459");
  assert.equal(calls.search.length + calls.deepseek + calls.gemini, 0);
});

Deno.test("identidad exacta no evita gate de tabla insegura", async () => {
  const { deps, calls, loaded } = identitySetup(catalog, [pending]);
  const result = await answerConsultaRuntime(
    "¿Qué multa corresponde a C3 de la Ley 29459?",
    deps,
  );
  assert.deepEqual(loaded, ["LEY-29459"]);
  assert.equal(result.blocked, true);
  assert.equal(result.sources[0].documentKey, "LEY-29459");
  assert.equal(calls.search.length + calls.deepseek + calls.gemini, 0);
  assert.equal(calls.contexts.length + calls.reports.length, 0);
  assert.doesNotMatch(
    JSON.stringify(result),
    /CONTENIDO_PENDIENTE|MENCION_AJENA/,
  );
});

Deno.test("identidad real: mezcla insegura con cita explícita mantiene el bloqueo conservador de C-01", async () => {
  const { deps, calls } = identitySetup(catalog, [pending, safe], {
    deepseekFails: true,
  });
  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459.",
    deps,
  );
  assert.equal(result.blocked, true);
  assert.equal(calls.deepseek + calls.gemini, 0);
  assert.equal(calls.search.length, 0);
  assert.equal(calls.contexts.length + calls.reports.length, 0);
  assert.equal(result.reportHtml, undefined);
  assert.doesNotMatch(
    JSON.stringify([calls.prompts, calls.contexts, result]),
    /CONTENIDO_PENDIENTE|MENCION_AJENA/,
  );
});

Deno.test("identidad exacta segura conserva la misma norma en fallback Gemini", async () => {
  const { deps, calls } = identitySetup(catalog, [safe], {
    deepseekFails: true,
  });
  const result = await answerConsultaRuntime(
    "Resume el artículo 5 de la Ley 29459.",
    deps,
  );
  assert.equal(calls.deepseek, 1);
  assert.equal(calls.gemini, 1);
  assert.equal(calls.search.length, 0);
  assert.equal(calls.prompts[0], calls.prompts[1]);
  assert.deepEqual(result.sources.map((s) => s.documentKey), ["LEY-29459"]);
  assert.doesNotMatch(JSON.stringify([calls.prompts, result]), /MENCION_AJENA/);
});

Deno.test("clave contradictoria no desaparece al evaluar duplicados", async () => {
  const { deps, calls } = identitySetup([
    catalog[2],
    { ...catalog[2], document_key: "DS-14-2011-SA", numero: "15-2011-SA" },
  ]);
  const result = await answerConsultaRuntime("DS 014-2011-SA", deps);
  assert.match(result.answer, /más de una/);
  assert.equal(calls.search.length + calls.deepseek + calls.gemini, 0);
});

for (const failure of ["error", "mismatch"] as const) {
  Deno.test(`carga exacta ${failure}: nunca permite búsqueda general`, async () => {
    const { deps, calls } = identitySetup();
    deps.getNormaConContenido = async () => {
      if (failure === "error") throw new Error("error simulado");
      return {
        norma: { document_key: "OTRA-NORMA" },
        paginas: [safe],
        relaciones: [],
      };
    };
    const result = await answerConsultaRuntime("Ley 29459", deps);
    assert.match(result.answer, /problema técnico/);
    assert.equal(calls.search.length + calls.deepseek + calls.gemini, 0);
    assert.deepEqual(result.sources, []);
  });
}
