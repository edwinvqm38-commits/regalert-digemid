/** Trazabilidad de la consulta IA (caso 18 de la lista de pruebas del MVP).
 *
 * Lo critico aqui no es que el modelo "responda bien" (eso no se puede
 * probar sin llamarlo), sino que el canal no pueda presentar una respuesta
 * sin sustento como si lo tuviera: que el prompt exija citar documento,
 * fecha y pagina; que las advertencias de confiabilidad de la extraccion
 * lleguen al modelo; y que la ausencia de evidencia quede marcada.
 *
 * Supabase y fetch se reemplazan por dobles: no hay red ni credenciales.
 */

import { assert, assertEquals, assertStringIncludes } from "jsr:@std/assert@1";
import {
  advertenciasDelBloque,
  answerConsulta,
  buildConsultaContext,
  construirSystemPrompt,
  consultaSources,
  extraerReferenciaNormativa,
  pareceConsultaSobreUltimaNorma,
  preguntaPideVerificacionVisual,
} from "./consulta-ia.ts";

const CHUNK_VERIFICADO = {
  document_key: "DS-20-2024",
  title: "Decreto Supremo N° 020-2024-SA",
  published_date: "2024-10-25",
  page_number: 5,
  text_content: "Infracción 66: 0.5 UIT para farmacia o botica.",
  detail_url: "https://www.digemid.minsa.gob.pe/ds-20-2024",
  quality_score: 0.95,
  revisado_manual: true,
  has_tables: false,
  posible_formula: false,
  estado_vigencia: "vigente",
};

function fakeSupabase(opciones: {
  chunks?: unknown[];
  sugerencias?: unknown[];
  normativaReciente?: unknown[];
  // deno-lint-ignore no-explicit-any
  norma?: any;
  // deno-lint-ignore no-explicit-any
  paginasNorma?: any[];
  // deno-lint-ignore no-explicit-any
  relacionesNorma?: any[];
} = {}) {
  return {
    rpc(nombre: string) {
      if (nombre === "buscar_paginas_texto") {
        return Promise.resolve({ data: opciones.chunks ?? [], error: null });
      }
      if (nombre === "sugerir_alertas_similares") {
        return Promise.resolve({ data: opciones.sugerencias ?? [], error: null });
      }
      return Promise.resolve({ data: [], error: null });
    },
    from(tabla: string) {
      if (tabla === "digemid_normas") {
        return {
          select() {
            return {
              eq() {
                return {
                  maybeSingle() {
                    return Promise.resolve({ data: opciones.norma ?? null, error: null });
                  },
                };
              },
            };
          },
        };
      }

      if (tabla === "digemid_norma_paginas") {
        return {
          select() {
            return {
              eq() {
                return {
                  order() {
                    return {
                      limit() {
                        return Promise.resolve({ data: opciones.paginasNorma ?? [], error: null });
                      },
                    };
                  },
                };
              },
            };
          },
        };
      }

      if (tabla === "digemid_norma_relaciones") {
        return {
          select() {
            return {
              or() {
                return Promise.resolve({ data: opciones.relacionesNorma ?? [], error: null });
              },
            };
          },
        };
      }

      return {
        select() {
          return {
            eq() {
              return {
                order() {
                  return {
                    limit() {
                      return Promise.resolve({ data: opciones.normativaReciente ?? [], error: null });
                    },
                  };
                },
              };
            },
          };
        },
      };
    },
  };
}

function fakeLlm(respuesta: string) {
  const original = globalThis.fetch;
  const cuerposEnviados: string[] = [];

  globalThis.fetch = ((_url: string | URL | Request, init?: RequestInit) => {
    cuerposEnviados.push(String(init?.body ?? ""));
    return Promise.resolve(
      new Response(
        JSON.stringify({ choices: [{ message: { content: respuesta } }] }),
        { status: 200 },
      ),
    );
  }) as typeof fetch;

  return { cuerposEnviados, restaurar: () => { globalThis.fetch = original; } };
}

const CONFIG = {
  deepseekApiKey: "clave-de-prueba",
  geminiApiKey: "",
  geminiModel: "gemini-flash-latest",
  estiloNegrita: "whatsapp" as const,
};

Deno.test("el prompt obliga a citar documento, fecha y página", () => {
  const prompt = construirSystemPrompt("whatsapp");

  assertStringIncludes(prompt, "PAGINA");
  assertStringIncludes(prompt, "No inventes datos que no esten en el contexto");
  assertStringIncludes(prompt, "dilo explicitamente en vez de adivinar");
  assertStringIncludes(prompt, "No reemplazas al Director Tecnico");
});

Deno.test("el prompt de WhatsApp pide asteriscos y el de Telegram HTML", () => {
  const whatsapp = construirSystemPrompt("whatsapp");
  const telegram = construirSystemPrompt("html");

  assertStringIncludes(whatsapp, "negrita de WhatsApp");
  assert(!whatsapp.includes("<b>texto</b>"));
  assertStringIncludes(telegram, "<b>texto</b>");
});

Deno.test("una norma derogada se le advierte al modelo", () => {
  const advertencias = advertenciasDelBloque({
    ...CHUNK_VERIFICADO,
    estado_vigencia: "derogada",
  });

  assert(advertencias.length > 0);
  assertStringIncludes(advertencias.join(" "), "DEROGADA");
});

Deno.test("una transcripción no verificada baja la confianza del bloque", () => {
  const baja = advertenciasDelBloque({
    ...CHUNK_VERIFICADO,
    revisado_manual: false,
    quality_score: 0.3,
  });
  assertStringIncludes(baja.join(" "), "BAJA confiabilidad");

  const conTabla = advertenciasDelBloque({
    ...CHUNK_VERIFICADO,
    revisado_manual: false,
    has_tables: true,
  });
  assertStringIncludes(conTabla.join(" "), "tabla");
});

Deno.test("una página ya verificada por un humano no arrastra advertencias", () => {
  assertEquals(advertenciasDelBloque(CHUNK_VERIFICADO), []);
});

Deno.test("el contexto que recibe el modelo identifica documento y página", () => {
  const contexto = buildConsultaContext([CHUNK_VERIFICADO]);

  assertStringIncludes(contexto, "DS-20-2024");
  assertStringIncludes(contexto, "pagina 5");
  assertStringIncludes(contexto, "2024-10-25");
  assertStringIncludes(contexto, "Link oficial: https://www.digemid.minsa.gob.pe/ds-20-2024");
});

Deno.test("las fuentes no se repiten aunque un documento aporte varias páginas", () => {
  const fuentes = consultaSources([CHUNK_VERIFICADO, { ...CHUNK_VERIFICADO, page_number: 6 }]);

  assertEquals(fuentes.length, 1);
  assertEquals(fuentes[0].documentKey, "DS-20-2024");
});

Deno.test("una consulta con evidencia devuelve respuesta y fuente trazable", async () => {
  // La reformulación y la respuesta final usan el mismo modelo mockeado: la
  // reformulación "responde" con este mismo texto, pero como solo se usa
  // para buscar (y el fake ignora el argumento de búsqueda), no afecta el
  // resultado — lo que importa es la ÚLTIMA llamada, la de la respuesta.
  const llm = fakeLlm("*La sanción es 0.5 UIT.*\n\n📌 Fuente: *DS-20-2024* — 25/10/2024, pag. 5");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({ chunks: [CHUNK_VERIFICADO] }),
      "cuánto es la multa por no informar precios",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, false);
    assertEquals(resultado.sources.length, 1);
    assertEquals(resultado.sources[0].documentKey, "DS-20-2024");
    assertEquals(resultado.sources[0].page, 5);
    assertStringIncludes(resultado.answer, "0.5 UIT");

    // Dos llamadas al modelo: reformular la pregunta y generar la respuesta.
    assertEquals(llm.cuerposEnviados.length, 2);
    // El contexto documental viajó al modelo en la llamada de respuesta
    // (no respondió de memoria).
    const cuerpoRespuesta = llm.cuerposEnviados[llm.cuerposEnviados.length - 1];
    assertStringIncludes(cuerpoRespuesta, "DS-20-2024");
    assertStringIncludes(cuerpoRespuesta, "pagina 5");
  } finally {
    llm.restaurar();
  }
});

Deno.test("sin documentos relacionados se marca sinEvidencia y no se fabrica una respuesta", async () => {
  const llm = fakeLlm("no debería usarse como respuesta final");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({ chunks: [], sugerencias: [] }),
      "pregunta sin sustento en la base",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, true);
    assertEquals(resultado.sources.length, 0);
    assertStringIncludes(resultado.answer, "No encontré documentos relacionados");
    // Solo la reformulación gasta modelo; sin evidencia jamás se gasta una
    // llamada para FABRICAR una respuesta.
    assertEquals(llm.cuerposEnviados.length, 1, "solo debe gastarse la reformulación, no una respuesta");
  } finally {
    llm.restaurar();
  }
});

Deno.test("sin coincidencia exacta se ofrecen alertas similares, no una conclusión", async () => {
  const llm = fakeLlm("no debería usarse como respuesta final");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        chunks: [],
        sugerencias: [{
          document_key: "75-2026",
          title: "Producto falsificado",
          published_date: "2026-09-01",
          detail_url: "https://www.digemid.minsa.gob.pe/alerta-75",
        }],
      }),
      "algo parecido",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, true);
    assertEquals(resultado.sources.length, 1);
    assertStringIncludes(resultado.answer, "No encontré una coincidencia exacta");
    assertEquals(llm.cuerposEnviados.length, 1, "solo debe gastarse la reformulación, no una respuesta");
  } finally {
    llm.restaurar();
  }
});

Deno.test("reformularConsultaParaBusqueda: si el modelo falla, se usa la pregunta original", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = (() => Promise.reject(new Error("caída de red"))) as typeof fetch;

  try {
    const { reformularConsultaParaBusqueda } = await import("./consulta-ia.ts");
    const resultado = await reformularConsultaParaBusqueda("pregunta cualquiera", CONFIG);
    assertEquals(resultado, "pregunta cualquiera");
  } finally {
    globalThis.fetch = original;
  }
});

Deno.test("pareceConsultaSobreUltimaNorma detecta preguntas de reciencia", () => {
  assert(pareceConsultaSobreUltimaNorma("¿Puedes explicarme la última norma que ha subido DIGEMID?"));
  assert(pareceConsultaSobreUltimaNorma("cual es la norma mas reciente publicada"));
  assert(pareceConsultaSobreUltimaNorma("dame la ley mas reciente"));
  assert(!pareceConsultaSobreUltimaNorma("qué establece la Ley 29459"));
  assert(!pareceConsultaSobreUltimaNorma("cuánto es la multa por no informar precios"));
});

Deno.test("una pregunta sobre la ultima norma se responde con datos ordenados por fecha, sin modelo", async () => {
  const llm = fakeLlm("no debería usarse");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        normativaReciente: [
          {
            document_key: "RM-793-2025",
            title: "Aprueba lineamientos",
            published_date: "2025-08-01",
            published_date_display: "01/08/2025",
            source_section: "resolucion-ministerial",
            detail_url: "https://www.digemid.minsa.gob.pe/rm-793-2025",
          },
        ],
      }),
      "¿Puedes explicarme la última norma que ha subido a la página web de DIGEMID?",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, false);
    assertEquals(resultado.sources.length, 1);
    assertEquals(resultado.sources[0].documentKey, "RM-793-2025");
    assertStringIncludes(resultado.answer, "RM-793-2025");
    assertEquals(llm.cuerposEnviados.length, 0, "no debe gastarse ninguna llamada al modelo");
  } finally {
    llm.restaurar();
  }
});

Deno.test("extraerReferenciaNormativa reconoce tipo + numero + año", () => {
  assertEquals(
    extraerReferenciaNormativa("interpreta la resolución ministerial 727-2025/minsa")?.documentKey,
    "RM-727-2025",
  );
  // "Ley 29459" no trae año explícito, así que no se puede armar un
  // document_key exacto (LEY-{numero}-{anio}): se deja sin resolver.
  assertEquals(extraerReferenciaNormativa("qué establece la Ley 29459"), null);
  assertEquals(extraerReferenciaNormativa("DS 020-2024")?.documentKey, "DS-020-2024");
  assertEquals(extraerReferenciaNormativa("cuánto es la multa por no informar precios"), null);
  assertEquals(extraerReferenciaNormativa("cual es la norma mas reciente publicada"), null);
});

Deno.test("una norma citada por numero exacto se responde con su contenido real, no busqueda difusa", async () => {
  const llm = fakeLlm("*RM-727-2025 establece...*\n\n📌 Fuente: *RM-727-2025* — 2025-08-01, pag. 1");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        // Si el flujo cayera a busqueda difusa, encontraria estos chunks de
        // OTRA norma en vez de la citada; la prueba falla si eso pasa.
        chunks: [{ ...CHUNK_VERIFICADO, document_key: "RM-899-2025" }],
        norma: {
          id: "norma-1",
          document_key: "RM-727-2025",
          titulo: "Resolución Ministerial N° 727-2025/MINSA",
          fecha_publicacion: "2025-08-01",
          source_url: "https://www.digemid.minsa.gob.pe/rm-727-2025",
          pdf_url: null,
          estado_vigencia: "vigente",
        },
        paginasNorma: [
          {
            page_number: 1,
            text_normalized: "Aprueban el listado de productos farmacéuticos...",
            text_raw: null,
            quality_score: 0.9,
            revisado_manual: false,
            has_tables: false,
            posible_formula: false,
          },
        ],
        relacionesNorma: [
          {
            tipo_relacion: "deroga",
            tipo_norma_afectada: "RM",
            numero_afectada: "049",
            anio_afectada: "2025",
            descripcion_afectada: "listado anterior",
            estado: "detectada_automaticamente",
            norma_origen_id: "norma-1",
            norma_afectada_id: "norma-otra",
            norma_origen_document_key: "RM-727-2025",
          },
        ],
      }),
      "interpreta la resolución ministerial 727-2025/minsa y a que otras normas afecta",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, false);
    assertEquals(resultado.sources.length, 1);
    assertEquals(resultado.sources[0].documentKey, "RM-727-2025");

    // Una sola llamada al modelo (responder), ninguna de reformulacion ni de
    // busqueda difusa: la referencia exacta evita ambas.
    assertEquals(llm.cuerposEnviados.length, 1);
    const cuerpo = llm.cuerposEnviados[0];
    assertStringIncludes(cuerpo, "RM-727-2025");
    assertStringIncludes(cuerpo, "deroga");
    assert(!cuerpo.includes("RM-899-2025"));

    // La relacion normativa tambien llega al USUARIO, no solo al modelo:
    // "a que otras normas afecta" no se contesta solo con el contexto que
    // recibio el modelo, sino con un bloque visible en la respuesta final.
    assertStringIncludes(resultado.answer, "Otras normas a tener en cuenta");
    assertStringIncludes(resultado.answer, "listado anterior");
  } finally {
    llm.restaurar();
  }
});

Deno.test("una norma citada que no existe en digemid_normas cae a busqueda de texto", async () => {
  const llm = fakeLlm("*Respuesta desde busqueda difusa*\n\n📌 Fuente: *RM-899-2025* — 2025-08-01, pag. 1");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        chunks: [{ ...CHUNK_VERIFICADO, document_key: "RM-899-2025" }],
        norma: null,
      }),
      "interpreta la resolución ministerial 727-2025/minsa",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, false);
    assertEquals(resultado.sources[0].documentKey, "RM-899-2025");
  } finally {
    llm.restaurar();
  }
});

Deno.test("una norma citada sin contenido extraido avisa en vez de inventar", async () => {
  const llm = fakeLlm("no deberia usarse");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        norma: {
          id: "norma-2",
          document_key: "RM-614-2026",
          titulo: "Resolución Ministerial N° 614-2026/MINSA",
          fecha_publicacion: "2026-08-27",
          source_url: "https://www.digemid.minsa.gob.pe/rm-614-2026",
          pdf_url: null,
          estado_vigencia: "vigente",
        },
        paginasNorma: [],
      }),
      "interpreta la resolución ministerial 614-2026/minsa",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, true);
    assertStringIncludes(resultado.answer, "RM-614-2026");
    assertEquals(llm.cuerposEnviados.length, 0);
  } finally {
    llm.restaurar();
  }
});

Deno.test("preguntaPideVerificacionVisual detecta preguntas sobre tablas/graficos", () => {
  assert(preguntaPideVerificacionVisual("interpreta la tabla de la resolución ministerial 727-2025"));
  assert(preguntaPideVerificacionVisual("qué dice el cuadro del anexo 1"));
  assert(preguntaPideVerificacionVisual("explica el gráfico de la página 3"));
  assert(!preguntaPideVerificacionVisual("interpreta la resolución ministerial 727-2025/minsa"));
  assert(!preguntaPideVerificacionVisual("cuánto es la multa por no informar precios"));
});

/** Mock de fetch que distingue las 3 llamadas de red que puede hacer una
 * norma puntual con pregunta sobre tabla/grafico: DeepSeek (respuesta
 * principal), Gemini (verificacion visual) y la descarga del PDF (todo lo
 * demas) — sin esto, un solo mock generico (como fakeLlm) no puede simular
 * este flujo porque las 3 llamadas necesitan formas de respuesta distintas. */
function fakeFetchConVision(respuestaPrincipal: string, respuestaVisual: string) {
  const original = globalThis.fetch;
  const llamadas: string[] = [];

  globalThis.fetch = ((url: string | URL | Request) => {
    const urlStr = String(url);
    llamadas.push(urlStr);

    if (urlStr.includes("deepseek.com")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({ choices: [{ message: { content: respuestaPrincipal } }] }),
          { status: 200 },
        ),
      );
    }

    if (urlStr.includes("generativelanguage.googleapis.com")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({ candidates: [{ content: { parts: [{ text: respuestaVisual }] } }] }),
          { status: 200 },
        ),
      );
    }

    // Cualquier otra URL se asume la descarga del PDF.
    return Promise.resolve(new Response(new Uint8Array([1, 2, 3, 4]), { status: 200 }));
  }) as typeof fetch;

  return { llamadas, restaurar: () => { globalThis.fetch = original; } };
}

Deno.test("una pregunta sobre una tabla puntual agrega verificacion visual del PDF real", async () => {
  const mock = fakeFetchConVision(
    "*Escala de sanciones...*\n\n📌 Fuente: *RM-727-2025* — 2025-08-01, pag. 6",
    "La tabla tiene 3 columnas: infracción, farmacia y botica, con celdas combinadas en la fila 1.",
  );

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        norma: {
          id: "norma-3",
          document_key: "RM-727-2025",
          titulo: "Resolución Ministerial N° 727-2025/MINSA",
          fecha_publicacion: "2025-08-01",
          source_url: "https://www.digemid.minsa.gob.pe/rm-727-2025",
          pdf_url: "https://www.digemid.minsa.gob.pe/rm-727-2025.pdf",
          estado_vigencia: "vigente",
        },
        paginasNorma: [
          {
            page_number: 6,
            text_normalized: "Tabla de escala de sanciones (texto aplanado, poco confiable)",
            text_raw: null,
            quality_score: 0.6,
            revisado_manual: false,
            has_tables: true,
            posible_formula: false,
          },
        ],
        relacionesNorma: [],
      }),
      "interpreta la tabla de sanciones de la resolución ministerial 727-2025/minsa",
      { ...CONFIG, geminiApiKey: "clave-gemini-prueba" },
    );

    assertEquals(resultado.sinEvidencia, false);
    assertStringIncludes(resultado.answer, "Verificación visual del PDF");
    assertStringIncludes(resultado.answer, "celdas combinadas");
    assert(mock.llamadas.some((u) => u.includes("rm-727-2025.pdf")));
    assert(mock.llamadas.some((u) => u.includes("generativelanguage.googleapis.com")));
  } finally {
    mock.restaurar();
  }
});

Deno.test("sin GEMINI_API_KEY configurada, una pregunta sobre tabla no intenta verificacion visual", async () => {
  const llm = fakeLlm("*Respuesta sin vision*\n\n📌 Fuente: *RM-727-2025* — 2025-08-01, pag. 6");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({
        norma: {
          id: "norma-4",
          document_key: "RM-727-2025",
          titulo: "Resolución Ministerial N° 727-2025/MINSA",
          fecha_publicacion: "2025-08-01",
          source_url: "https://www.digemid.minsa.gob.pe/rm-727-2025",
          pdf_url: "https://www.digemid.minsa.gob.pe/rm-727-2025.pdf",
          estado_vigencia: "vigente",
        },
        paginasNorma: [
          {
            page_number: 6,
            text_normalized: "Tabla de escala de sanciones",
            text_raw: null,
            quality_score: 0.6,
            revisado_manual: false,
            has_tables: true,
            posible_formula: false,
          },
        ],
        relacionesNorma: [],
      }),
      "interpreta la tabla de sanciones de la resolución ministerial 727-2025/minsa",
      CONFIG,
    );

    assert(!resultado.answer.includes("Verificación visual del PDF"));
  } finally {
    llm.restaurar();
  }
});
