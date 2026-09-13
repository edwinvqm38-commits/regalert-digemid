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

    // El contexto documental viajó al modelo (no respondió de memoria).
    assertStringIncludes(llm.cuerposEnviados[0], "DS-20-2024");
    assertStringIncludes(llm.cuerposEnviados[0], "pagina 5");
  } finally {
    llm.restaurar();
  }
});

Deno.test("sin documentos relacionados se marca sinEvidencia y no se llama al modelo", async () => {
  const llm = fakeLlm("no debería usarse");

  try {
    const resultado = await answerConsulta(
      fakeSupabase({ chunks: [], sugerencias: [] }),
      "pregunta sin sustento en la base",
      CONFIG,
    );

    assertEquals(resultado.sinEvidencia, true);
    assertEquals(resultado.sources.length, 0);
    assertStringIncludes(resultado.answer, "No encontré documentos relacionados");
    assertEquals(llm.cuerposEnviados.length, 0, "no debe gastarse una llamada al modelo");
  } finally {
    llm.restaurar();
  }
});

Deno.test("sin coincidencia exacta se ofrecen alertas similares, no una conclusión", async () => {
  const llm = fakeLlm("no debería usarse");

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
    assertEquals(llm.cuerposEnviados.length, 0);
  } finally {
    llm.restaurar();
  }
});
