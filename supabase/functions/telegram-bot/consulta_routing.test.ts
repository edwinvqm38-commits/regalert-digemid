import assert from "node:assert/strict";
import { deterministicConsultaIntent } from "./consulta_intent.ts";
import {
  answerConsultaRuntime,
  type ConsultaChunk,
  type ConsultaRuntimeDependencies,
} from "./consulta_runtime.ts";
import { esConsultaDeUltimaNormativa } from "./consulta_routing.ts";

for (
  const question of [
    "¿Cuál es la última normativa?",
    "Muéstrame las nuevas normas de DIGEMID",
    "¿Qué resolución reciente publicó DIGEMID?",
    "Dame las 5 últimas leyes, reglamentos y decretos",
  ]
) {
  Deno.test(`listado cronológico conserva ruta directa: ${question}`, () => {
    assert.equal(esConsultaDeUltimaNormativa(question), true);
  });
}

for (
  const question of [
    "¿Hay nueva normativa para los POES?",
    "¿Cuál es la última norma sobre almacenamiento?",
    "Nueva normativa POES",
    "Normativa reciente aplicable a boticas",
    "¿Qué obligaciones trae la nueva normativa?",
    "¿Hay nuevas normas de farmacovigilancia?",
  ]
) {
  Deno.test(`consulta temática no se redirige a /normas: ${question}`, () => {
    assert.equal(esConsultaDeUltimaNormativa(question), false);
  });
}

Deno.test("consulta POES recorre intención, recuperación, seguridad, DeepSeek y citas", async () => {
  const question = "¿Hay nueva normativa para los POES?";
  const evidence: ConsultaChunk = {
    document_key: "NORMA-DEMO",
    page_number: 4,
    detail_url: "https://example.invalid/norma-oficial.pdf",
    has_tables: false,
    text_content:
      "La botica debe documentar sus Procedimientos Operativos Estandarizados (POES).",
  };
  const calls = { intent: 0, search: 0, deepseek: 0 };
  const deps: ConsultaRuntimeDependencies = {
    interpretIntent: async (receivedQuestion) => {
      calls.intent++;
      assert.equal(receivedQuestion, question);
      return {
        ...deterministicConsultaIntent(receivedQuestion),
        intent: "regulatory_question",
        search_query: "POES normativa",
      };
    },
    searchChunks: async (query) => {
      calls.search++;
      assert.equal(query, "POES normativa");
      return [evidence];
    },
    suggestSimilarAlerts: async () => [],
    resolverNormaPuntual: async () => ({ status: "not_a_specific_norm" }),
    getNormaConContenido: async () => null,
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
    construirReporte: () => "",
    deepseek: async (prompt) => {
      calls.deepseek++;
      assert.match(prompt, /Procedimientos Operativos Estandarizados/);
      assert.match(prompt, /no lo reduzcas a procedimientos de saneamiento/);
      assert.match(prompt, /La botica debe documentar sus Procedimientos/);
      assert.match(prompt, /Pregunta: ¿Hay nueva normativa para los POES\?/);
      return "Respuesta sustentada en la evidencia recuperada.";
    },
  };

  assert.equal(esConsultaDeUltimaNormativa(question), false);
  const result = await answerConsultaRuntime(question, deps);

  assert.deepEqual(calls, { intent: 1, search: 1, deepseek: 1 });
  assert.equal(result.blocked, false);
  assert.deepEqual(result.sources, [{
    documentKey: "NORMA-DEMO",
    url: "https://example.invalid/norma-oficial.pdf",
    page: 4,
  }]);
});
