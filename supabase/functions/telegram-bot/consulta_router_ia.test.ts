import assert from "node:assert/strict";
import {
  frasePorRuta,
  parseRouterJson,
  routeConsultaConIA,
} from "./consulta_router_ia.ts";
import { esConsultaDeUltimaNormativa } from "./consulta_routing.ts";

const dsResponse = (content: string) =>
  Promise.resolve(
    new Response(JSON.stringify({ choices: [{ message: { content } }] })),
  );

Deno.test("parse: ruta valida y periodo por defecto", () => {
  const r = parseRouterJson('{"route":"listar_alertas","period":null,"limit":3}', "deepseek");
  assert.equal(r?.period, "ultimas");
  assert.equal(r?.limit, 3);
});

Deno.test("parse: conteo sin periodo se descarta", () => {
  assert.equal(parseRouterJson('{"route":"contar_alertas","period":null}', "deepseek"), null);
});

Deno.test("parse: ambigua exige aclaracion; basura se descarta", () => {
  assert.equal(parseRouterJson('{"route":"ambigua"}', "deepseek"), null);
  assert.equal(parseRouterJson("no es json", "deepseek"), null);
  assert.equal(parseRouterJson('{"route":"inventada"}', "deepseek"), null);
  assert.equal(
    parseRouterJson('{"route":"ambigua","clarification":"¿Sobre qué norma?"}', "gemini")?.clarification,
    "¿Sobre qué norma?",
  );
});

Deno.test("router: DeepSeek responde", async () => {
  const r = await routeConsultaConIA("que dice la ultima norma que ha salido?", {
    deepseekApiKey: "k",
    fetchImpl: (() => dsResponse('{"route":"ultima_norma"}')) as typeof fetch,
  });
  assert.equal(r?.route, "ultima_norma");
  assert.equal(frasePorRuta(r!), null);
  const lista = parseRouterJson('{"route":"listar_normativa"}', "deepseek");
  assert.equal(esConsultaDeUltimaNormativa(frasePorRuta(lista!)!), true);
  // La pregunta reescrita sobre una norma concreta no debe caer en el listado.
  assert.equal(
    esConsultaDeUltimaNormativa("De que trata la Resolución Ministerial N° 614-2026/MINSA? Resume su contenido y los puntos principales."),
    false,
  );
});

Deno.test("router: cae a Gemini si DeepSeek falla", async () => {
  const calls: string[] = [];
  const r = await routeConsultaConIA("cuantas alertas salieron hoy", {
    deepseekApiKey: "k",
    geminiApiKey: "g",
    fetchImpl: ((url: string) => {
      calls.push(url);
      if (url.includes("deepseek")) return Promise.resolve(new Response("x", { status: 500 }));
      return Promise.resolve(
        new Response(JSON.stringify({
          candidates: [{ content: { parts: [{ text: '{"route":"contar_alertas","period":"hoy"}' }] } }],
        })),
      );
    }) as unknown as typeof fetch,
  });
  assert.equal(r?.provider, "gemini");
  assert.equal(frasePorRuta(r!), "cuántas alertas hoy");
  assert.equal(calls.length, 2);
});

Deno.test("router: sin claves o con error devuelve null", async () => {
  assert.equal(await routeConsultaConIA("hola", {}), null);
  assert.equal(
    await routeConsultaConIA("hola", {
      deepseekApiKey: "k",
      fetchImpl: (() => Promise.reject(new Error("red"))) as typeof fetch,
    }),
    null,
  );
});

Deno.test("frases canonicas", () => {
  assert.equal(frasePorRuta({ route: "listar_alertas", period: "ultimas", limit: 1, clarification: null, provider: "gemini" }), "la última alerta");
  assert.equal(frasePorRuta({ route: "listar_alertas", period: "mes", limit: null, clarification: null, provider: "gemini" }), "alertas de este mes");
  assert.equal(frasePorRuta({ route: "pregunta_documental", period: null, limit: null, clarification: null, provider: "gemini" }), null);
});
