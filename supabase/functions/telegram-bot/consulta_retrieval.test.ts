import assert from "node:assert/strict";
import {
  buildThematicRetrievalPlan,
  detectConsultaTheme,
  formatEvidenceChronology,
  rankThematicEvidence,
  thematicGuidance,
} from "./consulta_retrieval.ts";

const themes: [string, string][] = [
  ["¿Hay nueva normativa que afecte los POES de una botica?", "poes"],
  ["¿Qué norma exige un POE en la farmacia?", "poes"],
  ["Normas sobre almacenamiento de medicamentos", "almacenamiento"],
  ["Obligaciones de dispensación en farmacias", "dispensacion"],
  ["Cambios de farmacovigilancia", "farmacovigilancia"],
];

for (const [question, expected] of themes) {
  Deno.test(`detecta recuperación temática: ${expected}`, () => {
    assert.equal(detectConsultaTheme(question), expected);
    const plan = buildThematicRetrievalPlan(question);
    assert.equal(plan?.theme, expected);
    assert.equal(plan?.evidenceLimit, 4);
    assert.ok((plan?.queries.length ?? 0) >= 2);
  });
}

Deno.test("POES conserva su definición y nunca se reduce a saneamiento", () => {
  const guidance = thematicGuidance("poes");
  assert.match(guidance, /Procedimientos Operativos Estandarizados/);
  assert.match(guidance, /no lo reduzcas a procedimientos de saneamiento/);
});

Deno.test("la pregunta original POES prevalece sobre una expansión errónea", () => {
  const plan = buildThematicRetrievalPlan(
    "¿Qué normativa afecta los POES?",
    "procedimientos operativos de saneamiento",
  );
  assert.equal(plan?.theme, "poes");
});

Deno.test("ranking descarta coincidencias OR sin ancla y prioriza boticas", () => {
  const ranked = rankThematicEvidence("poes", [
    {
      document_key: "IRRELEVANTE",
      page_number: 1,
      title: "Norma para boticas",
      text_content:
        "Regula establecimientos farmacéuticos sin mencionar el tema.",
    },
    {
      document_key: "GENERAL",
      page_number: 2,
      title: "Manual general",
      text_content: "Contiene procedimientos operativos estandarizados.",
    },
    {
      document_key: "FARMACIA",
      page_number: 3,
      title: "Buenas prácticas de oficina farmacéutica",
      text_content:
        "La botica debe mantener POES como establecimiento farmacéutico.",
    },
  ]);
  assert.deepEqual(ranked.map((row) => row.document_key), [
    "FARMACIA",
    "GENERAL",
  ]);
});

Deno.test("ranking POE prioriza plurales del ámbito farmacéutico", () => {
  const ranked = rankThematicEvidence("poes", [
    {
      document_key: "GENERAL-POE",
      page_number: 1,
      text_content: "Se requiere un POE para las actividades internas.",
    },
    {
      document_key: "BOTICAS-POE",
      page_number: 1,
      text_content:
        "Las boticas y farmacias mantienen un POE en sus establecimientos farmacéuticos.",
    },
  ]);
  assert.equal(ranked[0].document_key, "BOTICAS-POE");
});

Deno.test("palabras de estado son indicios de página, no estado documental", () => {
  const [project, approved] = rankThematicEvidence("farmacovigilancia", [
    {
      document_key: "PROYECTO",
      page_number: 1,
      text_content:
        "Se dispone la prepublicación del proyecto de farmacovigilancia.",
    },
    {
      document_key: "APROBADA",
      page_number: 1,
      text_content: "Se aprueba la norma técnica de farmacovigilancia.",
    },
  ]).sort((a, b) =>
    String(b.document_key).localeCompare(String(a.document_key))
  );
  assert.equal(project.document_status_indicator, "project_language");
  assert.equal(approved.document_status_indicator, "approval_language");
  for (const evidence of [project, approved]) {
    assert.equal(evidence.normative_status, undefined);
    const metadata = formatEvidenceChronology(evidence) ?? "";
    assert.match(metadata, /Indicio textual:/);
    assert.match(metadata, /Estado documental: por verificar/);
    assert.doesNotMatch(metadata, /Estado: (proyecto|aprobada)/);
  }
});

Deno.test("fecha ausente no se sustituye por fecha de registro", () => {
  const thematic = { thematic_relevance: 20 };
  assert.match(
    formatEvidenceChronology({ ...thematic, published_date: null }) ?? "",
    /sin fecha oficial/,
  );
  assert.doesNotMatch(
    formatEvidenceChronology({ ...thematic, published_date: null }) ?? "",
    /registro/,
  );
  assert.match(
    formatEvidenceChronology({
      ...thematic,
      published_date: "2026-10-20",
    }) ?? "",
    /Fecha oficial: 2026-10-20/,
  );
});

Deno.test("alerta general no recibe clasificación de estado normativo", () => {
  assert.equal(
    formatEvidenceChronology({
      document_key: "ALERTA-42-2026",
      published_date: "2026-10-20",
      text_content: "Se aprueba una medida sanitaria.",
    }),
    null,
  );
});

Deno.test("máximo cuatro evidencias y dos páginas por documento", () => {
  const candidates = Array.from({ length: 8 }, (_, index) => ({
    document_key: index < 5 ? "DOC-A" : `DOC-${index}`,
    page_number: index + 1,
    text_content: "Reglas de almacenamiento para productos farmacéuticos.",
  }));
  const ranked = rankThematicEvidence("almacenamiento", candidates, 20);
  assert.equal(ranked.length, 4);
  assert.ok(ranked.filter((row) => row.document_key === "DOC-A").length <= 2);
});

Deno.test("consulta general no activa expansión temática", () => {
  assert.equal(
    buildThematicRetrievalPlan("¿Cuáles son las últimas normas?"),
    null,
  );
});
