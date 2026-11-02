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

Deno.test("clasifica proyectos sin convertirlos en disposiciones aprobadas", () => {
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
  assert.equal(project.normative_status, "project");
  assert.equal(approved.normative_status, "approved");
});

Deno.test("fecha ausente no se sustituye por fecha de registro", () => {
  assert.match(
    formatEvidenceChronology({ published_date: null }),
    /sin fecha oficial/,
  );
  assert.doesNotMatch(
    formatEvidenceChronology({ published_date: null }),
    /registro/,
  );
  assert.match(
    formatEvidenceChronology({ published_date: "2026-10-20" }),
    /Fecha oficial: 2026-10-20/,
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
