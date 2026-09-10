import assert from "node:assert/strict";
import test from "node:test";

import {
  type ConsultaEvidence,
  evaluateConsultaSafety,
  executeConsultaWithSafetyGate,
} from "./consulta_safety.ts";
import {
  buildOrdinalEditableTableBlock,
  ORDINAL_COLUMN_IDENTITY_METHOD,
  rebuildOrdinalEditableTables,
  TABLE_VERIFICATION_METHOD,
} from "./revision_safety.ts";

const sha256 = "a".repeat(64);
const validTable = "| Categoría | Valor |\n| --- | --- |\n| A | 1 |";
const pending = {
  document_key: "DS-20-2024",
  page_number: 5,
  has_tables: true,
  possible_table: true,
  table_requires_review: true,
  tabla_verificada: false,
  revisado_manual: false,
};

const trusted: any = {
  ...pending,
  table_requires_review: false,
  tabla_verificada: true,
  revisado_manual: true,
  metadata: {
    source_pdf_sha256: sha256,
    tables_markdown: validTable,
    table_regions: { detected_regions: 1, accepted_regions: 1, unresolved_regions: 0 },
    manual_review: {
      table_verified: true,
      table_confirmation_requested: true,
      table_structure_present: true,
      table_verification: {
        confirmation_explicit: true,
        verification_method: TABLE_VERIFICATION_METHOD,
        verified_at: "2026-09-01T00:00:00.000Z",
        source_pdf_sha256: sha256,
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
  },
};

async function assertBlockedWithoutLlm(
  question: string,
  evidence: ConsultaEvidence[] = [pending],
) {
  let llmCalls = 0;
  const result = await executeConsultaWithSafetyGate(question, evidence, async () => {
    llmCalls++;
    return "respuesta del modelo";
  });
  assert.equal(result.blocked, true, question);
  assert.equal(llmCalls, 0, question);
}

test("tabla pendiente + consulta estructural bloquea y no llama al LLM", async () => {
  await assertBlockedWithoutLlm("¿Qué valor corresponde a cada categoría?");
});

test("seis consultas mixtas narrativa y relacional bloquean toda la intención", async () => {
  const questions = [
    "¿Qué establece el artículo 7 y qué sanción recibe una botica?",
    "Cita el artículo y señala la sanción correspondiente.",
    "Resume el artículo y después indica la multa.",
    "Explica la norma y dime qué valor corresponde a cada categoría.",
    "¿Qué dice el numeral y qué le corresponde a una botica?",
    "Resume esta disposición y dime cuál es la sanción aplicable.",
  ];
  for (const question of questions) await assertBlockedWithoutLlm(question);
});

test("revisado_manual no verifica una tabla ni habilita el LLM", async () => {
  await assertBlockedWithoutLlm(
    "¿Cuál es el monto para la categoría A?",
    [{ ...pending, revisado_manual: true }],
  );
});

test("tabla_verificada legacy sin evidencia moderna permanece bloqueada", async () => {
  await assertBlockedWithoutLlm(
    "¿Qué valor corresponde a cada categoría?",
    [{ ...pending, tabla_verificada: true }],
  );
});

test("evidencia moderna completa habilita el flujo normal", async () => {
  let llmCalls = 0;
  const result = await executeConsultaWithSafetyGate(
    "¿Qué valor corresponde a cada categoría?",
    [trusted],
    async () => {
      llmCalls++;
      return "respuesta verificada";
    },
  );
  assert.equal(result.blocked, false);
  assert.equal(result.value, "respuesta verificada");
  assert.equal(llmCalls, 1);
});

test("flags contradictorios usan OR conservador", async () => {
  await assertBlockedWithoutLlm(
    "¿Qué valor corresponde a cada categoría?",
    [{
      document_key: "CONTRADICTORIA",
      page_number: 1,
      has_tables: true,
      possible_table: false,
      table_requires_review: false,
      tabla_verificada: false,
    }],
  );
});

test("possible_table solo en metadata tambien falla cerrado", async () => {
  await assertBlockedWithoutLlm(
    "¿Qué establece el artículo 3?",
    [{
      document_key: "METADATA-MODERNA",
      page_number: 1,
      has_tables: false,
      possible_table: false,
      metadata: { possible_table: true },
    }],
  );
});

test("pagina sin tabla y consulta textual siguen el flujo normal", async () => {
  let llmCalls = 0;
  const result = await executeConsultaWithSafetyGate(
    "¿Qué establece el artículo 3?",
    [{ possible_table: false, has_tables: false }],
    async () => {
      llmCalls++;
      return "texto narrativo";
    },
  );
  assert.equal(result.blocked, false);
  assert.equal(llmCalls, 1);
});

test("tabla pendiente sin prosa segregada bloquea incluso pregunta narrativa", () => {
  const decision = evaluateConsultaSafety("¿Qué establece el artículo 3?", [pending]);
  assert.equal(decision.blocked, true);
  assert.equal(decision.reason, "unseparated_pending_table_content");
  assert.equal(decision.pending_evidence.length, 1);
  assert.deepEqual(decision.permitted_evidence, []);
});

test("pagina sin señal tabular no crea falso riesgo", () => {
  const decision = evaluateConsultaSafety(
    "¿Qué establece el artículo 3?",
    [{ possible_table: false, has_tables: false, table_requires_review: false }],
  );
  assert.equal(decision.blocked, false);
  assert.equal(decision.pending_evidence.length, 0);
  assert.equal(decision.permitted_evidence.length, 1);
});

test("metadata legacy con has_tables conserva default fail-closed", async () => {
  await assertBlockedWithoutLlm(
    "¿Qué valor aplica para cada tipo?",
    [{ document_key: "LEGACY", page_number: 2, has_tables: true }],
  );
});

test("multipagina bloquea si una fuente necesaria sigue pendiente", async () => {
  await assertBlockedWithoutLlm(
    "Compara el valor de cada categoría entre ambas páginas.",
    [trusted, { ...pending, page_number: 6 }],
  );
});

test("multirregion bloquea si queda una region sin cobertura", async () => {
  const incomplete = structuredClone(trusted);
  incomplete.metadata.table_regions = {
    detected_regions: 2,
    accepted_regions: 1,
    unresolved_regions: 1,
  };
  await assertBlockedWithoutLlm(
    "¿Qué sanción corresponde a cada establecimiento?",
    [incomplete],
  );
});

test("encabezado ambiguo tras roundtrip ordinal bloquea y no llama al LLM", async () => {
  const headers = ["", "BOTICA"];
  const roundtrip = rebuildOrdinalEditableTables(
    buildOrdinalEditableTableBlock(headers, [["A", "10"]], 1).join("\n"),
    [headers],
  );
  const ambiguous = structuredClone(trusted);
  ambiguous.metadata.tables_markdown = roundtrip.text;
  ambiguous.metadata.manual_review.table_verification.ordinal_columns = {
    column_identity: roundtrip.column_identity,
    editable_table_count: roundtrip.editable_table_count,
    invalid_block_count: roundtrip.invalid_block_count,
    cardinality_preserved: roundtrip.cardinality_preserved,
    source_headers_matched: roundtrip.source_headers_matched,
    synthetic_headers_present: roundtrip.synthetic_headers_present,
    empty_headers_present: roundtrip.empty_headers_present,
    duplicate_header_ambiguity: roundtrip.duplicate_header_ambiguity,
    duplicate_headers: roundtrip.duplicate_headers,
  };

  await assertBlockedWithoutLlm(
    "¿Qué sanción corresponde a cada establecimiento?",
    [ambiguous],
  );
});

test("table_requires_review contradictorio es veto absoluto integrado", async () => {
  const contradictory = structuredClone(trusted);
  contradictory.table_requires_review = true;
  await assertBlockedWithoutLlm(
    "¿Qué valor corresponde a la categoría A?",
    [contradictory],
  );
});

test("texto tabular aplanado pendiente nunca llega al callback del LLM", async () => {
  const flattened = {
    ...pending,
    text_content: "Artículo 7. Disposición narrativa.\n66\n0.5 UIT\nBOTICA\n2 UIT\nNA",
  };
  let llmCalls = 0;
  let observedContext = "";
  const result = await executeConsultaWithSafetyGate(
    "¿Qué establece el artículo 7?",
    [flattened],
    async (permitted) => {
      llmCalls++;
      observedContext = permitted.map((item) => item.text_content ?? "").join("\n");
      return "respuesta";
    },
  );
  assert.equal(result.blocked, true);
  assert.equal(result.answer.includes("no puede separar todavía"), true);
  assert.equal(llmCalls, 0);
  assert.equal(observedContext, "");
});

test("narrativa usa otras paginas seguras y excluye completa la pagina pendiente", async () => {
  const flattened = {
    ...pending,
    text_content: "Artículo 7.\n66\n0.5 UIT\nBOTICA",
  };
  const safePage = {
    document_key: "NORMA-SEGURA",
    page_number: 3,
    has_tables: false,
    possible_table: false,
    text_content: "Artículo 7. La autorización tiene vigencia anual.",
  };
  let llmCalls = 0;
  let observedContext = "";
  const result = await executeConsultaWithSafetyGate(
    "¿Qué establece el artículo 7?",
    [flattened, safePage],
    async (permitted) => {
      llmCalls++;
      observedContext = permitted.map((item) => item.text_content ?? "").join("\n");
      return "respuesta segura";
    },
  );
  assert.equal(result.blocked, false);
  assert.equal(llmCalls, 1);
  assert.equal(observedContext.includes("vigencia anual"), true);
  assert.equal(observedContext.includes("0.5 UIT"), false);
  assert.equal(result.evidence.length, 1);
});

function trustedDuplicateHeaders() {
  const duplicate = structuredClone(trusted);
  duplicate.metadata.tables_markdown =
    "| TIPO | VALOR | VALOR |\n| --- | --- | --- |\n| A | 10 | 20 |";
  duplicate.metadata.manual_review.table_verification.ordinal_columns = {
    column_identity: ORDINAL_COLUMN_IDENTITY_METHOD,
    editable_table_count: 1,
    invalid_block_count: 0,
    cardinality_preserved: true,
    source_headers_matched: true,
    synthetic_headers_present: false,
    empty_headers_present: false,
    duplicate_header_ambiguity: true,
    duplicate_headers: ["VALOR"],
  };
  return duplicate;
}

test("encabezado duplicado exige desambiguación y no llama al LLM", async () => {
  await assertBlockedWithoutLlm(
    "¿Cuál de los dos VALOR corresponde a esta categoría?",
    [trustedDuplicateHeaders()],
  );
});

test("C2 y C3 permiten consultar una tabla trusted con encabezados duplicados", async () => {
  for (const ordinal of [2, 3]) {
    let llmCalls = 0;
    const result = await executeConsultaWithSafetyGate(
      `¿Qué valor figura en C${ordinal} para la fila A?`,
      [trustedDuplicateHeaders()],
      async () => {
        llmCalls++;
        return ordinal === 2 ? "10" : "20";
      },
    );
    assert.equal(result.blocked, false);
    assert.equal(result.value, ordinal === 2 ? "10" : "20");
    assert.equal(llmCalls, 1);
  }
});

test("C1 no desambigua un encabezado VALOR duplicado", async () => {
  await assertBlockedWithoutLlm(
    "¿Qué VALOR figura en C1 para la fila A?",
    [trustedDuplicateHeaders()],
  );
});

test("ordinal inexistente en tabla trusted se bloquea sin LLM", async () => {
  await assertBlockedWithoutLlm(
    "¿Qué valor figura en C4 para la fila A?",
    [trustedDuplicateHeaders()],
  );
});
