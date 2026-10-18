// Identidad documental para /consulta de Telegram; no modifica el catálogo.
// Equivalencias de tipos/números alineadas con scripts/identidad_normativa.py.
export type NormaReference = {
  tipo: string | null;
  numero: string;
  anio: number | null;
  sufijo: string | null;
};

export type NormaResolution =
  | { status: "resolved"; documentKey: string }
  | { status: "ambiguous"; reason: "incomplete" | "multiple" | "catalog" }
  | { status: "not_found" }
  | { status: "not_a_specific_norm" }
  | { status: "error" };

const TYPES: Record<string, string> = {
  ley: "LEY",
  "decreto supremo": "DS",
  "decreto legislativo": "DL",
  "decreto de urgencia": "DU",
  "resolucion ministerial": "RM",
  "resolucion directoral": "RD",
  "resolucion suprema": "RS",
  "resolucion viceministerial": "RVM",
  "resolucion jefatural": "RJ",
  "resolucion de gerencia general": "RGE",
  ds: "DS",
  dl: "DL",
  du: "DU",
  rm: "RM",
  rd: "RD",
  rs: "RS",
  rvm: "RVM",
  rj: "RJ",
  rge: "RGE",
};
const plain = (value: string) =>
  value.normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "").toLowerCase();
const normType = (value: string) => {
  const normalized = plain(value).replace(/\./g, "").replace(/\s+/g, " ")
    .trim();
  return TYPES[normalized] ?? TYPES[normalized.replace(/ /g, "")] ?? null;
};
const normYear = (value?: string) => {
  if (!value) return null;
  const year = value.length === 2
    ? Number(value) + (Number(value) >= 50 ? 1900 : 2000)
    : Number(value);
  return year >= 1900 && year <= 2100 ? year : null;
};
const typePattern = [...Object.keys(TYPES), "resolucion", "decreto"]
  .sort((a, b) => b.length - a.length)
  .map((type) =>
    type.length <= 3 && type !== "ley"
      ? type.split("").join("\\.?\\s*") + "\\.?"
      : type.replace(/ /g, "\\s+")
  )
  .join("|");

/** Conserva el año ausente y el sector explícito. Nunca fabrica una clave. */
export function extraerReferenciaNormativa(question: string): {
  references: NormaReference[];
  incomplete: boolean;
} {
  const text = plain(question);
  const pattern = new RegExp(
    `\\b(${typePattern})\\s*[-:]?\\s*(?:(?:numero|n[°ºo.]?)\\s*\\.?\\s*)?` +
      `(\\d{1,6})\\b(?:[\\s/-]+(\\d{4}|\\d{2})\\b)?` +
      `(?:\\s*[-/]\\s*([a-z][a-z0-9]*(?:[-/][a-z0-9]+)*))?`,
    "g",
  );
  const references: NormaReference[] = [];
  let incomplete = false;
  for (const match of text.matchAll(pattern)) {
    const tipo = normType(match[1]);
    const anio = normYear(match[3]);
    incomplete ||= !tipo || Boolean(match[3] && !anio) ||
      /^\s*[-/]\s*\w/.test(text.slice(match.index! + match[0].length));
    const reference = {
      tipo,
      numero: String(Number(match[2])),
      anio,
      sufijo: match[4]?.toUpperCase().replace(/\//g, "-") ?? null,
    };
    if (
      !references.some((r) => JSON.stringify(r) === JSON.stringify(reference))
    ) {
      references.push(reference);
    }
  }
  return { references, incomplete };
}

type CatalogRow = {
  id: string;
  document_key: string;
  tipo_norma: string | null;
  numero: string | null;
  anio: number | null;
  process_status: string | null;
  reconciliado_con_document_key: string | null;
};

function numberParts(value: string) {
  const match = value.trim().toUpperCase().match(
    /^(\d{1,6})(?:[-/](\d{4}|\d{2}))?(?:[-/]([A-Z][A-Z0-9]*(?:[-/][A-Z0-9]+)*))?$/,
  );
  return match
    ? {
      numero: String(Number(match[1])),
      anio: normYear(match[2]),
      sufijo: match[3]?.replace(/\//g, "-") ?? null,
      invalid: Boolean(match[2] && !normYear(match[2])),
    }
    : null;
}

function rowIdentity(row: CatalogRow) {
  const fields = numberParts(row.numero ?? "");
  const key = row.document_key.match(/^([A-Z]+)-(.+)$/i);
  const keyType = key ? normType(key[1]) : null;
  const keyParts = keyType ? numberParts(key![2]) : null;
  const fieldType = normType(row.tipo_norma ?? "");
  const identity = {
    tipo: fieldType ?? keyType,
    numero: fields?.numero ?? keyParts?.numero ?? "",
    anio: row.anio ?? fields?.anio ?? keyParts?.anio ?? null,
    sufijo: fields?.sufijo ?? keyParts?.sufijo ?? null,
  };
  const conflict = Boolean(
    (row.numero && !fields) || (row.tipo_norma && !fieldType) ||
      fields?.invalid || keyParts?.invalid ||
      (fieldType && keyType && fieldType !== keyType) ||
      (fields && keyParts && fields.numero !== keyParts.numero) ||
      [fields?.anio, keyParts?.anio].some((year) =>
        year != null && year !== identity.anio
      ) ||
      (fields?.sufijo && keyParts?.sufijo && fields.sufijo !== keyParts.sufijo),
  );
  // Ante campos contradictorios, conservar también la identidad de la clave:
  // descartar esa fila podría hacer parecer única a otra candidata.
  const keyIdentity = keyParts
    ? {
      tipo: keyType,
      numero: keyParts.numero,
      anio: keyParts.anio,
      sufijo: keyParts.sufijo,
    }
    : null;
  return { row, identity, conflict, keyIdentity };
}

function compatible(reference: NormaReference, candidate: NormaReference) {
  return reference.tipo === candidate.tipo &&
    reference.numero === candidate.numero &&
    (reference.anio == null || candidate.anio == null ||
      reference.anio === candidate.anio) &&
    (reference.sufijo == null || candidate.sufijo == null ||
      reference.sufijo === candidate.sufijo);
}

/** Consulta únicamente campos de identidad, con filtro numérico parametrizado
 * por dígitos. El LIKE recupera un superconjunto: la igualdad se decide después.
 * Paginación + count impiden confundir una lista truncada con una coincidencia única. */
export async function resolverNormaPuntual(
  db: { from: (table: string) => any },
  question: string,
): Promise<NormaResolution> {
  const parsed = extraerReferenciaNormativa(question);
  if (!parsed.references.length) return { status: "not_a_specific_norm" };
  if (parsed.incomplete || parsed.references.length !== 1) {
    return { status: "ambiguous", reason: "incomplete" };
  }
  const reference = parsed.references[0];
  try {
    const rows: CatalogRow[] = [];
    let total: number | undefined;
    do {
      const { data, error, count } = await db.from("digemid_normas")
        .select(
          "id,document_key,tipo_norma,numero,anio,process_status,reconciliado_con_document_key:raw->>reconciliado_con_document_key",
          { count: "exact" },
        )
        .or(
          `numero.ilike.%${reference.numero}%,document_key.ilike.%${reference.numero}%`,
        )
        .order("id")
        .range(rows.length, rows.length + 499);
      if (
        error || !Array.isArray(data) || !Number.isInteger(count) ||
        count < 0 ||
        (total !== undefined && total !== count) ||
        (!data.length && rows.length < count) || count > 10000
      ) {
        return { status: "error" };
      }
      total = count;
      rows.push(...data);
    } while (rows.length < total!);
    if (
      rows.length !== total || new Set(rows.map((r) => r.id)).size !== total
    ) {
      return { status: "error" };
    }
    const candidates = rows.map(rowIdentity).filter((c) =>
      compatible(reference, c.identity) ||
      (c.conflict && c.keyIdentity && compatible(reference, c.keyIdentity))
    );
    // Solo colapsar un alias YA reconciliado, cuyo destino esté presente y
    // tenga identidad compatible. No elegir por tener PDF, páginas o clave bonita.
    const canonical = candidates.filter((c) => {
      if (c.conflict || c.row.process_status !== "stub_derogada_reconciliada") {
        return true;
      }
      return !candidates.some((target) =>
        target.row.document_key === c.row.reconciliado_con_document_key &&
        !target.conflict && !target.row.process_status?.startsWith("stub") &&
        compatible(c.identity, target.identity)
      );
    });
    if (!canonical.length) return { status: "not_found" };
    if (canonical.length > 1) {
      return { status: "ambiguous", reason: "multiple" };
    }
    const candidate = canonical[0];
    if (
      candidate.conflict || candidate.row.process_status?.startsWith("stub") ||
      (reference.anio != null && candidate.identity.anio == null) ||
      (reference.sufijo != null && candidate.identity.sufijo == null)
    ) {
      return { status: "ambiguous", reason: "catalog" };
    }
    return { status: "resolved", documentKey: candidate.row.document_key };
  } catch {
    // Error técnico separado de identidad ausente o ambigua; sin datos sensibles.
    return { status: "error" };
  }
}
