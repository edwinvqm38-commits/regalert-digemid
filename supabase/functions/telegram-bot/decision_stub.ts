// Decision de que hacer con la norma afectada al CONFIRMAR una relacion (H-08).
//
// Esta separado de index.ts a proposito: es la regla juridica, no la mecanica
// de Telegram ni de Supabase, y asi puede probarse sin base de datos.
//
// Regla central: un stub es el ultimo recurso. Solo puede nacer cuando la
// norma esta suficientemente identificada, NO existe una norma real
// equivalente, NO hay ambiguedad, la cita tiene evidencia verificada y un
// humano confirmo la relacion.

import {
  AMBIGUA,
  construirIdentidad,
  DATOS_INSUFICIENTES,
  type FilaNorma,
  type NormaIdentity,
  resolverIdentidad,
  textoIdentidad,
} from "./identidad_normativa.ts";

export type AccionVinculo =
  | "vincular_norma_real"
  | "crear_stub"
  | "abortar_ambigua"
  | "abortar_datos_insuficientes"
  | "abortar_sin_evidencia"
  | "ya_vinculada";

export interface RelacionParaDecidir {
  norma_afectada_id: string | null;
  tipo_relacion: string;
  tipo_norma_afectada: string | null;
  numero_afectada: string | null;
  anio_afectada: number | null;
  articulos_afectados: string | null;
  alcance: string | null;
  fragmento_verificado: boolean | null;
}

export interface DecisionVinculo {
  accion: AccionVinculo;
  normaId: string | null;
  documentKeyStub: string | null;
  /** estado_vigencia a escribir, o null si NO debe tocarse. */
  estadoVigencia: string | null;
  /** true cuando la afectacion es parcial y por eso no se toca la vigencia. */
  bloqueadoPorAlcanceParcial: boolean;
  identidad: NormaIdentity;
  candidatas: string[];
  motivo: string;
}

/** Solo los tipos que afectan el TEXTO o la APLICABILIDAD cambian la vigencia.
 * exonera/prorroga/pendiente_verificacion dejan la norma citada como esta. */
export const ESTADO_VIGENCIA_POR_RELACION: Record<string, string | undefined> = {
  deroga: "derogada",
  deja_sin_efecto: "derogada",
  modifica: "modificada",
  sustituye: "modificada",
  incorpora: "modificada",
  suspende: "suspendida",
  exonera: undefined,
  prorroga: undefined,
  pendiente_verificacion: undefined,
};

export function esStub(fila: FilaNorma): boolean {
  return String(fila.process_status ?? "").startsWith("stub") || fila.document_key.startsWith("NORM-");
}

/** La afectacion alcanza solo una parte de la norma (articulos, numerales,
 * anexos). Derogar el articulo 9 de una ley NO deroga la ley. */
export function esAfectacionParcial(relacion: RelacionParaDecidir): boolean {
  return relacion.alcance === "parcial" || Boolean(relacion.articulos_afectados);
}

/** document_key canonico de un stub: se construye desde la IDENTIDAD, nunca
 * desde la descripcion redactada por el modelo. */
export function documentKeyStubCanonico(ident: NormaIdentity): string | null {
  if (!ident.tipo || !ident.numero) return null;
  return textoIdentidad(ident);
}

export function decidirVinculoNorma(
  relacion: RelacionParaDecidir,
  candidatas: FilaNorma[],
): DecisionVinculo {
  const identidad = construirIdentidad(
    relacion.tipo_norma_afectada,
    relacion.numero_afectada,
    relacion.anio_afectada,
  );
  const parcial = esAfectacionParcial(relacion);
  const estadoPorTipo = ESTADO_VIGENCIA_POR_RELACION[relacion.tipo_relacion];
  const estadoVigencia = parcial ? null : (estadoPorTipo ?? null);
  const base = {
    estadoVigencia,
    bloqueadoPorAlcanceParcial: parcial && Boolean(estadoPorTipo),
    identidad,
    candidatas: [] as string[],
  };

  if (relacion.norma_afectada_id) {
    return {
      ...base,
      accion: "ya_vinculada",
      normaId: relacion.norma_afectada_id,
      documentKeyStub: null,
      motivo: "la relacion ya apunta a una norma; no se crea nada",
    };
  }

  // Un stub jamas compite con una norma real ni la vuelve ambigua.
  const reales = candidatas.filter((f) => !esStub(f));
  const resolucion = resolverIdentidad(identidad, reales);

  if (resolucion.resuelta && resolucion.norma) {
    return {
      ...base,
      accion: "vincular_norma_real",
      normaId: resolucion.norma.id,
      documentKeyStub: null,
      motivo: `norma real encontrada (${resolucion.nivel}, confianza ${resolucion.confianza})`,
    };
  }

  if (resolucion.nivel === AMBIGUA) {
    return {
      ...base,
      accion: "abortar_ambigua",
      normaId: null,
      documentKeyStub: null,
      candidatas: resolucion.candidatas.map((c) => c.document_key),
      motivo: "varias candidatas: elegir una seria adivinar",
    };
  }

  if (resolucion.nivel === DATOS_INSUFICIENTES) {
    return {
      ...base,
      accion: "abortar_datos_insuficientes",
      normaId: null,
      documentKeyStub: null,
      motivo: "la cita no identifica la norma (falta el numero)",
    };
  }

  const documentKeyStub = documentKeyStubCanonico(identidad);
  if (!documentKeyStub) {
    return {
      ...base,
      accion: "abortar_datos_insuficientes",
      normaId: null,
      documentKeyStub: null,
      motivo: "sin tipo y numero canonicos no se da de alta una norma nueva",
    };
  }

  if (!relacion.fragmento_verificado) {
    return {
      ...base,
      accion: "abortar_sin_evidencia",
      normaId: null,
      documentKeyStub,
      motivo: "la cita no se pudo verificar contra el texto: no hay evidencia para dar de alta una norma",
    };
  }

  return {
    ...base,
    accion: "crear_stub",
    normaId: null,
    documentKeyStub,
    motivo: "no existe norma real equivalente y la cita esta verificada",
  };
}
