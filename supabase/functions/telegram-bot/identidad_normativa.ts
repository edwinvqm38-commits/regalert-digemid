// Capa canonica de IDENTIDAD NORMATIVA para el bot (H-05/H-06/H-08).
//
// Es la traduccion 1:1 de scripts/identidad_normativa.py. NO es un segundo
// criterio: la tabla de tipos y las constantes vienen del mismo archivo
// (config/identidad_normativa.spec.json) y tests/test_paridad_identidad.py
// ejecuta los MISMOS casos contra los dos motores y falla si difieren en algo.
//
// Existe porque el bot creaba stubs con logica propia: buscaba por
// document_key armado a mano ("LEY"+"29459"+año), y como la cita de una ley
// no trae año, caia a un slug de la descripcion y creaba un stub aunque la
// LEY-29459 real ya estuviera en la base. Ese es el origen del stub
// NORM-LEY-29459-... que hoy tiene huerfana la exoneracion de la Ley 32319.

import {
  ANIO_MAXIMO,
  ANIO_MINIMO,
  ANIO_PIVOTE_DOS_DIGITOS,
  TIPOS_CANONICOS,
} from "./identidad_spec.generated.ts";

export const NIVEL_EXACTA = "RESUELTA_EXACTA";
export const NIVEL_TIPO_NUMERO_ANIO = "RESUELTA_TIPO_NUMERO_ANIO";
export const NIVEL_NUMERO_ANIO = "RESUELTA_NUMERO_ANIO";
export const NIVEL_TIPO_NUMERO = "RESUELTA_TIPO_NUMERO";
export const AMBIGUA = "IDENTIDAD_AMBIGUA";
export const NO_ENCONTRADA = "NORMA_NO_ENCONTRADA";
export const DATOS_INSUFICIENTES = "DATOS_INSUFICIENTES";

const NIVELES_RESUELTOS = new Set([
  NIVEL_EXACTA,
  NIVEL_TIPO_NUMERO_ANIO,
  NIVEL_NUMERO_ANIO,
  NIVEL_TIPO_NUMERO,
]);

const CONFIANZA: Record<string, string> = {
  [NIVEL_EXACTA]: "alta",
  [NIVEL_TIPO_NUMERO_ANIO]: "alta",
  [NIVEL_TIPO_NUMERO]: "media",
  [NIVEL_NUMERO_ANIO]: "media",
  [AMBIGUA]: "nula",
  [NO_ENCONTRADA]: "nula",
  [DATOS_INSUFICIENTES]: "nula",
};

export interface NormaIdentity {
  tipo: string | null;
  numero: string | null;
  anio: number | null;
  sector: string | null;
}

export interface FilaNorma {
  id: string;
  document_key: string;
  tipo_norma: string | null;
  numero: string | null;
  anio: number | null;
  process_status?: string | null;
  estado_vigencia?: string | null;
}

export interface ResultadoIdentidad {
  nivel: string;
  norma: FilaNorma | null;
  candidatas: FilaNorma[];
  resuelta: boolean;
  confianza: string;
}

function sinAcentos(valor: string): string {
  return valor.normalize("NFD").replace(/[̀-ͯ]/g, "");
}

/** Abreviatura canonica, o null si no se reconoce. NO adivina. */
export function normalizarTipoNorma(valor: unknown): string | null {
  if (valor === null || valor === undefined) return null;
  const base = sinAcentos(String(valor)).toLowerCase();
  // Dos lecturas, porque el punto significa cosas distintas: en "R.M." separa
  // iniciales ("rm"); en "Resolucion Ministerial." es puntuacion final.
  const candidatos = [
    base.replace(/\./g, "").replace(/\s+/g, " ").trim(),
    base.replace(/\./g, " ").replace(/\s+/g, " ").trim(),
  ];
  for (const plano of candidatos) {
    if (plano && TIPOS_CANONICOS[plano]) return TIPOS_CANONICOS[plano];
  }
  return null;
}

/** Solo el primer grupo de digitos, sin ceros a la izquierda: "014" === "14". */
export function normalizarNumero(valor: unknown): string | null {
  if (valor === null || valor === undefined) return null;
  const m = String(valor).match(/\d+/);
  return m ? String(parseInt(m[0], 10)) : null;
}

function anioCompleto(fragmento: string | null | undefined): number | null {
  if (!fragmento) return null;
  const n = parseInt(fragmento, 10);
  if (fragmento.length === 4) return n >= ANIO_MINIMO && n <= ANIO_MAXIMO ? n : null;
  if (fragmento.length === 2) return n >= ANIO_PIVOTE_DOS_DIGITOS ? 1900 + n : 2000 + n;
  return null;
}

export function normalizarSector(valor: unknown): string | null {
  if (!valor) return null;
  const plano = sinAcentos(String(valor)).toUpperCase().trim().replace(/^[-/]+|[-/]+$/g, "");
  // "SA/DM" y "SA-DM" son el mismo sector escrito distinto (ver el modulo Python).
  const limpio = plano.replace(/\//g, "-").replace(/[^A-Z0-9-]/g, "");
  return limpio || null;
}

// "014-2011-SA" -> 14 / 2011 / SA ; "354-99-DG-DIGEMID" -> 354 / 1999 / DG-DIGEMID
const PATRON_NUMERO =
  /^\s*(\d{1,6})(?:\s*[-/]\s*(\d{2,4}))?(?:\s*[-/]\s*([A-Za-zÁÉÍÓÚÑ][\w\-/]*))?/;

export function construirIdentidad(
  tipo: unknown,
  numero: unknown,
  anio?: number | null,
  sector?: unknown,
): NormaIdentity {
  let anioEmbebido: number | null = null;
  let sectorEmbebido: string | null = null;
  if (numero !== null && numero !== undefined) {
    const m = String(numero).match(PATRON_NUMERO);
    if (m) {
      anioEmbebido = anioCompleto(m[2]);
      sectorEmbebido = m[3] ?? null;
    }
  }
  return {
    tipo: normalizarTipoNorma(tipo),
    numero: normalizarNumero(numero),
    anio: anio !== null && anio !== undefined ? anio : anioEmbebido,
    sector: normalizarSector(sector !== null && sector !== undefined ? sector : sectorEmbebido),
  };
}

export function identidadDeNorma(fila: FilaNorma): NormaIdentity {
  return construirIdentidad(fila.tipo_norma, fila.numero, fila.anio);
}

export function esUtilizable(ident: NormaIdentity): boolean {
  return Boolean(ident.numero);
}

export function claveIdentidad(ident: NormaIdentity): string {
  return [
    ident.tipo ?? "?",
    ident.numero ?? "?",
    ident.anio ? String(ident.anio) : "?",
    ident.sector ?? "",
  ].join("|");
}

export function textoIdentidad(ident: NormaIdentity): string {
  const partes = [ident.tipo ?? "?", ident.numero ?? "?"];
  if (ident.anio) partes.push(String(ident.anio));
  if (ident.sector) partes.push(ident.sector);
  return partes.join("-");
}

function resultado(nivel: string, norma: FilaNorma | null, candidatas: FilaNorma[] = []): ResultadoIdentidad {
  return {
    nivel,
    norma,
    candidatas,
    resuelta: NIVELES_RESUELTOS.has(nivel) && norma !== null,
    confianza: CONFIANZA[nivel] ?? "nula",
  };
}

/** Resolucion JERARQUICA y conservadora. Regla de oro: mas de una candidata en
 * cualquier nivel devuelve IDENTIDAD_AMBIGUA con la lista completa. Nunca se
 * elige "la primera". */
export function resolverIdentidad(citada: NormaIdentity, catalogo: FilaNorma[]): ResultadoIdentidad {
  if (!esUtilizable(citada)) return resultado(DATOS_INSUFICIENTES, null);

  const indexado = catalogo.map((f) => ({ ident: identidadDeNorma(f), fila: f }));

  const elegir = (candidatas: FilaNorma[], nivel: string): ResultadoIdentidad | null => {
    if (candidatas.length === 1) return resultado(nivel, candidatas[0]);
    if (candidatas.length > 1) return resultado(AMBIGUA, null, candidatas);
    return null;
  };

  // NIVEL 1: tipo + numero + año + sector
  if (citada.tipo && citada.anio && citada.sector) {
    const exactas = indexado
      .filter(({ ident }) =>
        ident.tipo === citada.tipo && ident.numero === citada.numero &&
        ident.anio === citada.anio && ident.sector === citada.sector
      )
      .map(({ fila }) => fila);
    const r = elegir(exactas, NIVEL_EXACTA);
    if (r) return r;
  }

  // NIVEL 2: tipo + numero + año
  if (citada.tipo && citada.anio) {
    const porTipoAnio = indexado
      .filter(({ ident }) =>
        ident.tipo === citada.tipo && ident.numero === citada.numero && ident.anio === citada.anio
      )
      .map(({ fila }) => fila);
    const r = elegir(porTipoAnio, NIVEL_TIPO_NUMERO_ANIO);
    if (r) return r;
  }

  // NIVEL 4 (antes que el 3): tipo + numero, SOLO si la cita no trae año.
  // Caso "Ley 29459". Nunca se inventa el año.
  if (citada.tipo && !citada.anio) {
    const porTipo = indexado
      .filter(({ ident }) => ident.tipo === citada.tipo && ident.numero === citada.numero)
      .map(({ fila }) => fila);
    const r = elegir(porTipo, NIVEL_TIPO_NUMERO);
    if (r) return r;
  }

  // NIVEL 3: numero + año, SIN tipo
  if (citada.anio) {
    const porNumeroAnio = indexado
      .filter(({ ident }) => ident.numero === citada.numero && ident.anio === citada.anio);
    const compatibles = porNumeroAnio
      .filter(({ ident }) => !citada.tipo || ident.tipo === null || ident.tipo === citada.tipo)
      .map(({ fila }) => fila);
    const r = elegir(compatibles, NIVEL_NUMERO_ANIO);
    if (r) return r;
    if (porNumeroAnio.length) return resultado(AMBIGUA, null, porNumeroAnio.map(({ fila }) => fila));
  }

  return resultado(NO_ENCONTRADA, null);
}

/** Forma comparable de un texto libre: sin acentos, sin mayusculas y sin
 * puntuacion. Absorbe diferencias menores de redaccion. */
function slug(texto: unknown): string {
  return sinAcentos(String(texto ?? "").toLowerCase())
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/** Conjunto de unidades afectadas, ignorando la redaccion: "articulos 10 y 11",
 * "arts. 10 y 11" y "10, 11" dan lo mismo; 10 y 12 siguen siendo distintos. */
export function normalizarArticulos(valor: unknown): string {
  if (!valor) return "";
  const unidades = String(valor).match(/\d+(?:\.\d+)*/g) ?? [];
  const unicos = Array.from(new Set(unidades));
  unicos.sort((a, b) => {
    const pa = a.split(".").map(Number);
    const pb = b.split(".").map(Number);
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
      const va = pa[i] ?? -1;
      const vb = pb[i] ?? -1;
      if (va !== vb) return va - vb;
    }
    return 0;
  });
  return unicos.join(",");
}

/** Clave estable de una relacion. El FRAGMENTO no participa: es evidencia, no
 * identidad. La descripcion solo se usa si la identidad no pudo construirse. */
export function claveDedupe(
  normaOrigenId: string,
  tipoRelacion: string | null,
  identidadAfectada: NormaIdentity,
  articulosAfectados?: unknown,
  descripcionAfectada?: string | null,
): string {
  const parteAfectada = esUtilizable(identidadAfectada)
    ? claveIdentidad(identidadAfectada)
    : "desc:" + slug(descripcionAfectada).slice(0, 80);

  // Discriminador del OBJETO afectado cuando no hay unidades explicitas: sin
  // el, dos afectaciones distintas a la misma norma colapsan en una sola clave
  // (ver el modulo Python para el caso real que lo motivo).
  let parteUnidades = normalizarArticulos(articulosAfectados);
  if (!parteUnidades) parteUnidades = "obj:" + slug(descripcionAfectada).slice(0, 60);

  return [
    String(normaOrigenId),
    (tipoRelacion ?? "").toLowerCase(),
    parteAfectada,
    parteUnidades,
  ].join("::");
}
