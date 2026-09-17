/** Normalizacion de lo que escribe el usuario en WhatsApp a un comando.
 *
 * En Telegram el usuario tiene un menu de comandos con "/" y botones; en
 * WhatsApp escribe texto suelto. Por eso aqui se aceptan las tres formas de
 * pedir lo mismo: "/ultimas", "ultimas" y el numero de la opcion del menu
 * ("1"). Modulo puro: sin red, sin Supabase, sin variables de entorno.
 */

export type ComandoWhatsApp =
  | "menu"
  | "ayuda"
  | "ultimas"
  | "hoy"
  | "semana"
  | "mes"
  | "recientes"
  | "buscar"
  | "normas"
  | "consulta"
  | "detalle"
  | "miperfil"
  | "desconocido";

export type ComandoParseado = {
  comando: ComandoWhatsApp;
  /** Texto que sigue al comando ("paracetamol" en "buscar paracetamol"). */
  argumento: string;
  textoOriginal: string;
};

/** Minusculas y sin tildes, para que "MENÚ" y "menu" sean lo mismo. */
export function normalizarTexto(texto: string): string {
  return texto
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/** Sinonimos naturales: lo que la gente escribe de verdad al abrir un chat. */
const ALIAS: Record<string, ComandoWhatsApp> = {
  "menu": "menu",
  "start": "menu",
  "inicio": "menu",
  "hola": "menu",
  "buenas": "menu",
  "buenos dias": "menu",
  "buenas tardes": "menu",
  "buenas noches": "menu",
  "ayuda": "ayuda",
  "help": "ayuda",
  "ultimas": "ultimas",
  "ultimas alertas": "ultimas",
  "alertas": "ultimas",
  "hoy": "hoy",
  "semana": "semana",
  "esta semana": "semana",
  "mes": "mes",
  "este mes": "mes",
  "recientes": "recientes",
  "buscar": "buscar",
  "normas": "normas",
  "normativa": "normas",
  "consulta": "consulta",
  "consultar": "consulta",
  "detalle": "detalle",
  "miperfil": "miperfil",
  "mi perfil": "miperfil",
  "perfil": "miperfil",
};

/** Opciones numeradas del menu de texto (§10): en un celular es mas comodo
 * responder "1" que escribir el comando completo. El orden debe coincidir
 * con el del menu que arma formato.ts. */
const OPCIONES_MENU: Record<string, ComandoWhatsApp> = {
  "1": "ultimas",
  "2": "hoy",
  "3": "normas",
  "4": "buscar",
  "5": "consulta",
  "6": "miperfil",
  "7": "ayuda",
};

/** Comandos que esperan un argumento despues de la palabra clave. */
const COMANDOS_CON_ARGUMENTO: ReadonlySet<ComandoWhatsApp> = new Set([
  "buscar",
  "consulta",
  "detalle",
]);

/** Un texto libre que no es ningun comando conocido se trata como consulta
 * IA solo si "parece pregunta" (termina en "?" o tiene al menos 5 palabras).
 * Con menos que eso se muestra el menu: asi una palabra suelta mal escrita
 * no consume cuota de consultas IA del usuario sin que el lo pidiera. */
export function pareceConsultaEnLenguajeNatural(texto: string): boolean {
  const limpio = texto.trim();
  if (!limpio) return false;
  if (limpio.endsWith("?") || limpio.startsWith("¿")) return true;
  return limpio.split(/\s+/).filter(Boolean).length >= 5;
}

export function parsearComando(textoEntrante: string): ComandoParseado {
  const textoOriginal = (textoEntrante ?? "").trim();

  if (!textoOriginal) {
    return { comando: "desconocido", argumento: "", textoOriginal };
  }

  // El "/" es opcional en WhatsApp: "/buscar x" y "buscar x" son iguales.
  const sinBarra = textoOriginal.startsWith("/") ? textoOriginal.slice(1).trim() : textoOriginal;
  const normalizado = normalizarTexto(sinBarra);

  const opcionMenu = OPCIONES_MENU[normalizado];
  if (opcionMenu) {
    return { comando: opcionMenu, argumento: "", textoOriginal };
  }

  // Alias de frase completa ("mi perfil", "buenos dias") antes de partir por
  // palabras, porque algunos llevan espacio.
  const aliasExacto = ALIAS[normalizado];
  if (aliasExacto) {
    return { comando: aliasExacto, argumento: "", textoOriginal };
  }

  const separador = sinBarra.search(/\s/);
  const primeraPalabra = separador === -1 ? sinBarra : sinBarra.slice(0, separador);
  // El argumento se toma del texto ORIGINAL (sin normalizar): una busqueda
  // debe conservar tildes y mayusculas tal como las escribio el usuario.
  const argumento = separador === -1 ? "" : sinBarra.slice(separador + 1).trim();

  const comandoPorPalabra = ALIAS[normalizarTexto(primeraPalabra)];

  if (comandoPorPalabra) {
    if (COMANDOS_CON_ARGUMENTO.has(comandoPorPalabra)) {
      return { comando: comandoPorPalabra, argumento, textoOriginal };
    }
    // "ultimas alertas de hoy" no lleva argumento: se ignora el resto.
    return { comando: comandoPorPalabra, argumento: "", textoOriginal };
  }

  if (pareceConsultaEnLenguajeNatural(textoOriginal)) {
    return { comando: "consulta", argumento: textoOriginal, textoOriginal };
  }

  return { comando: "desconocido", argumento: "", textoOriginal };
}

/** Comandos de administracion (§ solo accesibles para wa_id en la lista de
 * admins, verificado en index.ts). Se parsean aparte de parsearComando()
 * para que un usuario normal que escriba "admin algo" no dispare nada por
 * accidente: sin autorizacion, este resultado simplemente no se usa. */
export type ComandoAdmin =
  | { tipo: "usuarios" }
  | { tipo: "vencen"; dias: number };

const VENCEN_DIAS_DEFECTO = 3;
const VENCEN_DIAS_MAXIMO = 30;

export function parsearComandoAdmin(textoEntrante: string): ComandoAdmin | null {
  const sinBarra = (textoEntrante ?? "").trim().replace(/^\//, "").trim();
  const normalizado = normalizarTexto(sinBarra);

  if (!normalizado.startsWith("admin")) return null;

  const resto = normalizado.slice("admin".length).trim();

  if (resto === "usuarios") return { tipo: "usuarios" };

  const matchVencen = resto.match(/^vencen(?:\s+(\d+))?$/);
  if (matchVencen) {
    const diasPedidos = matchVencen[1] ? parseInt(matchVencen[1], 10) : VENCEN_DIAS_DEFECTO;
    return { tipo: "vencen", dias: Math.max(1, Math.min(diasPedidos, VENCEN_DIAS_MAXIMO)) };
  }

  return null;
}
