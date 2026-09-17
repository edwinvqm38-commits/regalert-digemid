/** Normalizacion de lo que el usuario escribe en WhatsApp.
 *
 * Cubre los casos 8 a 11 y 16 de la lista de pruebas del MVP: "/menu" y
 * "menu" son el mismo comando, "/buscar paracetamol" y "buscar paracetamol"
 * tambien, y el texto vacio no dispara nada.
 */

import { assertEquals } from "jsr:@std/assert@1";
import { parsearComando, parsearComandoAdmin } from "./comandos.ts";

Deno.test("'/menu' se normaliza a menu", () => {
  assertEquals(parsearComando("/menu").comando, "menu");
});

Deno.test("'menu' sin barra se normaliza a menu", () => {
  assertEquals(parsearComando("menu").comando, "menu");
});

Deno.test("mayusculas y tildes no cambian el comando", () => {
  assertEquals(parsearComando("MENÚ").comando, "menu");
  assertEquals(parsearComando("Últimas").comando, "ultimas");
});

Deno.test("saludos comunes abren el menu", () => {
  for (const saludo of ["hola", "Hola", "inicio", "buenos dias", "Buenas tardes"]) {
    assertEquals(parsearComando(saludo).comando, "menu", `falló con "${saludo}"`);
  }
});

Deno.test("'/buscar paracetamol' separa comando y argumento", () => {
  const resultado = parsearComando("/buscar paracetamol");
  assertEquals(resultado.comando, "buscar");
  assertEquals(resultado.argumento, "paracetamol");
});

Deno.test("'buscar paracetamol' sin barra se comporta igual", () => {
  const resultado = parsearComando("buscar paracetamol");
  assertEquals(resultado.comando, "buscar");
  assertEquals(resultado.argumento, "paracetamol");
});

Deno.test("el argumento conserva tildes y mayusculas del usuario", () => {
  // Normalizar el argumento rompería la búsqueda de un principio activo.
  const resultado = parsearComando("buscar Ácido Acetilsalicílico");
  assertEquals(resultado.argumento, "Ácido Acetilsalicílico");
});

Deno.test("'/detalle 75-2026' extrae el numero de alerta", () => {
  const resultado = parsearComando("/detalle 75-2026");
  assertEquals(resultado.comando, "detalle");
  assertEquals(resultado.argumento, "75-2026");
});

Deno.test("'consulta ...' pasa la pregunta completa como argumento", () => {
  const resultado = parsearComando("consulta qué establece la Ley 29459");
  assertEquals(resultado.comando, "consulta");
  assertEquals(resultado.argumento, "qué establece la Ley 29459");
});

Deno.test("las opciones numeradas del menu funcionan", () => {
  assertEquals(parsearComando("1").comando, "ultimas");
  assertEquals(parsearComando("2").comando, "hoy");
  assertEquals(parsearComando("3").comando, "normas");
  assertEquals(parsearComando("4").comando, "buscar");
  assertEquals(parsearComando("5").comando, "consulta");
  assertEquals(parsearComando("6").comando, "miperfil");
  assertEquals(parsearComando("7").comando, "ayuda");
});

Deno.test("comandos simples ignoran texto sobrante", () => {
  assertEquals(parsearComando("ultimas alertas").comando, "ultimas");
  assertEquals(parsearComando("mi perfil").comando, "miperfil");
});

Deno.test("texto vacio no dispara ningun comando", () => {
  assertEquals(parsearComando("").comando, "desconocido");
  assertEquals(parsearComando("   ").comando, "desconocido");
});

Deno.test("una pregunta en lenguaje natural se trata como consulta IA", () => {
  const pregunta = "qué sanción corresponde a una botica sin químico farmacéutico";
  const resultado = parsearComando(pregunta);
  assertEquals(resultado.comando, "consulta");
  assertEquals(resultado.argumento, pregunta);
});

Deno.test("un signo de pregunta basta para tratarlo como consulta", () => {
  assertEquals(parsearComando("¿qué es una droguería?").comando, "consulta");
});

Deno.test("una palabra suelta desconocida NO consume cuota de IA", () => {
  // Se muestra el menú en vez de gastar una consulta del plan del usuario.
  assertEquals(parsearComando("paracetamol").comando, "desconocido");
  assertEquals(parsearComando("asdfgh").comando, "desconocido");
});

Deno.test("'admin usuarios' se reconoce como comando admin", () => {
  assertEquals(parsearComandoAdmin("admin usuarios"), { tipo: "usuarios" });
  assertEquals(parsearComandoAdmin("/admin usuarios"), { tipo: "usuarios" });
  assertEquals(parsearComandoAdmin("ADMIN Usuarios"), { tipo: "usuarios" });
});

Deno.test("'admin vencen' usa 3 dias por defecto", () => {
  assertEquals(parsearComandoAdmin("admin vencen"), { tipo: "vencen", dias: 3 });
});

Deno.test("'admin vencen 7' respeta los dias pedidos", () => {
  assertEquals(parsearComandoAdmin("admin vencen 7"), { tipo: "vencen", dias: 7 });
});

Deno.test("'admin vencen' con un numero fuera de rango se acota", () => {
  assertEquals(parsearComandoAdmin("admin vencen 0")?.dias, 1);
  assertEquals(parsearComandoAdmin("admin vencen 999")?.dias, 30);
});

Deno.test("un texto que no es un comando admin devuelve null", () => {
  assertEquals(parsearComandoAdmin("administracion"), null);
  assertEquals(parsearComandoAdmin("admin"), null);
  assertEquals(parsearComandoAdmin("admin borrartodo"), null);
  assertEquals(parsearComandoAdmin("ultimas"), null);
  assertEquals(parsearComandoAdmin(""), null);
});
