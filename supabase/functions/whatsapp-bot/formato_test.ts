/** Formato de salida para WhatsApp.
 *
 * Lo que se prueba aqui no es estetica: si una etiqueta HTML de Telegram
 * llega sin convertir, el usuario ve "<b>1 UIT</b>" en medio de una cita
 * normativa. Tambien se verifica que /recientes no mezcle la fecha de
 * publicacion oficial con la fecha en que RegAlert registro el documento.
 */

import { assert, assertEquals, assertStringIncludes } from "jsr:@std/assert@1";
import {
  aFormatoWhatsApp,
  formatAlertListWhatsApp,
  formatConsultaWhatsApp,
  formatNormativaListWhatsApp,
  formatRecentAlertListWhatsApp,
  TEXTO_AYUDA,
  TEXTO_MENU,
} from "./formato.ts";

Deno.test("el HTML de Telegram se convierte a marcado de WhatsApp", () => {
  assertEquals(aFormatoWhatsApp("<b>0.5 UIT</b>"), "*0.5 UIT*");
  assertEquals(aFormatoWhatsApp("<i>vigente</i>"), "_vigente_");
  assertEquals(aFormatoWhatsApp("<code>/detalle 75-2026</code>"), "`/detalle 75-2026`");
  assertEquals(aFormatoWhatsApp("**negrita markdown**"), "*negrita markdown*");
});

Deno.test("ninguna etiqueta HTML llega visible al usuario", () => {
  const salida = aFormatoWhatsApp(
    '<b>Artículo 30</b><br><span class="x">detalle</span><a href="http://x">link</a>',
  );

  assert(!salida.includes("<"), `quedaron etiquetas: ${salida}`);
  assert(!salida.includes(">"), `quedaron etiquetas: ${salida}`);
  assertStringIncludes(salida, "*Artículo 30*");
});

Deno.test("las entidades HTML escapadas se muestran como el carácter real", () => {
  assertEquals(aFormatoWhatsApp("Droguería &amp; Botica"), "Droguería & Botica");
  assertEquals(aFormatoWhatsApp("temperatura &lt; 25°C"), "temperatura < 25°C");
});

Deno.test("el menú ofrece las opciones del MVP numeradas", () => {
  for (const opcion of ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣"]) {
    assertStringIncludes(TEXTO_MENU, opcion);
  }
  assertStringIncludes(TEXTO_MENU, "Últimas alertas");
  assertStringIncludes(TEXTO_MENU, "Consulta con IA");
  assert(!TEXTO_MENU.includes("<b>"), "el menú no debe llevar HTML");
});

Deno.test("la ayuda advierte que no reemplaza al Director Técnico", () => {
  assertStringIncludes(TEXTO_AYUDA, "Director Técnico");
  assert(!TEXTO_AYUDA.includes("<b>"));
});

Deno.test("una lista vacía lo dice en vez de inventar alertas", () => {
  const salida = formatAlertListWhatsApp("🚨 Últimas alertas DIGEMID", []);

  assertStringIncludes(salida, "No encontré alertas");
  assert(!salida.includes("Total mostrado"));
});

Deno.test("la lista de alertas muestra número, fecha y enlace oficial", () => {
  const salida = formatAlertListWhatsApp("🚨 Últimas alertas DIGEMID", [{
    alert_number: "75-2026",
    alert_title: "Producto falsificado",
    published_date: "2026-09-01",
    published_date_display: "01/09/2026",
    detail_url: "https://www.digemid.minsa.gob.pe/alerta-75",
  }]);

  assertStringIncludes(salida, "*75-2026*");
  assertStringIncludes(salida, "Producto falsificado");
  assertStringIncludes(salida, "01/09/2026");
  assertStringIncludes(salida, "https://www.digemid.minsa.gob.pe/alerta-75");
  assert(!salida.includes("<b>"));
});

Deno.test("/recientes distingue fecha de publicación de fecha de registro", () => {
  // published_date es el hecho oficial; created_at es cuándo lo registró
  // RegAlert. Presentarlos como lo mismo induce a error jurídico.
  const salida = formatRecentAlertListWhatsApp([{
    document_key: "75-2026",
    title: "Producto falsificado",
    published_date_display: "01/09/2026",
    created_at: "2026-09-03T15:04:00.000Z",
    detail_url: "https://www.digemid.minsa.gob.pe/alerta-75",
  }]);

  assertStringIncludes(salida, "Fecha publicada: 01/09/2026");
  assertStringIncludes(salida, "Registrada:");
  assert(
    salida.indexOf("Fecha publicada") < salida.indexOf("Registrada"),
    "la fecha oficial debe ir primero",
  );
});

Deno.test("la normativa muestra el tipo derivado del document_key", () => {
  const salida = formatNormativaListWhatsApp("📚 Últimas normas publicadas", [
    { document_key: "DS-20-2024", title: "Modifica el Anexo 01", published_date: "2024-10-25" },
    { document_key: "LEY-29459", title: "Ley de productos farmacéuticos", published_date: "2009-11-26" },
  ]);

  assertStringIncludes(salida, "*Decreto Supremo DS-20-2024*");
  assertStringIncludes(salida, "*Ley LEY-29459*");
});

Deno.test("la respuesta de consulta conserva la cita y agrega los documentos", () => {
  const salida = formatConsultaWhatsApp(
    "<b>La sanción es 0.5 UIT.</b>\n\n📌 Fuente: <b>DS-20-2024</b> — 25/10/2024, pag. 5",
    [{ documentKey: "DS-20-2024", url: "https://www.digemid.minsa.gob.pe/ds-20-2024" }],
  );

  assertStringIncludes(salida, "*La sanción es 0.5 UIT.*");
  assertStringIncludes(salida, "📌 Fuente: *DS-20-2024*");
  assertStringIncludes(salida, "pag. 5");
  assertStringIncludes(salida, "https://www.digemid.minsa.gob.pe/ds-20-2024");
  assert(!salida.includes("<b>"));
});

Deno.test("sin fuentes no se inventa una sección de documentos", () => {
  const salida = formatConsultaWhatsApp("No encontré documentos relacionados.", []);

  assert(!salida.includes("Documentos:"));
});
