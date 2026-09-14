/** Canal WhatsApp Business (Meta Cloud API) de DIGEMID RegAlert — MVP.
 *
 * ALCANCE: exclusivamente consultas iniciadas por el usuario.
 *   usuario escribe -> RegAlert procesa -> RegAlert responde.
 *
 * Esta funcion NUNCA inicia una conversacion: no hay campanas, broadcasts,
 * recordatorios, alertas automaticas ni message templates. El envio pasa
 * siempre por sendWhatsAppServiceMessage(), que solo acepta responder al
 * mensaje entrante que se esta procesando (ver whatsapp-api.ts).
 *
 * Reutiliza la base documental, el RAG y las consultas de DIGEMID que ya usa
 * el bot de Telegram (supabase/functions/_shared/), sin modificar ese canal.
 */

import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "@supabase/supabase-js";

import {
  getAlertasSemana,
  getAlertDetail,
  getLatestAlerts,
  getLatestNormativa,
  getMonthAlerts,
  getRecentAlerts,
  getTodayAlerts,
  searchAlerts,
} from "../_shared/digemid-datos.ts";
import { answerConsulta } from "../_shared/consulta-ia.ts";
import { type ComandoAdmin, type ComandoParseado, parsearComando, parsearComandoAdmin } from "./comandos.ts";
import {
  cerrarMensaje,
  contarConsultasIaHoy,
  listarUsuariosPorVencer,
  listarUsuariosRecientes,
  logConsulta,
  reservarMensaje,
  upsertUsuarioWhatsApp,
  usuarioTieneAcceso,
} from "./persistencia.ts";
import {
  extraerMensajeEntrante,
  firmaValida,
  type MensajeEntranteWhatsApp,
  truncarParaLog,
  verificarWebhookGet,
} from "./webhook.ts";
import {
  type ConfigWhatsApp,
  type ContextoInbound,
  crearContextoInbound,
  sendWhatsAppServiceMessage,
} from "./whatsapp-api.ts";
import {
  formatAdminUsuariosWhatsApp,
  formatAdminVencenWhatsApp,
  formatAlertDetailWhatsApp,
  formatAlertListWhatsApp,
  formatConsultaWhatsApp,
  formatNormativaListWhatsApp,
  formatRecentAlertListWhatsApp,
  formatWeekAlertListWhatsApp,
  negrita,
  TEXTO_AYUDA,
  TEXTO_MENU,
  TEXTO_NO_RECONOCIDO,
  TEXTO_PRUEBA_VENCIDA,
  TEXTO_SOLO_TEXTO,
} from "./formato.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ??
  JSON.parse(Deno.env.get("SUPABASE_SECRET_KEYS") ?? "{}").service_role ??
  "";

const WHATSAPP_ACCESS_TOKEN = Deno.env.get("WHATSAPP_ACCESS_TOKEN") ?? "";
const WHATSAPP_PHONE_NUMBER_ID = Deno.env.get("WHATSAPP_PHONE_NUMBER_ID") ?? "";
const WHATSAPP_VERIFY_TOKEN = Deno.env.get("WHATSAPP_VERIFY_TOKEN") ?? "";
const WHATSAPP_APP_SECRET = Deno.env.get("WHATSAPP_APP_SECRET") ?? "";
// Version de Graph API configurable: fijar una antigua en el codigo obliga a
// un cambio de codigo cada vez que Meta deja de soportarla.
const WHATSAPP_GRAPH_API_VERSION = Deno.env.get("WHATSAPP_GRAPH_API_VERSION")?.trim() ||
  "v21.0";
const WHATSAPP_OUTBOUND_MODE = Deno.env.get("WHATSAPP_OUTBOUND_MODE") ?? "";
// wa_id (numero sin "+") de quienes pueden usar comandos "admin ...". Sigue
// siendo inbound-only: el admin escribe, se le responde a el; nadie recibe
// nada que no haya pedido.
const WHATSAPP_ADMIN_WA_IDS = new Set(
  (Deno.env.get("WHATSAPP_ADMIN_WA_IDS") ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean),
);

const DEEPSEEK_API_KEY = Deno.env.get("DEEPSEEK_API_KEY") ?? "";
const GEMINI_API_KEY = Deno.env.get("GEMINI_API_KEY") ?? "";
const GEMINI_MODEL = "gemini-flash-latest";

/** Mismos limites diarios de consulta IA por nivel que usa Telegram. */
const NIVEL_LIMITES_DIARIOS: Record<string, number | null> = {
  gratis: 5,
  basico: 30,
  consultoria: 100,
  empresarial: null,
};

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const configWhatsApp: ConfigWhatsApp = {
  accessToken: WHATSAPP_ACCESS_TOKEN,
  phoneNumberId: WHATSAPP_PHONE_NUMBER_ID,
  graphApiVersion: WHATSAPP_GRAPH_API_VERSION,
  outboundMode: WHATSAPP_OUTBOUND_MODE,
};

async function responder(
  contexto: ContextoInbound,
  texto: string,
): Promise<void> {
  await sendWhatsAppServiceMessage(contexto, texto, configWhatsApp);
}

async function ejecutarConsultaIa(
  contexto: ContextoInbound,
  waId: string,
  nivel: string,
  pregunta: string,
): Promise<void> {
  if (!pregunta.trim()) {
    return await responder(
      contexto,
      [
        negrita("🤖 Consulta con IA"),
        "",
        "Escríbeme tu pregunta después de la palabra `consulta`. Por ejemplo:",
        "`consulta qué establece la Ley 29459`",
      ].join("\n"),
    );
  }

  const limite = NIVEL_LIMITES_DIARIOS[nivel] ?? NIVEL_LIMITES_DIARIOS.gratis;

  if (limite !== null) {
    const usadas = await contarConsultasIaHoy(supabase, waId);
    if (usadas >= limite) {
      await logConsulta(supabase, { waId, command: "/consulta", queryText: pregunta, status: "limite" });
      return await responder(
        contexto,
        [
          negrita("🚦 Llegaste a tu límite diario de consultas con IA"),
          "",
          `Tu plan permite ${limite} consultas por día. Vuelve mañana o escribe ` +
          "`miperfil` para ver tu plan.",
          "",
          "Mientras tanto puedes usar `ultimas`, `buscar` o `normas` sin límite.",
        ].join("\n"),
      );
    }
  }

  const { answer, sources, sinEvidencia } = await answerConsulta(supabase, pregunta, {
    deepseekApiKey: DEEPSEEK_API_KEY,
    geminiApiKey: GEMINI_API_KEY,
    geminiModel: GEMINI_MODEL,
    estiloNegrita: "whatsapp",
  });

  await logConsulta(supabase, {
    waId,
    command: "/consulta",
    queryText: pregunta,
    resultCount: sources.length,
    status: sinEvidencia ? "sin_evidencia" : "ok",
  });

  const cuerpo = formatConsultaWhatsApp(answer, sources);
  const cierre = sinEvidencia
    ? "\n\n⚠️ No hay sustento documental suficiente en la base para responder esto. " +
      "Verifica directamente con DIGEMID antes de tomar cualquier decisión."
    : "\n\n📌 Respuesta informativa basada en los documentos citados. No reemplaza al " +
      "Químico Farmacéutico Director Técnico ni a DIGEMID.";

  await responder(contexto, `${cuerpo}${cierre}`);
}

async function enviarMiPerfil(
  contexto: ContextoInbound,
  mensaje: MensajeEntranteWhatsApp,
  nivel: string,
): Promise<void> {
  const limite = NIVEL_LIMITES_DIARIOS[nivel] ?? NIVEL_LIMITES_DIARIOS.gratis;
  const usadas = await contarConsultasIaHoy(supabase, mensaje.waId);

  const lineas = [
    negrita("👤 Tu perfil"),
    "",
    `Nombre: ${mensaje.nombrePerfil || "sin nombre de perfil"}`,
    `Canal: WhatsApp`,
    `Plan: ${negrita(nivel)}`,
  ];

  lineas.push(
    limite === null
      ? "Consultas IA: sin límite diario"
      : `Consultas IA hoy: ${usadas}/${limite}`,
  );

  lineas.push(
    "",
    "ℹ️ Este canal responde solo cuando tú escribes: RegAlert no te enviará mensajes por su cuenta.",
  );

  await responder(contexto, lineas.join("\n"));
}

/** Comandos de administracion: solo se llega aqui si el wa_id ya pasó el
 * filtro de WHATSAPP_ADMIN_WA_IDS en procesarMensaje(). Es de solo lectura:
 * ninguna rama envía nada a otro usuario ni cambia su nivel. */
async function ejecutarComandoAdmin(
  contexto: ContextoInbound,
  comando: ComandoAdmin,
): Promise<void> {
  if (comando.tipo === "usuarios") {
    const usuarios = await listarUsuariosRecientes(supabase, 20);
    return await responder(contexto, formatAdminUsuariosWhatsApp(usuarios));
  }

  const usuarios = await listarUsuariosPorVencer(supabase, comando.dias);
  return await responder(contexto, formatAdminVencenWhatsApp(usuarios, comando.dias));
}

async function ejecutarComando(
  contexto: ContextoInbound,
  mensaje: MensajeEntranteWhatsApp,
  nivel: string,
  parseado: ComandoParseado,
): Promise<void> {
  const { comando, argumento } = parseado;

  switch (comando) {
    case "menu":
      await logConsulta(supabase, { waId: mensaje.waId, command: "/menu", status: "ok" });
      return await responder(contexto, TEXTO_MENU);

    case "ayuda":
      await logConsulta(supabase, { waId: mensaje.waId, command: "/ayuda", status: "ok" });
      return await responder(contexto, TEXTO_AYUDA);

    case "ultimas": {
      const rows = await getLatestAlerts(supabase, 5);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/ultimas",
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(contexto, formatAlertListWhatsApp("🚨 Últimas alertas DIGEMID", rows));
    }

    case "hoy": {
      const rows = await getTodayAlerts(supabase);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/hoy",
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(
        contexto,
        formatAlertListWhatsApp("📅 Alertas publicadas hoy", rows),
      );
    }

    case "semana": {
      const { rows, total } = await getAlertasSemana(supabase, 10);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/semana",
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(contexto, formatWeekAlertListWhatsApp(rows, total));
    }

    case "mes": {
      const rows = await getMonthAlerts(supabase);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/mes",
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(
        contexto,
        formatAlertListWhatsApp("🗓️ Alertas publicadas este mes", rows),
      );
    }

    case "recientes": {
      const rows = await getRecentAlerts(supabase, 10);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/recientes",
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(contexto, formatRecentAlertListWhatsApp(rows));
    }

    case "normas": {
      const rows = await getLatestNormativa(supabase, 8);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/normas",
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(
        contexto,
        formatNormativaListWhatsApp("📚 Últimas normas publicadas", rows),
      );
    }

    case "buscar": {
      if (!argumento) {
        return await responder(
          contexto,
          [
            negrita("🔎 Buscar alertas"),
            "",
            "Escribe qué quieres buscar después de la palabra `buscar`. Por ejemplo:",
            "`buscar paracetamol`",
          ].join("\n"),
        );
      }

      const rows = await searchAlerts(supabase, argumento);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/buscar",
        queryText: argumento,
        resultCount: rows.length,
        status: "ok",
      });
      return await responder(
        contexto,
        formatAlertListWhatsApp(`🔎 Resultados para "${argumento}"`, rows),
      );
    }

    case "detalle": {
      if (!argumento) {
        return await responder(
          contexto,
          [
            negrita("🔢 Consultar una alerta por número"),
            "",
            "Escribe el número de la alerta. Por ejemplo:",
            "`detalle 75-2026`",
          ].join("\n"),
        );
      }

      const row = await getAlertDetail(supabase, argumento);
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "/detalle",
        queryText: argumento,
        resultCount: row ? 1 : 0,
        status: row ? "ok" : "no_encontrado",
      });

      if (!row) {
        return await responder(
          contexto,
          `📭 No encontré la alerta ${negrita(argumento)}.\n\n` +
            "Verifica el número (formato `75-2026`) o escribe `ultimas` para ver las más recientes.",
        );
      }

      return await responder(contexto, formatAlertDetailWhatsApp(row));
    }

    case "consulta":
      return await ejecutarConsultaIa(contexto, mensaje.waId, nivel, argumento);

    case "miperfil":
      await logConsulta(supabase, { waId: mensaje.waId, command: "/miperfil", status: "ok" });
      return await enviarMiPerfil(contexto, mensaje, nivel);

    case "desconocido":
    default:
      await logConsulta(supabase, {
        waId: mensaje.waId,
        command: "desconocido",
        queryText: parseado.textoOriginal,
        status: "no_reconocido",
      });
      return await responder(contexto, TEXTO_NO_RECONOCIDO);
  }
}

async function procesarMensaje(mensaje: MensajeEntranteWhatsApp): Promise<void> {
  const contexto = crearContextoInbound(mensaje);
  const usuario = await upsertUsuarioWhatsApp(supabase, mensaje);
  const nivel = usuario.nivel;
  const esAdmin = WHATSAPP_ADMIN_WA_IDS.has(mensaje.waId);

  // El admin nunca queda bloqueado por su propia prueba (en la practica su
  // wa_id ni deberia quedar en "gratis", pero esto lo hace explicito).
  if (!esAdmin && !usuarioTieneAcceso(usuario)) {
    await responder(contexto, TEXTO_PRUEBA_VENCIDA);
    await cerrarMensaje(supabase, mensaje.messageId, "ignorado", "prueba_vencida");
    return;
  }

  // Audio, imagen, ubicacion, etc.: el MVP responde solo texto.
  if (mensaje.tipo !== "text") {
    await responder(contexto, TEXTO_SOLO_TEXTO);
    await cerrarMensaje(supabase, mensaje.messageId, "ignorado", `tipo:${mensaje.tipo}`);
    return;
  }

  if (!mensaje.texto.trim()) {
    await responder(contexto, TEXTO_NO_RECONOCIDO);
    await cerrarMensaje(supabase, mensaje.messageId, "ignorado", "texto_vacio");
    return;
  }

  // Comandos "admin ..." se prueban antes que el parser normal, y solo si
  // el wa_id esta autorizado: para cualquier otro usuario, "admin usuarios"
  // sigue el camino normal (cae en "desconocido", nunca en este bloque).
  if (esAdmin) {
    const comandoAdmin = parsearComandoAdmin(mensaje.texto);
    if (comandoAdmin) {
      await ejecutarComandoAdmin(contexto, comandoAdmin);
      await cerrarMensaje(supabase, mensaje.messageId, "procesado", `admin:${comandoAdmin.tipo}`);
      return;
    }
  }

  const parseado = parsearComando(mensaje.texto);

  console.log(
    JSON.stringify({
      evento: "whatsapp_mensaje",
      message_id: truncarParaLog(mensaje.messageId),
      wa_id: truncarParaLog(mensaje.waId, 5),
      tipo: mensaje.tipo,
      comando: parseado.comando,
    }),
  );

  await ejecutarComando(contexto, mensaje, nivel, parseado);
  await cerrarMensaje(supabase, mensaje.messageId, "procesado", parseado.comando);
}

serve(async (req: Request) => {
  const url = new URL(req.url);

  // Verificacion del webhook (Meta la hace una sola vez, al configurarlo).
  if (req.method === "GET") {
    const resultado = verificarWebhookGet(url, WHATSAPP_VERIFY_TOKEN);

    if (!resultado.ok) {
      // El motivo se registra, el token jamas.
      console.warn("WHATSAPP_VERIFY_FALLIDO:", resultado.motivo);
      return new Response("Forbidden", { status: 403 });
    }

    return new Response(resultado.challenge, {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }

  if (req.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const inicio = Date.now();

  // El cuerpo CRUDO es obligatorio para validar la firma: re-serializar el
  // JSON cambia bytes y el HMAC dejaria de coincidir.
  const rawBody = await req.text();

  if (!WHATSAPP_APP_SECRET) {
    console.error("WHATSAPP_APP_SECRET no configurado: no se procesa el webhook.");
    return new Response("Configuración incompleta", { status: 500 });
  }

  const firmaOk = await firmaValida(
    rawBody,
    req.headers.get("x-hub-signature-256"),
    WHATSAPP_APP_SECRET,
  );

  if (!firmaOk) {
    console.warn("WHATSAPP_FIRMA_INVALIDA: se descarta el evento.");
    return new Response("Firma inválida", { status: 401 });
  }

  let payload: unknown;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    // Evento malformado: se responde 200 para que Meta no lo reintente en
    // bucle (reintentarlo no lo va a arreglar).
    console.warn("WHATSAPP_PAYLOAD_MALFORMADO");
    return new Response("OK", { status: 200 });
  }

  const mensaje = extraerMensajeEntrante(payload);

  // statuses (sent/delivered/read), cambios administrativos o eventos
  // desconocidos: no son consultas, se confirman y se ignoran.
  if (!mensaje) {
    return new Response("OK", { status: 200 });
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error("Faltan credenciales de Supabase: no se procesa el mensaje.");
    return new Response("Configuración incompleta", { status: 500 });
  }

  // Se procesa ANTES de responder 200. Una consulta IA puede tardar mas que
  // el timeout de Meta, y entonces Meta reintenta el webhook: ese reintento
  // llega con el mismo message_id y la reserva de abajo lo corta en seco, en
  // vez de contestarle al usuario dos veces y cobrarle dos consultas.
  try {
    const esNuevo = await reservarMensaje(supabase, mensaje);

    if (!esNuevo) {
      console.log(
        JSON.stringify({
          evento: "whatsapp_duplicado",
          message_id: truncarParaLog(mensaje.messageId),
        }),
      );
      return new Response("OK", { status: 200 });
    }

    await procesarMensaje(mensaje);

    console.log(
      JSON.stringify({
        evento: "whatsapp_ok",
        message_id: truncarParaLog(mensaje.messageId),
        duracion_ms: Date.now() - inicio,
      }),
    );
  } catch (error) {
    console.error("WHATSAPP_ERROR:", error instanceof Error ? error.message : String(error));

    // El message_id queda registrado (como "error"), asi que un reintento de
    // Meta tampoco lo reprocesa: se prefiere no repetirle la respuesta al
    // usuario antes que reintentar en automatico, y el error queda en la
    // tabla para revisarlo.
    await cerrarMensaje(
      supabase,
      mensaje.messageId,
      "error",
      undefined,
      error instanceof Error ? error.message : String(error),
    ).catch(() => {});

    // Se intenta avisar al usuario, pero sin romper la respuesta al webhook.
    try {
      await responder(
        crearContextoInbound(mensaje),
        "⚠️ Tuve un problema procesando tu consulta. Intenta de nuevo en unos minutos.",
      );
    } catch (_errorEnvio) {
      // Si tampoco se puede responder, queda registrado arriba.
    }
  }

  // Siempre 200 tras haber aceptado el evento: un 500 haria que Meta
  // reintente un mensaje que ya quedo marcado como procesado.
  return new Response("OK", { status: 200 });
});
