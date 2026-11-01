import { withTelegramWebhookAuthentication } from "./webhook_auth.ts";

export const TELEGRAM_HEALTH_RESPONSE = "RegAlert DIGEMID Telegram Bot OK";

export interface TelegramWebhookHandlerOptions {
  webhookSecret: string;
  handleAuthenticatedPost: (request: Request) => Promise<Response>;
}

/**
 * Handler HTTP de producción. Solo GET es público; todo POST se autentica antes
 * de delegar y los demás métodos no ejecutan lógica del bot.
 */
export function createTelegramWebhookHandler(
  options: TelegramWebhookHandlerOptions,
): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    if (request.method === "GET") {
      return new Response(TELEGRAM_HEALTH_RESPONSE, { status: 200 });
    }

    if (request.method !== "POST") {
      return new Response("Method not allowed", {
        status: 405,
        headers: { Allow: "GET, POST" },
      });
    }

    return await withTelegramWebhookAuthentication(
      request,
      options.webhookSecret,
      options.handleAuthenticatedPost,
    );
  };
}
