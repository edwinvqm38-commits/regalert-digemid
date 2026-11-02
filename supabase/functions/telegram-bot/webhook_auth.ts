export const TELEGRAM_WEBHOOK_SECRET_HEADER = "X-Telegram-Bot-Api-Secret-Token";

const TELEGRAM_SECRET_PATTERN = /^[A-Za-z0-9_-]{1,256}$/;

function isValidConfiguredSecret(secret: string): boolean {
  return TELEGRAM_SECRET_PATTERN.test(secret);
}

async function sha256(value: string): Promise<Uint8Array> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return new Uint8Array(digest);
}

/** Compara valores de longitud fija sin terminar al encontrar el primer byte distinto. */
export async function secureSecretEquals(
  expected: string,
  received: string,
): Promise<boolean> {
  const [expectedHash, receivedHash] = await Promise.all([
    sha256(expected),
    sha256(received),
  ]);

  let difference = 0;
  for (let index = 0; index < expectedHash.length; index++) {
    difference |= expectedHash[index] ^ receivedHash[index];
  }
  return difference === 0;
}

/**
 * Devuelve una respuesta de rechazo o null cuando el POST está autenticado.
 * Los mensajes son deliberadamente genéricos y nunca incluyen secretos.
 */
export async function telegramWebhookAuthFailure(
  request: Request,
  configuredSecret: string,
): Promise<Response | null> {
  if (!isValidConfiguredSecret(configuredSecret)) {
    return new Response("Service unavailable", { status: 503 });
  }

  const receivedSecret = request.headers.get(TELEGRAM_WEBHOOK_SECRET_HEADER);
  if (
    receivedSecret === null ||
    !(await secureSecretEquals(configuredSecret, receivedSecret))
  ) {
    return new Response("Unauthorized", { status: 401 });
  }

  return null;
}

/** Ejecuta el handler protegido únicamente después de autenticar el webhook. */
export async function withTelegramWebhookAuthentication(
  request: Request,
  configuredSecret: string,
  protectedHandler: (request: Request) => Promise<Response>,
): Promise<Response> {
  const failure = await telegramWebhookAuthFailure(request, configuredSecret);
  if (failure) return failure;
  return await protectedHandler(request);
}
