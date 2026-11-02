import { strict as assert } from "node:assert";
import {
  secureSecretEquals,
  TELEGRAM_WEBHOOK_SECRET_HEADER,
  telegramWebhookAuthFailure,
  withTelegramWebhookAuthentication,
} from "./webhook_auth.ts";

const CONFIGURED_SECRET = "telegram_webhook_secret_test-2026";

function requestWithSecret(secret?: string, body = '{"update_id":1}'): Request {
  const headers = new Headers({ "content-type": "application/json" });
  if (secret !== undefined) headers.set(TELEGRAM_WEBHOOK_SECRET_HEADER, secret);
  return new Request("https://example.test/functions/v1/telegram-bot", {
    method: "POST",
    headers,
    body,
  });
}

Deno.test("acepta un secreto configurado y un encabezado válido", async () => {
  const failure = await telegramWebhookAuthFailure(
    requestWithSecret(CONFIGURED_SECRET),
    CONFIGURED_SECRET,
  );
  assert.equal(failure, null);
  assert.equal(
    await secureSecretEquals(CONFIGURED_SECRET, CONFIGURED_SECRET),
    true,
  );
});

Deno.test("rechaza el encabezado ausente sin revelar detalles", async () => {
  const response = await telegramWebhookAuthFailure(
    requestWithSecret(),
    CONFIGURED_SECRET,
  );
  assert.equal(response?.status, 401);
  assert.equal(await response?.text(), "Unauthorized");
});

Deno.test("rechaza un encabezado incorrecto", async () => {
  const response = await telegramWebhookAuthFailure(
    requestWithSecret("secret_incorrecto"),
    CONFIGURED_SECRET,
  );
  assert.equal(response?.status, 401);
  assert.equal(
    await secureSecretEquals(CONFIGURED_SECRET, "secret_incorrecto"),
    false,
  );
});

Deno.test("falla de forma segura si el secreto no está configurado", async () => {
  const response = await telegramWebhookAuthFailure(
    requestWithSecret(CONFIGURED_SECRET),
    "",
  );
  assert.equal(response?.status, 503);
  assert.equal(await response?.text(), "Service unavailable");
});

Deno.test("falla de forma segura si el secreto configurado no cumple Telegram Bot API", async () => {
  const response = await telegramWebhookAuthFailure(
    requestWithSecret("secret with spaces"),
    "secret with spaces",
  );
  assert.equal(response?.status, 503);
});

Deno.test("un update malicioso sin autenticación no ejecuta operaciones protegidas", async () => {
  let protectedOperations = 0;
  const maliciousBody = '{"callback_query":{"data":"pago:confirmar:forjado"}}';

  const response = await withTelegramWebhookAuthentication(
    requestWithSecret(undefined, maliciousBody),
    CONFIGURED_SECRET,
    async (request) => {
      protectedOperations++;
      await request.json();
      return new Response("OK");
    },
  );

  assert.equal(response.status, 401);
  assert.equal(protectedOperations, 0);
});

Deno.test("solo el update autenticado alcanza el handler protegido", async () => {
  let protectedOperations = 0;
  const response = await withTelegramWebhookAuthentication(
    requestWithSecret(CONFIGURED_SECRET),
    CONFIGURED_SECRET,
    async (request) => {
      protectedOperations++;
      const update = await request.json();
      assert.equal(update.update_id, 1);
      return new Response("OK", { status: 200 });
    },
  );

  assert.equal(response.status, 200);
  assert.equal(protectedOperations, 1);
});
