import { strict as assert } from "node:assert";
import { TELEGRAM_WEBHOOK_SECRET_HEADER } from "./webhook_auth.ts";
import {
  createTelegramWebhookHandler,
  TELEGRAM_HEALTH_RESPONSE,
} from "./webhook_handler.ts";

const SECRET = "telegram_integration_secret-2026";

class SpyRequest extends Request {
  jsonCalls = 0;

  override async json(): Promise<unknown> {
    this.jsonCalls++;
    return await super.json();
  }
}

interface ProtectedCounters {
  callbacks: number;
  commands: number;
  setMyCommands: number;
  supabase: number;
}

function post(secret?: string): SpyRequest {
  const headers = new Headers({ "content-type": "application/json" });
  if (secret !== undefined) headers.set(TELEGRAM_WEBHOOK_SECRET_HEADER, secret);
  return new SpyRequest("https://example.test/functions/v1/telegram-bot", {
    method: "POST",
    headers,
    body: '{"update_id":1}',
  });
}

function instrumentedHandler(
  webhookSecret: string,
  counters: ProtectedCounters,
) {
  return createTelegramWebhookHandler({
    webhookSecret,
    handleAuthenticatedPost: async (request) => {
      counters.setMyCommands++;
      counters.supabase++;
      counters.commands++;
      counters.callbacks++;
      await request.json();
      return new Response("OK", { status: 200 });
    },
  });
}

function emptyCounters(): ProtectedCounters {
  return { callbacks: 0, commands: 0, setMyCommands: 0, supabase: 0 };
}

function assertNoProtectedOperations(counters: ProtectedCounters): void {
  assert.deepEqual(counters, emptyCounters());
}

Deno.test("handler real acepta POST autenticado y delega una sola vez", async () => {
  const counters = emptyCounters();
  const request = post(SECRET);
  const response = await instrumentedHandler(SECRET, counters)(request);

  assert.equal(response.status, 200);
  assert.equal(request.jsonCalls, 1);
  assert.deepEqual(counters, {
    callbacks: 1,
    commands: 1,
    setMyCommands: 1,
    supabase: 1,
  });
});

for (
  const testCase of [
    {
      name: "sin encabezado",
      configured: SECRET,
      received: undefined,
      status: 401,
    },
    {
      name: "con secreto incorrecto",
      configured: SECRET,
      received: "incorrecto",
      status: 401,
    },
    {
      name: "sin secreto configurado",
      configured: "",
      received: SECRET,
      status: 503,
    },
  ]
) {
  Deno.test(`handler real rechaza POST ${testCase.name} antes del body y operaciones`, async () => {
    const counters = emptyCounters();
    const request = post(testCase.received);
    const response = await instrumentedHandler(testCase.configured, counters)(
      request,
    );

    assert.equal(response.status, testCase.status);
    assert.equal(request.jsonCalls, 0);
    assert.equal(request.bodyUsed, false);
    assertNoProtectedOperations(counters);
  });
}

Deno.test("handler real conserva GET de salud sin operaciones protegidas", async () => {
  const counters = emptyCounters();
  const handler = instrumentedHandler(SECRET, counters);
  const response = await handler(
    new Request("https://example.test/functions/v1/telegram-bot"),
  );

  assert.equal(response.status, 200);
  assert.equal(await response.text(), TELEGRAM_HEALTH_RESPONSE);
  assertNoProtectedOperations(counters);
});

for (const method of ["PUT", "PATCH", "DELETE", "OPTIONS"]) {
  Deno.test(`handler real rechaza ${method} sin operaciones protegidas`, async () => {
    const counters = emptyCounters();
    const handler = instrumentedHandler(SECRET, counters);
    const response = await handler(
      new Request("https://example.test/functions/v1/telegram-bot", { method }),
    );

    assert.equal(response.status, 405);
    assert.equal(response.headers.get("allow"), "GET, POST");
    assert.equal(response.headers.get("access-control-allow-origin"), null);
    assertNoProtectedOperations(counters);
  });
}
