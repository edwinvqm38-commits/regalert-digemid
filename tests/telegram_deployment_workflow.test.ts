import { strict as assert } from "node:assert";

const deployWorkflow = await Deno.readTextFile(
  new URL(
    "../.github/workflows/deploy-supabase-functions.yml",
    import.meta.url,
  ),
);
const prepareWorkflow = await Deno.readTextFile(
  new URL("../.github/workflows/set-telegram-webhook.yml", import.meta.url),
);

function position(source: string, marker: string): number {
  const index = source.indexOf(marker);
  assert.notEqual(index, -1, `No se encontró: ${marker}`);
  return index;
}

Deno.test("orden protegido: secreto, webhook, verificación y despliegue", () => {
  const sync = position(prepareWorkflow, "id: sync-secret");
  const configure = position(prepareWorkflow, "id: configure-webhook");
  const verify = position(prepareWorkflow, "id: verify-webhook");

  assert.ok(sync < configure);
  assert.ok(configure < verify);
  assert.match(
    deployWorkflow,
    /deploy-telegram:[\s\S]*?needs: prepare-telegram[\s\S]*?functions deploy telegram-bot/,
  );
});

Deno.test("cualquier fallo de preparación impide desplegar telegram-bot", () => {
  assert.match(prepareWorkflow, /id: validate[\s\S]*?set -euo pipefail/);
  assert.match(prepareWorkflow, /id: sync-secret[\s\S]*?set -euo pipefail/);
  assert.match(
    prepareWorkflow,
    /id: configure-webhook[\s\S]*?set -euo pipefail/,
  );
  assert.match(prepareWorkflow, /id: verify-webhook[\s\S]*?set -euo pipefail/);
  assert.match(
    deployWorkflow,
    /deploy-telegram:[\s\S]*?needs: prepare-telegram/,
  );
  assert.equal(
    [...deployWorkflow.matchAll(/functions deploy telegram-bot/g)].length,
    1,
  );
});

Deno.test("preparación exige y sincroniza una única fuente del secreto", () => {
  assert.match(
    prepareWorkflow,
    /TELEGRAM_WEBHOOK_SECRET: \$\{\{ secrets\.TELEGRAM_WEBHOOK_SECRET \}\}/,
  );
  assert.match(
    prepareWorkflow,
    /supabase secrets set[\s\S]*?TELEGRAM_WEBHOOK_SECRET=\$\{TELEGRAM_WEBHOOK_SECRET\}/,
  );
  assert.match(prepareWorkflow, /secret_token: \$secret_token/);
});

Deno.test("webhook conserva parámetros y nunca descarta updates", () => {
  assert.match(prepareWorkflow, /\.result\.allowed_updates \/\/ null/);
  assert.match(prepareWorkflow, /\.result\.max_connections \/\/ null/);
  assert.match(prepareWorkflow, /allowed_updates: \$allowed_updates/);
  assert.match(prepareWorkflow, /max_connections: \$max_connections/);
  assert.doesNotMatch(prepareWorkflow, /drop_pending_updates["']?\s*:\s*true/);
  assert.match(prepareWorkflow, /has\("drop_pending_updates"\)/);
});

Deno.test("verificación comprueba respuesta, URL y errores nuevos", () => {
  assert.match(prepareWorkflow, /\.ok == true and \.result == true/);
  assert.match(prepareWorkflow, /ACTUAL_URL[\s\S]*?EXPECTED_URL/);
  assert.match(prepareWorkflow, /AFTER_ERROR_DATE[\s\S]*?BEFORE_ERROR_DATE/);
  assert.match(prepareWorkflow, /OPERATION_STARTED_AT/);
});

Deno.test("WhatsApp se despliega sin depender de la preparación de Telegram", () => {
  const whatsappJob = deployWorkflow.slice(
    position(deployWorkflow, "deploy-whatsapp:"),
    position(deployWorkflow, "prepare-telegram:"),
  );
  assert.doesNotMatch(whatsappJob, /needs:/);
  assert.match(whatsappJob, /functions deploy whatsapp-bot/);
});
