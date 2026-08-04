import test from "node:test";
import assert from "node:assert/strict";

import { classifyKnownMaintenanceRepair } from "../src/maintenance-repair.js";

test("repara somente sold-out principal que Telegram confirmou como não modificado", () => {
  assert.deepEqual(
    classifyKnownMaintenanceRepair({
      status: "sold_out",
      main_sold_out_synced_at: "",
      main_sold_out_attempts: 10,
      main_sold_out_error: "telegram_editMessageText_400:Bad Request: message is not modified",
      canal2_sold_out_synced_at: "",
      canal2_sold_out_attempts: 0,
      canal2_sold_out_error: "",
    }, 10),
    { main: "mark_synced", canal2: "none" },
  );
});

test("recoloca cópia do canal 2 na fila quando a edição de caption falhou", () => {
  assert.deepEqual(
    classifyKnownMaintenanceRepair({
      status: "sold_out",
      main_sold_out_synced_at: "2026-08-04T00:00:00Z",
      main_sold_out_attempts: 1,
      main_sold_out_error: "",
      canal2_sold_out_synced_at: "",
      canal2_sold_out_attempts: 10,
      canal2_sold_out_error: "telegram_editMessageCaption_400:Bad Request: there is no caption in the message to edit",
    }, 10),
    { main: "none", canal2: "retry" },
  );
});

test("não altera falhas diferentes nem tentativas ainda em andamento", () => {
  assert.deepEqual(
    classifyKnownMaintenanceRepair({
      status: "sold_out",
      main_sold_out_synced_at: "",
      main_sold_out_attempts: 9,
      main_sold_out_error: "telegram_editMessageText_500:server",
      canal2_sold_out_synced_at: "",
      canal2_sold_out_attempts: 10,
      canal2_sold_out_error: "telegram_editMessageText_400:message to edit not found",
    }, 10),
    { main: "none", canal2: "none" },
  );
});
