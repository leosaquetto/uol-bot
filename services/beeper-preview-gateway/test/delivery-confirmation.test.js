import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import {
  createDeliveryConfirmation,
  inspectDeliveryRow,
} from "../src/delivery-confirmation.js";

const chatId = "!group:local-whatsapp.localhost";
const pendingMessageID = "~txn:network:TEST";

function databaseFixture() {
  const directory = mkdtempSync(join(tmpdir(), "beeper-confirmation-test-"));
  const databasePath = join(directory, "index.db");
  const database = new DatabaseSync(databasePath);
  database.exec(`
    CREATE TABLE mx_room_messages (
      roomID TEXT NOT NULL,
      eventID TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      echo_echoID TEXT,
      sendStatus TEXT,
      message TEXT
    );
  `);
  return { database, databasePath };
}

test("distingue eco pendente, rejeição e evento final com thumbnail", () => {
  assert.deepEqual(inspectDeliveryRow(null, pendingMessageID, true), { state: "pending" });
  assert.deepEqual(inspectDeliveryRow({
    eventID: pendingMessageID,
    sendStatus: JSON.stringify({ status: "FAIL_PERMANENT" }),
    message: JSON.stringify({ links: [{ img: "file:///preview.jpg" }] }),
  }, pendingMessageID, true), { state: "rejected" });
  assert.deepEqual(inspectDeliveryRow({
    eventID: pendingMessageID,
    sendStatus: JSON.stringify({ status: "FAIL_RETRIABLE" }),
    message: JSON.stringify({ links: [{ img: "file:///preview.jpg" }] }),
  }, pendingMessageID, true), { state: "pending" });
  assert.deepEqual(inspectDeliveryRow({
    eventID: "$final-event",
    sendStatus: JSON.stringify({ status: "SUCCESS" }),
    message: JSON.stringify({ links: [{ img: "mxc://preview" }] }),
  }, pendingMessageID, true), { state: "delivered", previewPresent: true });
  assert.deepEqual(inspectDeliveryRow({
    eventID: "$final-without-preview",
    sendStatus: JSON.stringify({ status: "SUCCESS" }),
    message: JSON.stringify({ links: [] }),
  }, pendingMessageID, true), { state: "unknown", code: "preview_missing" });
});

test("confirma no índice local somente o evento final do mesmo pendingMessageID", async () => {
  const { database, databasePath } = databaseFixture();
  database.prepare(`
    INSERT INTO mx_room_messages
      (roomID, eventID, timestamp, echo_echoID, sendStatus, message)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    chatId,
    "$final-event",
    Date.now(),
    pendingMessageID,
    JSON.stringify({ status: "SUCCESS" }),
    JSON.stringify({ links: [{ img: "mxc://preview" }] }),
  );
  database.close();
  const confirmation = createDeliveryConfirmation({ databasePath, chatId });
  assert.equal(confirmation.isReady(), true);
  assert.deepEqual(
    await confirmation.waitForDelivery({ pendingMessageID, requirePreview: true }),
    { state: "delivered", previewPresent: true },
  );
  confirmation.close();
});

test("timeout de confirmação permanece ambíguo", async () => {
  const { database, databasePath } = databaseFixture();
  database.close();
  let clock = 0;
  const confirmation = createDeliveryConfirmation({
    databasePath,
    chatId,
    timeoutMs: 10,
    pollIntervalMs: 5,
    now: () => clock,
    sleep: async (duration) => {
      clock += duration;
    },
  });
  assert.deepEqual(
    await confirmation.waitForDelivery({ pendingMessageID }),
    { state: "unknown", code: "confirmation_timeout" },
  );
  confirmation.close();
});
