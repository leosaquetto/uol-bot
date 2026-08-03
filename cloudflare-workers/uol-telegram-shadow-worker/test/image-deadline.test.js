import assert from "node:assert/strict";
import test from "node:test";

import {
  deferredMainDeliveryState,
  imageDeadline,
  lateImageUpgradeDue,
  mainImageDeliveryOffer,
} from "../src/image-deadline.js";

test("mantém prazo absoluto desde a primeira detecção", () => {
  assert.deepEqual(
    imageDeadline(
      "2026-08-03T12:00:00.000Z",
      new Date("2026-08-03T12:00:45.000Z"),
      60,
    ),
    {
      deadlineAt: "2026-08-03T12:01:00.000Z",
      expired: false,
      remainingMs: 15_000,
    },
  );
});

test("expira exatamente aos 60 segundos sem renovar prazo", () => {
  assert.deepEqual(
    imageDeadline(
      "2026-08-03T12:00:00.000Z",
      new Date("2026-08-03T12:01:00.000Z"),
      60,
    ),
    {
      deadlineAt: "2026-08-03T12:01:00.000Z",
      expired: true,
      remainingMs: 0,
    },
  );
});

test("timestamp inválido falha fechado como prazo expirado", () => {
  assert.deepEqual(
    imageDeadline("inválido", new Date("2026-08-03T12:00:00.000Z"), 60),
    { deadlineAt: "", expired: true, remainingMs: 0 },
  );
});

test("monta tentativa com foto antes do prazo e libera texto ao expirar", () => {
  const offer = {
    id: "offer-1",
    firstSeenAt: "2026-08-03T12:00:00.000Z",
  };
  assert.deepEqual(
    mainImageDeliveryOffer(offer, new Date("2026-08-03T12:00:45.000Z"), 60, 38),
    {
      ...offer,
      imageDeadlineAt: "2026-08-03T12:01:00.000Z",
      imageMutationDeadlineAt: "2026-08-03T12:00:22.000Z",
      deferTextFallback: true,
    },
  );
  assert.deepEqual(
    mainImageDeliveryOffer(offer, new Date("2026-08-03T12:01:00.000Z"), 60, 38),
    {
      ...offer,
      imageDeadlineAt: "2026-08-03T12:01:00.000Z",
      imageMutationDeadlineAt: "2026-08-03T12:00:22.000Z",
      deferTextFallback: false,
    },
  );
});

test("só atualiza texto vencido quando foto existe e retry está liberado", () => {
  const due = {
    telegram_image_strategy: "text_timeout",
    main_message_id: 48,
    main_message_kind: "text",
    image_url: "https://example.com/show.jpg",
    main_image_upgrade_attempts: 2,
    main_image_upgrade_next_attempt_at: "2026-08-03T12:00:10.000Z",
  };
  const now = new Date("2026-08-03T12:00:15.000Z");
  assert.equal(lateImageUpgradeDue(due, now, 10), true);
  assert.equal(lateImageUpgradeDue({ ...due, image_url: "" }, now, 10), false);
  assert.equal(lateImageUpgradeDue({ ...due, main_image_upgrade_attempts: 10 }, now, 10), false);
  assert.equal(lateImageUpgradeDue({
    ...due,
    main_image_upgrade_next_attempt_at: "2026-08-03T12:00:20.000Z",
  }, now, 10), false);
  assert.equal(lateImageUpgradeDue({ ...due, telegram_image_strategy: "remote_url" }, now, 10), false);
});

test("adiamento de foto preserva tentativas de entrega", () => {
  assert.deepEqual(
    deferredMainDeliveryState(
      3,
      "2026-08-03T12:01:00.000Z",
      new Date("2026-08-03T12:00:45.000Z"),
    ),
    {
      attempts: 3,
      nextAttemptAt: "2026-08-03T12:00:46.000Z",
    },
  );
});
