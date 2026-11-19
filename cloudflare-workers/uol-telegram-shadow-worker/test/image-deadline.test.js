import assert from "node:assert/strict";
import test from "node:test";

import {
  deferredMainDeliveryState,
  imageDeadline,
  lateImageUpgradeDue,
  mainImageDeliveryOffer,
  photoMutationSafetySeconds,
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

test("janela da foto: com 45 s a tentativa em +30 s ainda pode enviar foto", () => {
  // timeout do Telegram 8 s, prazo de texto 60 s
  const safety = photoMutationSafetySeconds(60, 45, 8);
  assert.equal(safety, 7);
  const offer = { id: "o", firstSeenAt: "2026-11-19T12:00:00.000Z" };
  const built = mainImageDeliveryOffer(offer, new Date("2026-11-19T12:00:30.000Z"), 60, safety);
  // a foto precisa de >= 8 s até imageMutationDeadlineAt: 12:00:53 - 12:00:30 = 23 s
  assert.equal(built.imageMutationDeadlineAt, "2026-11-19T12:00:53.000Z");
  assert.ok(Date.parse(built.imageMutationDeadlineAt) - Date.parse("2026-11-19T12:00:30.000Z") >= 8_000);
  // a última tentativa que ainda cabe começa em +45 s e termina (8 s) antes do texto
  assert.ok(Date.parse(built.imageMutationDeadlineAt) - 8_000 === Date.parse("2026-11-19T12:00:45.000Z"));
});

test("janela da foto nunca passa o prazo menos o timeout e o padrão antigo era +14 s", () => {
  assert.equal(photoMutationSafetySeconds(60, 300, 8), 0);
  assert.equal(photoMutationSafetySeconds(60, 52, 8), 0);
  assert.equal(photoMutationSafetySeconds(60, 14, 8), 38);
  assert.equal(photoMutationSafetySeconds(60, 0, 8), 51);
});
