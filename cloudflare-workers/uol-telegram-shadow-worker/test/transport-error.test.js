import test from "node:test";
import assert from "node:assert/strict";

import {
  createHttpTransportError,
  isTelegramMessageCaptionMissingError,
  isTelegramMessageMissingError,
  isTelegramMessageNotModifiedError,
} from "../src/transport-error.js";

test("classifica somente edição Telegram 400 com mensagem inexistente", () => {
  const missing = createHttpTransportError({
    transport: "telegram",
    operation: "editMessageText",
    status: 400,
    httpStatus: 400,
    description: "Bad Request: message to edit not found",
  });
  assert.equal(isTelegramMessageMissingError(missing), true);

  const rateLimited = createHttpTransportError({
    transport: "telegram",
    operation: "editMessageText",
    status: 429,
    httpStatus: 429,
    description: "Too Many Requests",
  });
  assert.equal(isTelegramMessageMissingError(rateLimited), false);

  const sendFailure = createHttpTransportError({
    transport: "telegram",
    operation: "sendMessage",
    status: 400,
    httpStatus: 400,
    description: "Bad Request: message to edit not found",
  });
  assert.equal(isTelegramMessageMissingError(sendFailure), false);
});

test("classifica caption ausente e edição sem alteração apenas no Telegram", () => {
  const captionMissing = createHttpTransportError({
    transport: "telegram",
    operation: "editMessageCaption",
    status: 400,
    httpStatus: 400,
    description: "Bad Request: there is no caption in the message to edit",
  });
  assert.equal(isTelegramMessageCaptionMissingError(captionMissing), true);

  const notModified = createHttpTransportError({
    transport: "telegram",
    operation: "editMessageText",
    status: 400,
    httpStatus: 400,
    description: "Bad Request: message is not modified",
  });
  assert.equal(isTelegramMessageNotModifiedError(notModified), true);

  assert.equal(isTelegramMessageCaptionMissingError(notModified), false);
  assert.equal(isTelegramMessageNotModifiedError(captionMissing), false);
});
