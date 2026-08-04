import test from "node:test";
import assert from "node:assert/strict";

import {
  classifyTicketProbeResponse,
  nextTicketProbeState,
  ticketProbeBudget,
} from "../src/ticket-soldout-probe.js";

const requestedUrl = "https://clube.uol.com.br/campanhasdeingresso/ticket";

test("classifica 404 e redirecionamento para home como oferta ausente", () => {
  assert.deepEqual(
    classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: requestedUrl,
      status: 404,
      body: "",
    }),
    { result: "gone", reason: "http_404" },
  );
  assert.deepEqual(
    classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: "https://clube.uol.com.br/",
      status: 200,
      body: "Clube UOL",
    }),
    { result: "gone", reason: "home_redirect" },
  );
});

test("preserva página válida e falha fechado para respostas ambíguas", () => {
  assert.deepEqual(
    classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: requestedUrl,
      status: 200,
      body: "Detalhes da oferta",
    }),
    { result: "available", reason: "offer_page" },
  );
  assert.deepEqual(
    classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: "https://clube.uol.com.br/",
      status: 503,
      body: "temporarily unavailable",
    }),
    { result: "indeterminate", reason: "http_503" },
  );
  assert.deepEqual(
    classifyTicketProbeResponse({
      requestedUrl,
      finalUrl: requestedUrl,
      status: 200,
      body: "",
    }),
    { result: "indeterminate", reason: "empty_body" },
  );
});

test("exige duas ausências consecutivas e libera fallback em resultado incerto", () => {
  const now = new Date("2026-08-04T18:40:00.000Z");
  assert.deepEqual(
    nextTicketProbeState({
      result: "gone",
      goneCount: 0,
      attempts: 0,
      now,
      confirmGoneCount: 2,
      maxAttempts: 2,
    }),
    {
      action: "continue",
      goneCount: 1,
      nextAt: "2026-08-04T18:40:05.000Z",
      lastResult: "gone",
    },
  );
  assert.deepEqual(
    nextTicketProbeState({
      result: "gone",
      goneCount: 1,
      attempts: 1,
      now,
      confirmGoneCount: 2,
      maxAttempts: 2,
    }),
    {
      action: "confirm",
      goneCount: 2,
      nextAt: "",
      lastResult: "gone",
    },
  );
  assert.deepEqual(
    nextTicketProbeState({
      result: "indeterminate",
      goneCount: 1,
      attempts: 0,
      now,
      confirmGoneCount: 2,
      maxAttempts: 2,
    }),
    {
      action: "fallback",
      goneCount: 0,
      nextAt: "",
      lastResult: "indeterminate",
    },
  );
});

test("limita a fila crítica diária sem ultrapassar o orçamento", () => {
  assert.deepEqual(
    ticketProbeBudget({ used: 0, dailyLimit: 256, perScanLimit: 1 }),
    { remaining: 256, allowed: true, batchSize: 1 },
  );
  assert.deepEqual(
    ticketProbeBudget({ used: 255, dailyLimit: 256, perScanLimit: 1 }),
    { remaining: 1, allowed: true, batchSize: 1 },
  );
  assert.deepEqual(
    ticketProbeBudget({ used: 256, dailyLimit: 256, perScanLimit: 1 }),
    { remaining: 0, allowed: false, batchSize: 0 },
  );
});
