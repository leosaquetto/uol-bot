import assert from "node:assert/strict";
import test from "node:test";

import {
  contractHealthSignal,
  validateTicketApiPayload,
} from "../src/uol-contract.js";

test("aceita payload de benefícios com oferta pública válida", () => {
  const result = validateTicketApiPayload({
    beneficios: [{
      url: "https://clube.uol.com.br/campanhasdeingresso/pA1-ingresso",
      titulo: "2 INGRESSOS: Teatro",
    }],
  });
  assert.deepEqual(result, {
    ok: true,
    reason: "ok",
    total: 1,
    valid: 1,
    invalid: 0,
    fields: ["titulo", "url"],
  });
});

test("detecta ausência ou tipo inválido de benefícios", () => {
  assert.equal(validateTicketApiPayload({}).reason, "beneficios_missing");
  assert.equal(validateTicketApiPayload({ beneficios: null }).reason, "beneficios_missing");
  assert.equal(validateTicketApiPayload(null).reason, "payload_invalid");
});

test("bloqueia resposta não vazia sem nenhuma oferta parseável", () => {
  const result = validateTicketApiPayload({
    beneficios: [
      { url: "https://example.com/fora-do-clube", titulo: "Oferta" },
      { url: "", titulo: "" },
    ],
  });
  assert.equal(result.ok, false);
  assert.equal(result.reason, "no_parseable_offers");
  assert.equal(result.total, 2);
  assert.equal(result.valid, 0);
  assert.equal(result.invalid, 2);
  assert.deepEqual(result.fields, ["titulo", "url"]);
});

test("permite resposta mista e registra somente metadados dos campos", () => {
  const result = validateTicketApiPayload({
    beneficios: [
      { link: "/cacaushow/pB2-oferta", titulo: "Oferta válida" },
      { descricao: "sem URL pública" },
    ],
  });
  assert.equal(result.ok, true);
  assert.equal(result.reason, "partial_parseable_offers");
  assert.equal(result.degraded, true);
  assert.equal(result.total, 2);
  assert.equal(result.valid, 1);
  assert.equal(result.invalid, 1);
  assert.deepEqual(result.fields, ["descricao", "link", "titulo"]);
});

test("considera lista vazia um contrato válido", () => {
  assert.deepEqual(validateTicketApiPayload({ beneficios: [] }), {
    ok: true,
    reason: "empty",
    total: 0,
    valid: 0,
    invalid: 0,
    fields: [],
  });
});

test("converte falha de contrato em sinal crítico sanitizado", () => {
  assert.deepEqual(contractHealthSignal({
    ok: false,
    reason: "no_parseable_offers",
    total: 4,
    valid: 0,
    invalid: 4,
    fields: ["titulo", "url"],
  }), {
    key: "ticket-api-contract",
    severity: "critical",
    summary: "A API de ofertas mudou de contrato ou retornou dados inválidos",
    details: "motivo: no_parseable_offers; total: 4; válidas: 0; inválidas: 4",
  });
  assert.equal(contractHealthSignal({ ok: true }), null);
});

test("converte contrato parcialmente parseável em sinal degradado", () => {
  assert.deepEqual(contractHealthSignal({
    ok: true,
    degraded: true,
    reason: "partial_parseable_offers",
    total: 2,
    valid: 1,
    invalid: 1,
  }), {
    key: "ticket-api-contract",
    severity: "warning",
    summary: "A API de ofertas mudou de contrato ou retornou dados inválidos",
    details: "motivo: partial_parseable_offers; total: 2; válidas: 1; inválidas: 1",
  });
});
