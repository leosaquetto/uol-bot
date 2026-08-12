import { evaluateHealthContract } from "./health-contract.js";

export function classifyHeadlessHealth(options = {}) {
  return evaluateHealthContract(options);
}
