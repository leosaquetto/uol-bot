'use strict';

const { createHash } = require('node:crypto');

const ACTION = {
  download: 'is.workflow.actions.downloadurl',
  save: 'is.workflow.actions.documentpicker.save',
  dictionary: 'is.workflow.actions.dictionary',
  run: 'dk.simonbs.Scriptable.ParameterizedRunScriptIntent',
  conditional: 'is.workflow.actions.conditional',
};
const PREMIUM_NAME = 'Clube UOL Premium';
const LEGACY_PREMIUM_NAME = 'Clube UOL - Stories Hunter';
const EXPECTED_DOWNLOAD_URL = 'http://MacBook-Air-de-Leo.local:53318/scriptable';
const DIAGNOSTIC = 'Update Scriptable: título ausente ou não reconhecido. Passe o título da notificação do Scriptable como entrada do atalho.';
const NAMESPACE = Buffer.from('cbcd65dc1cf74d73971eead7b288784d', 'hex');

function uuid(name) {
  const bytes = createHash('sha1').update(NAMESPACE).update(name).digest().subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const value = bytes.toString('hex').toUpperCase();
  return `${value.slice(0, 8)}-${value.slice(8, 12)}-${value.slice(12, 16)}-${value.slice(16, 20)}-${value.slice(20)}`;
}

function assertSource(condition, message) {
  if (!condition) throw new TypeError(`Cannot unify shortcuts: ${message}`);
}

function isAction(action, identifier) {
  return action && typeof action === 'object' && !Array.isArray(action)
    && typeof action.WFWorkflowActionIdentifier === 'string'
    && (!identifier || action.WFWorkflowActionIdentifier === identifier)
    && action.WFWorkflowActionParameters && typeof action.WFWorkflowActionParameters === 'object'
    && !Array.isArray(action.WFWorkflowActionParameters);
}

function inputValue(input) {
  return (input?.Type === 'Variable' ? input.Variable : input)?.Value;
}

function validateSources(genericActions, premiumShortcut) {
  assertSource(Array.isArray(genericActions) && genericActions.length >= 2, 'expected generic action array');
  assertSource(genericActions.every(action => isAction(action)), 'invalid generic action');
  const [download, save] = genericActions;
  assertSource(isAction(download, ACTION.download), 'first generic action must download the script');
  assertSource(isAction(save, ACTION.save), 'second generic action must save the downloaded script');
  assertSource(download.WFWorkflowActionParameters.WFURL === EXPECTED_DOWNLOAD_URL,
    'unexpected generic download URL');
  const downloadId = download.WFWorkflowActionParameters.UUID;
  assertSource(typeof downloadId === 'string' && downloadId.length > 0, 'download action must have a UUID');
  const saveInput = inputValue(save.WFWorkflowActionParameters.WFInput);
  assertSource(saveInput?.Type === 'ActionOutput' && saveInput.OutputUUID === downloadId,
    'save action must explicitly consume the original download output');
  const ids = genericActions.map(action => action.WFWorkflowActionParameters.UUID).filter(Boolean);
  assertSource(new Set(ids).size === ids.length, 'duplicate generic action UUID');

  assertSource(premiumShortcut?.WFWorkflowName === 'Update Clube UOL Premium', 'unexpected Premium shortcut');
  const premiumActions = premiumShortcut.WFWorkflowActions;
  assertSource(Array.isArray(premiumActions) && premiumActions.length === 2, 'expected two Premium actions');
  const [dictionary, run] = premiumActions;
  assertSource(isAction(dictionary, ACTION.dictionary) && isAction(run, ACTION.run),
    'expected Premium dictionary followed by native Scriptable action');
  const dictionaryParameters = dictionary.WFWorkflowActionParameters;
  const runParameters = run.WFWorkflowActionParameters;
  const entries = dictionaryParameters.WFItems?.Value?.WFDictionaryFieldValueItems;
  assertSource(Array.isArray(entries) && entries.length === 1 && entries[0].WFItemType === 0
    && entries[0].WFKey?.Value?.string === 'action' && entries[0].WFValue?.Value?.string === 'update-auto',
  'unexpected Premium update dictionary');
  assertSource(typeof dictionaryParameters.UUID === 'string' && dictionaryParameters.UUID.length > 0
    && typeof runParameters.UUID === 'string' && runParameters.UUID.length > 0
    && dictionaryParameters.UUID !== runParameters.UUID, 'invalid Premium action UUIDs');
  assertSource(runParameters.fileName === PREMIUM_NAME && runParameters.ShowWhenRun === false
    && runParameters.runInApp === false && runParameters.parameter?.Value?.Type === 'ActionOutput'
    && runParameters.parameter.Value.OutputUUID === dictionaryParameters.UUID,
  'unexpected Premium native updater parameters');
}

function token(value) {
  return { Value: value, WFSerializationType: 'WFTextTokenAttachment' };
}

function outputToken(id, name) {
  return token({ Type: 'ActionOutput', OutputUUID: id, OutputName: name });
}

/**
 * Wrap an existing generic updater without changing its action parameters.
 * Callers keep the result private: the generic actions may contain a folder bookmark.
 */
function unifyShortcut(genericActions, premiumShortcut) {
  validateSources(genericActions, premiumShortcut);
  const generic = structuredClone(genericActions);
  const shortcut = structuredClone(premiumShortcut);
  const actions = [];
  const sourceIds = new Set(generic.map(action => action.WFWorkflowActionParameters.UUID).filter(Boolean));
  const insertedIds = new Set();

  function newId(name) {
    const id = uuid(`Update Scriptable/${name}`);
    assertSource(!sourceIds.has(id) && !insertedIds.has(id), 'generated UUID collision');
    insertedIds.add(id);
    return id;
  }

  function add(identifier, name, parameters = {}) {
    const id = newId(name);
    actions.push({ WFWorkflowActionIdentifier: identifier, WFWorkflowActionParameters: { ...parameters, UUID: id } });
    return id;
  }

  function startIf(name, input, text) {
    const group = newId(`${name}/group`);
    add(ACTION.conditional, `${name}/if`, {
      WFControlFlowMode: 0, GroupingIdentifier: group, WFInput: { Type: 'Variable', Variable: input },
      WFCondition: 99, WFConditionalActionString: text,
    });
    return { name, group };
  }

  function control(branch, mode) {
    add(ACTION.conditional, `${branch.name}/${mode === 1 ? 'else' : 'end'}`, {
      WFControlFlowMode: mode, GroupingIdentifier: branch.group,
    });
  }

  function stop(name, input) {
    add('is.workflow.actions.exit', `${name}/stop`, input ? { WFResult: input } : {});
  }

  function premium(name) {
    const copies = structuredClone(premiumShortcut.WFWorkflowActions);
    const replacements = new Map(copies.map((action, index) => [action.WFWorkflowActionParameters.UUID, newId(`${name}/premium/${index}`)]));
    function remap(value) {
      if (!value || typeof value !== 'object') return;
      for (const [key, child] of Object.entries(value)) {
        if ((key === 'UUID' || key === 'OutputUUID') && replacements.has(child)) value[key] = replacements.get(child);
        else remap(child);
      }
    }
    copies.forEach(remap);
    actions.push(...copies);
    stop(name);
  }

  const input = token({ Type: 'ExtensionInput' });
  const titlePremium = startIf('title-premium', input, PREMIUM_NAME);
  premium('title-premium');
  control(titlePremium, 1);
  const titleGeneric = startIf('title-generic', input, 'Scriptable');
  actions.push(generic[0]);
  const downloadId = generic[0].WFWorkflowActionParameters.UUID;
  const filenameId = add('is.workflow.actions.getitemname', 'download-name', {
    WFInput: outputToken(downloadId, 'Conteúdo do URL'), GetWebPageTitle: false,
  });
  for (const [name, match] of [['filename-premium', PREMIUM_NAME], ['filename-legacy-premium', LEGACY_PREMIUM_NAME]]) {
    const guard = startIf(name, outputToken(filenameId, 'Nome'), match);
    premium(name);
    control(guard, 2);
  }
  actions.push(...generic.slice(1));
  control(titleGeneric, 1);
  const diagnosticId = add('is.workflow.actions.gettext', 'unknown-title', { WFTextActionText: DIAGNOSTIC });
  stop('unknown-title', outputToken(diagnosticId, 'Texto'));
  control(titleGeneric, 2);
  control(titlePremium, 2);

  shortcut.WFWorkflowName = 'Update Scriptable';
  shortcut.WFWorkflowInputContentItemClasses = ['WFStringContentItem'];
  shortcut.WFWorkflowHasShortcutInputVariables = true;
  shortcut.WFWorkflowActions = actions;
  return shortcut;
}

/** Preserve the source workflow's trigger, compatibility and other metadata. */
function unifyWorkflow(genericWorkflow, premiumShortcut) {
  assertSource(genericWorkflow && typeof genericWorkflow === 'object' && !Array.isArray(genericWorkflow),
    'expected generic workflow object');
  const unified = unifyShortcut(genericWorkflow.WFWorkflowActions, premiumShortcut);
  const workflow = structuredClone(genericWorkflow);
  workflow.WFWorkflowName = 'Update Scriptable';
  workflow.WFWorkflowHasShortcutInputVariables = true;
  workflow.WFWorkflowActions = unified.WFWorkflowActions;
  return workflow;
}

module.exports = { unifyShortcut, unifyWorkflow };
