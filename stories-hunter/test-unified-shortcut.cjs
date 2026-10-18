'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { unifyShortcut, unifyWorkflow } = require('./shortcuts/unify.cjs');

const action = (identifier, UUID, parameters = {}) => ({
  WFWorkflowActionIdentifier: identifier,
  WFWorkflowActionParameters: { UUID, ...parameters },
});
const output = id => ({
  Type: 'Variable',
  Variable: { Value: { Type: 'ActionOutput', OutputUUID: id }, WFSerializationType: 'WFTextTokenAttachment' },
});
const text = string => ({ Value: { string, attachmentsByRange: {} }, WFSerializationType: 'WFTextTokenString' });

function sources() {
  // Synthetic values only. Never load an installed workflow or its folder bookmark.
  return {
    generic: [
      action('is.workflow.actions.downloadurl', '10000000-0000-4000-8000-000000000001', { WFURL: 'http://MacBook-Air-de-Leo.local:53318/scriptable' }),
      action('is.workflow.actions.documentpicker.save', '10000000-0000-4000-8000-000000000002', {
        WFInput: output('10000000-0000-4000-8000-000000000001').Variable, WFSaveFileOverwrite: true,
        WFFileDestination: { bookmark: 'SYNTHETIC-FOLDER-BOOKMARK' },
      }),
      action('is.workflow.actions.wait', '10000000-0000-4000-8000-000000000003', { WFDelayTime: 1 }),
      action('is.workflow.actions.runworkflow', '10000000-0000-4000-8000-000000000004', {
        WFWorkflowName: 'Synthetic existing continuation', WFInput: output('10000000-0000-4000-8000-000000000002'),
      }),
    ],
    premium: {
      WFWorkflowName: 'Update Clube UOL Premium', WFWorkflowClientVersion: '3036.0.4',
      WFWorkflowTypes: [], WFWorkflowInputContentItemClasses: [], WFWorkflowHasShortcutInputVariables: false,
      WFWorkflowActions: [
        action('is.workflow.actions.dictionary', '20000000-0000-4000-8000-000000000001', {
          WFItems: { Value: { WFDictionaryFieldValueItems: [{
            WFItemType: 0, WFKey: text('action'), WFValue: text('update-auto'),
          }] }, WFSerializationType: 'WFDictionaryFieldValue' },
        }),
        action('dk.simonbs.Scriptable.ParameterizedRunScriptIntent', '20000000-0000-4000-8000-000000000002', {
          fileName: 'Clube UOL Premium', ShowWhenRun: false, runInApp: false,
          parameter: output('20000000-0000-4000-8000-000000000001').Variable,
        }),
      ],
    },
  };
}

// Interpret only this workflow's control flow and side effects, with synthetic I/O.
function execute(shortcut, title, filename = 'Another Script.js') {
  const outputs = new Map();
  const events = [];
  const stack = [];
  let enabled = true;
  const resolve = input => {
    const value = (input?.Type === 'Variable' ? input.Variable : input)?.Value;
    if (value?.Type === 'ExtensionInput') return title;
    if (value?.Type === 'ActionOutput') {
      assert.ok(outputs.has(value.OutputUUID), 'action output must exist on the executed branch');
      return outputs.get(value.OutputUUID);
    }
    return input;
  };

  for (const entry of shortcut.WFWorkflowActions) {
    const id = entry.WFWorkflowActionIdentifier;
    const p = entry.WFWorkflowActionParameters;
    if (id === 'is.workflow.actions.conditional') {
      if (p.WFControlFlowMode === 0) {
        assert.equal(p.WFCondition, 99);
        const matches = enabled && String(resolve(p.WFInput) ?? '').includes(p.WFConditionalActionString);
        stack.push({ group: p.GroupingIdentifier, parent: enabled, matches });
        enabled = matches;
      } else {
        const branch = stack.at(-1);
        assert.equal(branch?.group, p.GroupingIdentifier);
        if (p.WFControlFlowMode === 1) enabled = branch.parent && !branch.matches;
        else {
          assert.equal(p.WFControlFlowMode, 2);
          enabled = stack.pop().parent;
        }
      }
      continue;
    }
    if (!enabled) continue;
    switch (id) {
      case 'is.workflow.actions.downloadurl':
        events.push('download');
        outputs.set(p.UUID, { name: filename, content: 'synthetic downloaded script' });
        break;
      case 'is.workflow.actions.getitemname':
        assert.equal(p.GetWebPageTitle, false);
        outputs.set(p.UUID, resolve(p.WFInput).name);
        break;
      case 'is.workflow.actions.documentpicker.save':
        events.push('save');
        assert.equal(resolve(p.WFInput).content, 'synthetic downloaded script');
        outputs.set(p.UUID, resolve(p.WFInput));
        break;
      case 'is.workflow.actions.dictionary': {
        const entries = p.WFItems.Value.WFDictionaryFieldValueItems;
        outputs.set(p.UUID, Object.fromEntries(entries.map(item => [item.WFKey.Value.string, item.WFValue.Value.string])));
        break;
      }
      case 'dk.simonbs.Scriptable.ParameterizedRunScriptIntent':
        assert.deepEqual(resolve(p.parameter), { action: 'update-auto' });
        events.push('premium');
        outputs.set(p.UUID, 'premium result');
        break;
      case 'is.workflow.actions.gettext':
        outputs.set(p.UUID, p.WFTextActionText);
        break;
      case 'is.workflow.actions.exit':
        return { events, result: p.WFResult ? resolve(p.WFResult) : undefined, stopped: true };
      default:
        if (p.WFInput) resolve(p.WFInput);
        events.push(id);
        outputs.set(p.UUID, id);
    }
  }
  assert.equal(stack.length, 0);
  return { events, stopped: false };
}

test('Premium notification goes directly to native updater without network or file writes', () => {
  const { generic, premium } = sources();
  const shortcut = unifyShortcut(generic, premium);
  for (const title of ['Clube UOL Premium', 'Scriptable — Clube UOL Premium: atualização']) {
    assert.deepEqual(execute(shortcut, title), { events: ['premium'], result: undefined, stopped: true });
  }
  assert.equal(shortcut.WFWorkflowName, 'Update Scriptable');
  assert.deepEqual(shortcut.WFWorkflowInputContentItemClasses, ['WFStringContentItem']);
  assert.equal(shortcut.WFWorkflowHasShortcutInputVariables, true);
});

test('missing, empty and unknown notification titles stop with diagnostic and no side effects', () => {
  const { generic, premium } = sources();
  const shortcut = unifyShortcut(generic, premium);
  for (const title of [undefined, null, '', 'Another application']) {
    const result = execute(shortcut, title);
    assert.deepEqual(result.events, []);
    assert.equal(result.stopped, true);
    assert.match(result.result, /título ausente ou não reconhecido/);
  }
});

test('ordinary Scriptable update preserves every legacy action and its explicit download input', () => {
  const { generic, premium } = sources();
  const before = structuredClone({ generic, premium });
  const shortcut = unifyShortcut(generic, premium);
  const sourceIds = new Set(generic.map(entry => entry.WFWorkflowActionParameters.UUID));
  const retained = shortcut.WFWorkflowActions.filter(entry => sourceIds.has(entry.WFWorkflowActionParameters.UUID));
  assert.equal(JSON.stringify(retained), JSON.stringify(generic));
  assert.deepEqual({ generic, premium }, before);
  assert.deepEqual(execute(shortcut, 'Scriptable'), {
    events: ['download', 'save', 'is.workflow.actions.wait', 'is.workflow.actions.runworkflow'], stopped: false,
  });
});

test('both current and old Premium filenames route to native updater before the generic save', () => {
  const { generic, premium } = sources();
  const shortcut = unifyShortcut(generic, premium);
  for (const filename of ['Clube UOL Premium.js', 'Clube UOL - Stories Hunter.js']) {
    assert.deepEqual(execute(shortcut, 'Scriptable', filename), {
      events: ['download', 'premium'], result: undefined, stopped: true,
    });
  }
});

test('whole-workflow generation preserves one trigger, folder and all source metadata without mutation', () => {
  const { generic, premium } = sources();
  const source = {
    WFWorkflowName: 'Synthetic original update',
    WFWorkflowActions: generic,
    WFWorkflowHasShortcutInputVariables: false,
    WFWorkflowTriggers: [{ WFWorkflowTriggerType: 'WFNotificationTrigger',
      App: 'synthetic.ntfy', Title: { Operator: 'contains', Value: 'Scriptable' },
      SyntheticFutureTriggerOption: { enabled: true } }],
    WFWorkflowIcon: { WFWorkflowIconStartColor: 12345, WFWorkflowIconGlyphNumber: 67890 },
    WFWorkflowInputContentItemClasses: ['WFStringContentItem', 'WFDictionaryContentItem'],
    WFWorkflowMinimumClientVersion: 5000,
    WFWorkflowMinimumClientVersionString: '5000',
    WFWorkflowClientVersion: '5001.2.3',
    WFWorkflowTypes: ['WatchKit', 'ActionExtension'],
    WFQuickActionSurfaces: ['SyntheticSurface'],
    WFWorkflowImportQuestions: [{ Category: 'SyntheticImportQuestion', ActionIndex: 1 }],
    SyntheticFutureMetadata: { nested: ['keep', { version: 7 }] },
  };
  const before = structuredClone({ source, premium });
  const workflow = unifyWorkflow(source, premium);
  const metadata = value => Object.fromEntries(Object.entries(value).filter(([key]) =>
    !['WFWorkflowName', 'WFWorkflowHasShortcutInputVariables', 'WFWorkflowActions'].includes(key)));
  assert.deepEqual(metadata(workflow), metadata(source));
  assert.equal(workflow.WFWorkflowName, 'Update Scriptable');
  assert.equal(workflow.WFWorkflowHasShortcutInputVariables, true);
  assert.equal(workflow.WFWorkflowTriggers.length, 1);
  const save = workflow.WFWorkflowActions.find(entry => entry.WFWorkflowActionIdentifier === 'is.workflow.actions.documentpicker.save');
  assert.equal(JSON.stringify(save), JSON.stringify(generic[1]));
  assert.deepEqual(execute(workflow, 'Scriptable'), {
    events: ['download', 'save', 'is.workflow.actions.wait', 'is.workflow.actions.runworkflow'], stopped: false,
  });
  assert.deepEqual({ source, premium }, before);
  workflow.WFWorkflowTriggers[0].SyntheticFutureTriggerOption.enabled = false;
  workflow.SyntheticFutureMetadata.nested[1].version = 99;
  save.WFWorkflowActionParameters.WFFileDestination.bookmark = 'CHANGED-SYNTHETIC-BOOKMARK';
  assert.deepEqual({ source, premium }, before, 'output must not alias nested source objects');
});

test('whole-workflow generation rejects invalid objects and missing action arrays', () => {
  const { premium } = sources();
  for (const source of [undefined, null, [], 'workflow', {}, { WFWorkflowActions: {} }]) {
    assert.throws(() => unifyWorkflow(source, premium), /Cannot unify shortcuts:/);
  }
});

test('inserted UUIDs are deterministic and unique; all references target earlier actions', () => {
  const { generic, premium } = sources();
  const shortcut = unifyShortcut(generic, premium);
  assert.deepEqual(shortcut, unifyShortcut(generic, premium));
  const seen = new Set();
  const groups = new Map();
  function checkReferences(value) {
    if (!value || typeof value !== 'object') return;
    if (value.OutputUUID) assert.ok(seen.has(value.OutputUUID), 'reference must target an earlier action');
    for (const child of Object.values(value)) checkReferences(child);
  }
  for (const entry of shortcut.WFWorkflowActions) {
    const p = entry.WFWorkflowActionParameters;
    checkReferences(p);
    assert.ok(!seen.has(p.UUID), 'action UUIDs must be unique');
    assert.match(p.UUID, /^[0-9A-F-]{36}$/);
    seen.add(p.UUID);
    if (p.GroupingIdentifier) groups.set(p.GroupingIdentifier, (groups.get(p.GroupingIdentifier) || 0) + 1);
  }
  assert.equal(groups.size, 4);
  for (const [group, count] of groups) {
    assert.ok(!seen.has(group), 'group UUID must differ from action UUIDs');
    assert.ok(count === 2 || count === 3);
  }
  const runs = shortcut.WFWorkflowActions.filter(entry => entry.WFWorkflowActionIdentifier === 'dk.simonbs.Scriptable.ParameterizedRunScriptIntent');
  assert.equal(runs.length, 3);
  assert.equal(new Set(runs.map(entry => entry.WFWorkflowActionParameters.parameter.Value.OutputUUID)).size, 3);
});

test('generated input and stop parameters follow their native action schemas', () => {
  const { generic, premium } = sources();
  const actions = unifyShortcut(generic, premium).WFWorkflowActions;
  const conditionals = actions.filter(entry => entry.WFWorkflowActionIdentifier === 'is.workflow.actions.conditional'
    && entry.WFWorkflowActionParameters.WFControlFlowMode === 0);
  assert.equal(conditionals.length, 4);
  for (const entry of conditionals) {
    const input = entry.WFWorkflowActionParameters.WFInput;
    assert.equal(input.Type, 'Variable');
    assert.equal(input.Variable.WFSerializationType, 'WFTextTokenAttachment');
  }
  assert.deepEqual(conditionals[0].WFWorkflowActionParameters.WFInput.Variable.Value, { Type: 'ExtensionInput' });
  assert.deepEqual(conditionals[1].WFWorkflowActionParameters.WFInput.Variable.Value, { Type: 'ExtensionInput' });
  const name = actions.find(entry => entry.WFWorkflowActionIdentifier === 'is.workflow.actions.getitemname');
  assert.deepEqual(name.WFWorkflowActionParameters.WFInput, {
    Value: { Type: 'ActionOutput', OutputUUID: generic[0].WFWorkflowActionParameters.UUID, OutputName: 'Conteúdo do URL' },
    WFSerializationType: 'WFTextTokenAttachment',
  });
  const stops = actions.filter(entry => entry.WFWorkflowActionIdentifier === 'is.workflow.actions.exit');
  assert.equal(stops.length, 4);
  for (const stop of stops) assert.ok(!Object.hasOwn(stop.WFWorkflowActionParameters, 'WFInput'));
  const diagnosticStop = stops.at(-1).WFWorkflowActionParameters;
  assert.equal(diagnosticStop.WFResult.WFSerializationType, 'WFTextTokenAttachment');
  assert.equal(diagnosticStop.WFResult.Value.Type, 'ActionOutput');
  const diagnostic = actions.find(entry => entry.WFWorkflowActionIdentifier === 'is.workflow.actions.gettext');
  assert.equal(diagnosticStop.WFResult.Value.OutputUUID, diagnostic.WFWorkflowActionParameters.UUID);
});

test('unexpected source shapes and implicit save inputs are rejected', () => {
  const mutations = [
    ({ generic }) => generic.splice(0),
    ({ generic }) => { generic[0].WFWorkflowActionIdentifier = 'is.workflow.actions.gettext'; },
    ({ generic }) => { generic[0].WFWorkflowActionParameters.WFURL = 'https://example.invalid/unexpected'; },
    ({ generic }) => { generic[0].WFWorkflowActionParameters.WFURL = output('unknown'); },
    ({ generic }) => { generic[1].WFWorkflowActionIdentifier = 'is.workflow.actions.deletefiles'; },
    ({ generic }) => { delete generic[1].WFWorkflowActionParameters.WFInput; },
    ({ generic }) => { generic[1].WFWorkflowActionParameters.WFInput = output('unknown'); },
    ({ generic }) => { generic[2].WFWorkflowActionParameters.UUID = generic[0].WFWorkflowActionParameters.UUID; },
    ({ premium }) => { premium.WFWorkflowName = 'Unexpected'; },
    ({ premium }) => premium.WFWorkflowActions.push(action('is.workflow.actions.downloadurl', 'unexpected')),
    ({ premium }) => { premium.WFWorkflowActions[0].WFWorkflowActionParameters.WFItems.Value.WFDictionaryFieldValueItems[0].WFValue.Value.string = 'another-action'; },
    ({ premium }) => { premium.WFWorkflowActions[1].WFWorkflowActionParameters.parameter.Value.OutputUUID = 'unknown'; },
    ({ premium }) => { premium.WFWorkflowActions[1].WFWorkflowActionParameters.runInApp = true; },
  ];
  for (const mutate of mutations) {
    const fixture = sources();
    mutate(fixture);
    assert.throws(() => unifyShortcut(fixture.generic, fixture.premium), /Cannot unify shortcuts:/);
  }
});
