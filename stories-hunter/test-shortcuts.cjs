const {test}=require('node:test'),assert=require('node:assert/strict');
const shortcut=require('./shortcuts/Update Clube UOL Premium.json');
test('dedicated Shortcut passes dictionary to native Scriptable without menus, HTTP or file writes',()=>{
  assert.equal(shortcut.WFWorkflowName,'Update Clube UOL Premium');
  const [dictionary,run]=shortcut.WFWorkflowActions;assert.equal(shortcut.WFWorkflowActions.length,2);
  assert.equal(dictionary.WFWorkflowActionIdentifier,'is.workflow.actions.dictionary');
  const entries=dictionary.WFWorkflowActionParameters.WFItems.Value.WFDictionaryFieldValueItems;
  assert.equal(entries.length,1);assert.equal(entries[0].WFItemType,0);assert.equal(entries[0].WFKey.Value.string,'action');assert.equal(entries[0].WFValue.Value.string,'update-auto');
  assert.equal(run.WFWorkflowActionIdentifier,'dk.simonbs.Scriptable.ParameterizedRunScriptIntent');
  const p=run.WFWorkflowActionParameters;assert.equal(p.fileName,'Clube UOL Premium');assert.equal(p.ShowWhenRun,false);assert.equal(p.runInApp,false);
  assert.equal(p.parameter.Value.OutputUUID,dictionary.WFWorkflowActionParameters.UUID);
});
test('dedicated launcher opens new canonical name',()=>{
  const s=require('./shortcuts/Clube UOL Premium.json');assert.equal(s.WFWorkflowName,'Clube UOL Premium');
  assert.equal(s.WFWorkflowActions.length,1);assert.equal(s.WFWorkflowActions[0].WFWorkflowActionParameters.WFInput,'scriptable:///run/Clube%20UOL%20Premium');
});
