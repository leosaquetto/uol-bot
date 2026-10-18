const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = fs.readFileSync(path.join(__dirname, 'template.html'), 'utf8');
const layout = template.slice(template.indexOf('// HEADER_LAYOUT_START'), template.indexOf('// HEADER_LAYOUT_END'));
function headerHarness({ resizeObserver = true } = {}) {
  let height = 110, resizeCallback, mutationCallback;
  const values = new Map(), handlers = new Map(), classes = new Set();
  const header = { getBoundingClientRect: () => ({ height }), classList: { add: value => classes.add(value), remove: value => classes.delete(value) } };
  const status = { hidden: true, textContent: '' };
  const context = vm.createContext({
    document: {
      documentElement: { style: { setProperty: (key, value) => values.set(key, value) } },
      querySelector: selector => { assert.equal(selector, '.top-header'); return header; },
      getElementById: id => { assert.equal(id, 'hunterCollectionStatus'); return status; },
    },
    window: { scrollY: 0, addEventListener: (name, callback) => handlers.set(name, callback) },
    ...(resizeObserver ? { ResizeObserver: class { constructor(callback) { resizeCallback = callback; } observe(target) { assert.equal(target, header); } } } : {}),
    MutationObserver: class { constructor(callback) { mutationCallback = callback; } observe(target, options) { assert.equal(target, header); assert.equal(options.subtree, true); assert.equal(options.characterData, true); } },
  });
  vm.runInContext(layout, context);
  return { context, values, handlers, classes, status, setHeight: value => { height = value; },
    resized: () => resizeCallback(), mutated: () => mutationCallback() };
}

test('header offset follows its measured border box, wrapped status and viewport changes without losing scroll fade', () => {
  const h = headerHarness();
  h.context.installHeaderLayout();
  assert.equal(h.values.get('--hunter-header-height'), '110px');
  h.setHeight(143.4); h.resized();
  assert.equal(h.values.get('--hunter-header-height'), '144px', 'wrapped status cannot overlap the content');
  h.setHeight(122); h.handlers.get('resize')();
  assert.equal(h.values.get('--hunter-header-height'), '122px');
  h.setHeight(156); h.handlers.get('orientationchange')();
  assert.equal(h.values.get('--hunter-header-height'), '156px');
  h.context.window.scrollY = 30; h.context.handleHeaderScroll();
  assert.equal(h.classes.has('header-scrolled'), true);
  h.context.window.scrollY = 0; h.context.handleHeaderScroll();
  assert.equal(h.classes.has('header-scrolled'), false);
  h.setHeight(0); h.resized();
  assert.equal(h.values.get('--hunter-header-height'), '156px', 'transient zero does not collapse the reserved space');
});

test('older WebView status changes retain the complete diagnosis and update the offset through the observer fallback', () => {
  const h = headerHarness({ resizeObserver: false });
  h.context.installHeaderLayout();
  const start = template.indexOf('    function setHunterCollectionStatus(');
  const end = template.indexOf('\n    }', start) + '\n    }'.length;
  vm.runInContext(template.slice(start, end), h.context);
  h.context.setHunterCollectionStatus({ status: 'not_configured', reason: 'bootstrap_missing' });
  assert.equal(h.status.hidden, false);
  assert.match(h.status.textContent, /aguardando arquivo privado no iCloud/);
  h.setHeight(152); h.mutated();
  assert.equal(h.values.get('--hunter-header-height'), '152px');
  h.context.setHunterCollectionStatus({ status: 'disabled' });
  assert.equal(h.status.hidden, true);
  h.setHeight(98); h.mutated();
  assert.equal(h.values.get('--hunter-header-height'), '98px');
});

test('all tabs reserve measured header height plus a gap and the refresh SVG has explicit nonzero dimensions', () => {
  const body = template.match(/\n    body \{([^}]+)\}/)[1];
  assert.match(body, /padding-top: calc\(var\(--hunter-header-height\) \+ 12px\)/);
  assert.match(template, /window\.addEventListener\('load', \(\) => \{\s*installHeaderLayout\(\)/);
  const button = template.match(/<button[^>]*id="btnRefreshCatalog"[^>]*>([\s\S]*?)<\/button>/)[1];
  assert.match(button, /<svg[^>]*width="20"[^>]*height="20"[^>]*flex-shrink: 0/);
});
