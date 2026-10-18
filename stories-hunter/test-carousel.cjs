const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const template = require('./assemble-ui.cjs').assembleUI();
function fn(name) {
  const start = template.indexOf('    function ' + name + '(');
  assert.ok(start > 0, name);
  return template.slice(start, template.indexOf('\n    }', start) + '\n    }'.length);
}
function css(selector) {
  const start = template.indexOf('    ' + selector + ' {');
  assert.ok(start > 0, selector);
  return Object.fromEntries(template.slice(start + selector.length + 6, template.indexOf('}', start))
    .split(';').map(item => item.trim()).filter(Boolean).map(item => {
      const colon = item.indexOf(':'); return [item.slice(0, colon).trim(), item.slice(colon + 1).trim()];
    }));
}
function element() {
  return { children: [], style: {}, appendChild(child) { this.children.push(child); } };
}
function carouselHarness() {
  const created = [];
  const context = vm.createContext({
    document: { createElement: element },
    createPosterCard: (group, index, destaque, hero) => {
      const card = { group, index, destaque, hero }; created.push(card); return card;
    },
  });
  vm.runInContext(fn('createCategoryCarousel') + fn('createDestaqueHeroSection'), context);
  return { context, created };
}

test('single and multiple common cards use the same inset track without altering cards or order', () => {
  for (const count of [1, 2, 5]) {
    const h = carouselHarness();
    const groups = Array.from({ length: count }, (_, index) => ({ key: 'offer-' + index }));
    const section = h.context.createCategoryCarousel('EXPOSICOES', 'Exposições', groups, false, '');
    assert.equal(section.className, 'category-carousel-section');
    assert.equal(section.children[0].className, 'category-carousel-header');
    assert.equal(section.children[1].className, 'category-carousel-track');
    assert.equal(section.children[1].children.length, count);
    assert.deepEqual(h.created.map(card => card.group.key), groups.map(group => group.key));
    assert.ok(h.created.every(card => card.destaque === false && card.hero === false));
    assert.deepEqual(section.children[1].style, {}, 'no item-count branch overrides the shared inset');
  }
});

test('overflow snap geometry retains sixteen-pixel leading and trailing gutters', () => {
  const track = css('.category-carousel-track'), card = css('.poster-card');
  const gutter = Number.parseFloat(track['scroll-padding-inline']);
  const gap = Number.parseFloat(track.gap), width = Number.parseFloat(card.width);
  assert.equal(gutter, 16); assert.equal(track.padding, '6px 16px 14px 16px');
  assert.equal(track['margin-left'], '-16px'); assert.equal(track['margin-right'], '-16px');
  assert.equal(track['overflow-x'], 'auto'); assert.equal(track['scroll-snap-type'], 'x mandatory');
  assert.equal(card['scroll-snap-align'], 'start'); assert.equal(width, 225);
  for (const viewport of [320, 390]) {
    for (const count of [1, 2, 5]) {
      // The body has 16px gutters; equal negative track margins restore its
      // viewport width. Snap positions must account for the padded snapport.
      const extent = gutter * 2 + count * width + (count - 1) * gap;
      const maxScroll = Math.max(0, extent - viewport);
      const firstStart = gutter;
      const firstSnap = Math.min(maxScroll, Math.max(0, firstStart - gutter));
      assert.equal(firstStart - firstSnap, 16, count + ' cards keep the first gutter');
      if (maxScroll > 0) {
        const lastStart = gutter + (count - 1) * (width + gap);
        const lastSnap = Math.min(maxScroll, Math.max(0, lastStart - gutter));
        assert.equal(viewport - (lastStart + width - lastSnap), 16, count + ' cards keep the final gutter');
      }
    }
  }
  assert.equal(css('.category-carousel-section > .category-carousel-header').padding, '0', 'ordinary titles align with the body gutter');
});

test('Destaques retain their separate hero container and original header inset', () => {
  const h = carouselHarness();
  const section = h.context.createDestaqueHeroSection([{ key: 'highlight' }]);
  assert.equal(section.className, 'destaque-hero-section');
  assert.equal(section.children[1].className, 'destaque-hero-container');
  assert.equal(h.created[0].hero, true);
  assert.equal(css('.category-carousel-header').padding, '0 4px');
});
