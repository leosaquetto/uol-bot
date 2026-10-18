const fs = require('node:fs');
const path = require('node:path');

// Tests keep exercising the same native/distribution blocks after extraction.
// This reader never executes the build or touches an installed script.
module.exports = function buildSource() {
  const root = path.join(__dirname, '..');
  const build = fs.readFileSync(path.join(root, 'build.js'), 'utf8');
  const host = fs.readFileSync(path.join(root, 'native/host.js'), 'utf8')
    .replace('__BUILD_UPDATER__', () => fs.readFileSync(path.join(root, 'native/updater.js'), 'utf8')); 
  const distribution = fs.readFileSync(path.join(root, 'distribute.cjs'), 'utf8');
  const body = distribution.slice(distribution.indexOf('// PUBLIC_ARTIFACT_START'), distribution.indexOf('// DISTRIBUTION_BODY_END'));
  return build + '\n' + host + '\n' + body;
};
