// Like gulp-angular-filesort, but with a simple regex.
// No dependencies. No parse errors.
'use strict';

const through = require('through2');
const toposort = require('toposort');

module.exports = function() {
  const files = [];
  const dependencies = [];
  const modules = {};

  function transform(file, enc, done) {
    let matches = file.contents.toString().matchAll(
      /angular.module\(.*?\)\s*.(?:config|controller|directive|factory|filter|service)\(\s*['"](.*?)['"].*?function.*?\((.*?)\)/sg);
    for (const m of matches) {
      modules[m[1]] = file;
      for (const dep of m[2].trim().split(/,\s*/)) {
        dependencies.push([file, dep]);
      }
      dependencies.push([file, 'main']);
      if (file.path.endsWith('/app.js')) {
        modules.main = file;
      }
    }
    files.push(file);
    done();
  }

  function flush(done) {
    let deps = dependencies.filter(d => modules[d[1]]);
    for (const d of deps) {
      d[1] = modules[d[1]];
    }
    deps = deps.filter(d => d[0] !== d[1]);
    for (const file of toposort.array(files, deps).reverse()) {
      this.push(file);
    }
    done();
  }

  return through.obj(transform, flush);
};
