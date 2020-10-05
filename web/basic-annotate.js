// Like gulp-ng-annotate, but with a simple regex.
// No dependencies. No parse errors.
'use strict';

const through = require('through2');

module.exports = through.obj(function (file, enc, done) {
  let src = file.contents.toString();
  src = src.replace(
    /(angular.module\(.*?\)\s*.(?:config|controller|directive|factory|filter|service)\(.*?)(function.*?)\((.*?)\)(.*?)^( ? ?)}\);/msg,
    (match, head, fn, list, code, indent) => list ?
      `${head}["${ list.trim().split(/,\s*/).join('", "') }", ${fn}(${list})${code}${indent}}]);`
      : match);
  file.contents = new Buffer(src);
  this.push(file);
  done();
});
