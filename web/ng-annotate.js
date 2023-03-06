// I've used this to burn the annotations.
// for f in `find app -name \*.js`; do node ng-annotate.js $f > tmp; mv tmp $f; done

const fs = require('fs');

let src = fs.readFileSync(process.argv[2], 'utf8');
src = src.replace(
  /(angular.module\(.*?\)\s*.(?:config|controller|directive|factory|filter|service)\(.*?)(function.*?)\((.*?)\)(.*?)^( ? ?)}\);/msg,
  (match, head, fn, list, code, indent) => list ?
    `${head}["${ list.trim().split(/,\s*/).join('", "') }", ${fn}(${list})${code}${indent}}]);`
    : match);
console.log(src.trim());
