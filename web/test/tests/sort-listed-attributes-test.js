'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  var attributes = {
        'goals': 73,
        'points': 90,
        'draws': 12,
        'red cards': 3,
        'Dennis Bergkamp': 10,
        'Thiery Henry' : 14,
        'Fredrik Ljunberg': 11,
        'Ashley Cole': 3,
        'Patrick Viera': 4,
        'Robert Pires': 7
        };

  var expected = Object.keys(attributes).sort();

  fw.transitionTest(
  'empty test-example project',
  'the invincibles',
  function() {
    lib.left.runOperation('new vertex set',{size: 11});
    lib.left.runOperation('discard vertex attribute',{name:'id'});
    lib.left.runOperation('discard vertex attribute',{name:'ordinal'});
    for (var attr in attributes) {
      lib.left.runOperation('derived vertex attribute',{expr: attributes[attr], output: attr, type: 'double'});}

    // Checking for aggregation.
    lib.left.openOperation('Aggregate vertex attribute globally');
    var aggrList = lib.left.toolbox.$$('operation-parameters .form-group label[for^=aggregate]');
    expect(aggrList.getText()).toEqual(expected);
    lib.left.closeOperation();

    // Checking for parameter input mode like in casting operations.
    lib.left.openOperation('Vertex attribute to string');
    var castList = lib.left.operationParameter(lib.left.toolbox, 'attr');
    expect(castList.getText()).toEqual(expected.join('\n'));
    lib.left.closeOperation();

    // Checking for parameter input mode like in filter operations..
    lib.left.openOperation('Filter by attribute');
    var filterList = lib.left.toolbox.$$('operation-parameters .form-group label[for^=filterva]');
    expect(filterList.getText()).toEqual(expected);
    lib.left.closeOperation();

},
  function() {});

};
