'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  // We use the Derive vertex attribute operation to generate attributes for the graph.
  // The attributes object below contains the attributes we are using:
  //     - the keys are the names of the attributes,
  //     - the corresponding values are the expression for the Derive vertex attribute operation.
  //
  // We have several attribute pairs which starts with the same letter but one of them is
  // capitalized while the other is all lower case letters to test how the sorting handles
  // the lower and the upper case.
  //
  var attributes = {
        'goals': '73',
        'points': '90',
        'draws': '12',
        'red cards': '3',
        'Dennis Bergkamp': '10',
        'Thiery Henry' : '14',
        'Fredrik Ljunberg': '11',
        'Ashley Cole': '3',
        'Patrick Viera': '4',
        'Robert Pires': '7'
        };

  var sortedAttributes = Object.keys(attributes).sort();

  fw.transitionTest(
  'empty test-example project',
  'attributes listed in order',
  function() {
    lib.left.runOperation('new vertex set', {size: 11});
    // We delete the default id and ordinal vertex attributes in order to only have the vertex
    // attributes listed under the attributes object. This is done to eliminate the possibility of
    // future surprises caused by not listed attributes if another test would ever use the
    // 'attributes listed in order' state its the starting state.
    lib.left.runOperation('discard vertex attribute', {name: 'id'});
    lib.left.runOperation('discard vertex attribute', {name: 'ordinal'});
    // Add the attributes to the graph.
    for (var attr in attributes) {
      if (attributes.hasOwnProperty(attr)) {
        lib.left.runOperation('add constant vertex attribute',
                              {name: attr, value: attributes[attr], type: 'Double'}
                             );
      }
    }

    // When opening an operation, the operation's panel can have different ways to ask the user
    // which attributes to use the operation on. E.g: for aggregation the attributes are listed
    // one attribute per line but in the vertex attribute for string operation the attributes are
    // chosen from a listbox. So we test the operations which have different looking panels.

    // Checking if the attributes listed for aggregation are in correct order.
    lib.left.openOperation('Aggregate vertex attribute globally');
    // The list of the attributes in the order they are displayed in the operation's panel.
    var aggrList = lib.left.toolbox.$$('operation-parameters .form-group label[for^=aggregate]');
    expect(aggrList.getText()).toEqual(sortedAttributes);
    lib.left.closeOperation();

    // Checking if the attributes listed for the Convert vertex attribute to string operation are in
    // correct order.
    lib.left.openOperation('Convert vertex attribute to string');
    // The list of the attributes in the order they are displayed in the listbox.
    var castList = lib.left.operationParameter(lib.left.toolbox, 'attr');
    expect(castList.getText()).toEqual(sortedAttributes.join('\n'));
    lib.left.closeOperation();

    // Checking if the attributes listed for the filter operation are in correct order.
    lib.left.openOperation('Filter by attribute');
    // The list of the attributes in the order they are displayed in the filter operation's panel.
    var filterList = lib.left.toolbox.$$('operation-parameters .form-group label[for^=filterva]');
    expect(filterList.getText()).toEqual(sortedAttributes);
    lib.left.closeOperation();
  },
  function() {});
};
*/
