'use strict';


var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'example graph with filters set',
    function() {
      var state = lib.workspace.openStateView('eg0', 'project');
      state.left.vertexAttribute('name').setFilter('Adam,Eve,Bob');
      state.left.vertexAttribute('age').setFilter('<40');
      state.left.edgeAttribute('weight').setFilter('!1');
    },
    function() {
    });

  fw.transitionTest(
    'example graph with filters set',
    'example graph with filters applied',
    function() {
      // In LK2 we cannot directly apply attribute (soft) filters, we need a box.
      lib.workspace.closeLastPopup();
      lib.workspace.addBox({
        id: 'filter0',
        name: 'Filter by attributes',
        x: 100, y: 400});
      lib.workspace.connectBoxes('eg0', 'project', 'filter0', 'project');
      lib.workspace.openBoxEditor('filter0').populateOperation({
        'filterva_age': '<40',
        'filterva_name': 'Adam,Eve,Bob',
        'filterea_weight': '!1'
      });
      lib.workspace.closeLastPopup();
      var state = lib.workspace.openStateView('filter0', 'project');
    },
    function() {
      var state = lib.workspace.getStateView('filter0', 'project');
      expect(state.left.vertexCount()).toEqual(2);
      expect(state.left.edgeCount()).toEqual(1);
    });

};
