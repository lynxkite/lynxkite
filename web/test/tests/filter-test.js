'use strict';


const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'example graph with filters set',
    function() {
      const state = lib.workspace.openStateView('eg0', 'graph');
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
      lib.workspace.connectBoxes('eg0', 'graph', 'filter0', 'graph');
      lib.workspace.openBoxEditor('filter0').populateOperation({
        'filterva_age': '<40',
        'filterva_name': 'Adam,Eve,Bob',
        'filterea_weight': '!1'
      });
      lib.workspace.closeLastPopup();
      lib.workspace.openStateView('filter0', 'graph');
    },
    function() {
      const state = lib.workspace.getStateView('filter0', 'graph');
      expect(state.left.vertexCount()).toEqual(2);
      expect(state.left.edgeCount()).toEqual(1);
    });

};
