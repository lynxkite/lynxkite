'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test unexpected parameters',
    function() {
      lib.workspace.addBox({id: 'ex', name: 'Create example graph', x: 100, y: 100});
      lib.workspace.addBox({
        id: 'attr', name: 'Add constant vertex attribute',
        params: {name: 'x'},
        x: 100, y: 200, after: 'ex'});
      lib.workspace.addBox({
        id: 'agg', name: 'Aggregate on neighbors',
        params: {'aggregate_x': ['average', 'count']},
        x: 100, y: 300, after: 'attr'});
      var attr = lib.workspace.openBoxEditor('attr');
      var agg = lib.workspace.openBoxEditor('agg');
      var output = lib.workspace.getOutputPlug('agg');
      // Original setup.
      lib.expectElement(agg.operationParameter('aggregate_x'));
      lib.expectNotElement(agg.operationParameter('aggregate_y'));
      expect(output.getAttribute('class')).not.toContain('plug-progress-error');
      // Change attribute name.
      attr.populateOperation({name: 'y'});
      lib.expectElement(agg.operationParameter('aggregate_x'));
      lib.expectElement(agg.operationParameter('aggregate_y'));
      agg.expectParameter('aggregate_x', 'average,count');
      expect(output.getAttribute('class')).toContain('plug-progress-error');
      // Remove unexpected parameter.
      agg.removeParameter('aggregate_x');
      lib.expectNotElement(agg.operationParameter('aggregate_x'));
      lib.expectElement(agg.operationParameter('aggregate_y'));
      expect(output.getAttribute('class')).not.toContain('plug-progress-error');
    }, function() {});
};
