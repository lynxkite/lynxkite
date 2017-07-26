'use strict';

var lib = require('../test-lib.js');

function tableIsGood(t) {
  t.expect(
    ['age', 'gender', 'id', 'income', 'location', 'name'],
    ['Double', 'String', 'Long', 'Double', '(Double, Double)', 'String'],
    [['20.3', 'Male', '0', '1000', '(40.71448,-74.00598)', 'Adam'],
     ['18.2', 'Female', '1', 'null', '(47.5269674,19.0323968)', 'Eve'],
     ['50.3', 'Male', '2', '2000', '(1.352083,103.819836)', 'Bob'],
     ['2', 'Male', '3', 'null', '(-33.8674869,151.2069902)', 'Isolated Joe']]);
}

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example workspace with example graph',
    'visualize with instrument',
    function() {
      lib.addConcurMatcher();
      var popup = lib.workspace.openStateView('eg0', 'project');
      popup.setInstrument(0, 'visualize', {});
      popup.left.vertexAttribute('name').visualizeAs('label');
      popup.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
          { label: 'Bob' },
        ]);
      });
      popup.close();
    });

  fw.statePreservingTest(
    'test-example workspace with example graph',
    'sql and plot with instrument',
    function() {
      var popup = lib.workspace.openStateView('eg0', 'project');
      popup.setInstrument(0, 'sql');
      tableIsGood(popup.table);
      popup.setInstrument(1, 'sql', {
        sql: 'select gender, mean(age) as age from input group by gender' });
      popup.setInstrument(2, 'plot');
      popup.plot.expectBarHeightsToBe(['225', '299']);
      popup.clearInstrument(1);
      tableIsGood(popup.table);
      popup.close();
    });
};
