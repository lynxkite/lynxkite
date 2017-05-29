'use strict';


module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');

  fw.transitionTest(
    'empty test-example workspace',
    'plot data imported as table',
    function() {
      lib.workspace.addBox({
        id: 'ib0',
        name: 'Import CSV',
        x: 100, y: 100 });
      var boxEditor = lib.workspace.openBoxEditor('ib0');
      var importPath = path.resolve(__dirname, 'data/plot_data.csv');
      boxEditor.populateOperation({
        'filename': importPath,
        'columns': 'product,cnt'
      });
      lib.loadImportedTable();
      boxEditor.close();
    },
    function() {
    }
  );

  fw.transitionTest(
    'plot data imported as table',
    'plot viewer opened',
    function() {
      lib.workspace.addBox({
        id: 'plot1',
        name: 'Create plot',
        x: 200, y: 200 });
      lib.workspace.connectBoxes('ib0', 'table', 'plot1', 'table');
      var plot_code = 'Vegas("Simple bar chart")\n.withData(Data)' +
        '.\nencodeX("product", Ordinal).\nencodeY("cnt", Quantitative).\nmark(Bar)';
      var boxEditor = lib.workspace.openBoxEditor('plot1');
      boxEditor.populateOperation({
        'plot_code': plot_code,
      });
      boxEditor.close();
      var plotState = lib.workspace.openStateView('plot1', 'plot');
      var plot = plotState.plot;
      plot.expectBarHeightsAre(['97', '191', '149', '316']);
    },
    function() {
    }
  );

};

