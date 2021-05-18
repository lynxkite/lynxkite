'use strict';


module.exports = function(fw) {
  const lib = require('../test-lib.js');
  const path = require('path');
  const fs = require('fs');

  fw.transitionTest(
    'empty test-example workspace',
    'plot data imported as table',
    function() {
      lib.workspace.addBox({
        id: 'ib0',
        name: 'Import CSV',
        x: 100, y: 100 });
      const boxEditor = lib.workspace.openBoxEditor('ib0');
      const importPath = path.resolve(__dirname, 'data/plot_data.csv');
      boxEditor.openGroup('Advanced settings').populateOperation({
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
        name: 'Custom plot',
        x: 200, y: 200 });
      lib.workspace.connectBoxes('ib0', 'table', 'plot1', 'table');
      const plotCode = fs.readFileSync(__dirname + '/data/plot_code.json', 'utf8');
      const boxEditor = lib.workspace.openBoxEditor('plot1');
      boxEditor.populateOperation({
        'plot_code': plotCode,
      });
      boxEditor.close();
      const plotState = lib.workspace.openStateView('plot1', 'plot');
      const plot = plotState.plot;
      plot.expectBarHeightsToBe([56, 110, 86, 182]);
    },
    function() {
    }
  );

};
