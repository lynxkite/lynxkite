'use strict';


module.exports = function(fw) {
  const lib = require('../test-lib.js');
  const path = require('path');

  fw.transitionTest(
    'empty test-example workspace',
    'regression data imported as vertices',
    function() {
      lib.workspace.addBox({
        id: 'ib0',
        name: 'Import CSV',
        x: 100, y: 100 });
      const boxEditor = lib.workspace.openBoxEditor('ib0');
      const importPath = path.resolve(__dirname, 'data/regression_data.csv');
      boxEditor.populateOperation({
        'filename': importPath
      });
      lib.loadImportedTable();
      boxEditor.close();
      lib.workspace.addBox({
        id: 'utv',
        name: 'Use table as vertices',
        x: 100, y: 200});
      lib.workspace.connectBoxes('ib0', 'table', 'utv', 'table');
    },
    function() {
      const state = lib.workspace.openStateView('utv', 'graph');
      expect(state.left.vertexCount()).toEqual(5);
      lib.workspace.closeLastPopup();
    }
  );

  fw.transitionTest(
    'regression data imported as vertices',
    'trained regression model',
    function() {
      lib.workspace.addBox({
        id: 'con0',
        name: 'Convert vertex attribute to number',
        x: 100, y: 300});
      lib.workspace.connectBoxes('utv', 'graph', 'con0', 'graph');
      lib.workspace.openBoxEditor('con0').populateOperation({
        'attr': ['age', 'yob']
      });
      lib.workspace.closeLastPopup();
      lib.workspace.addBox({
        id: 'train0',
        name: 'Train linear regression model',
        x: 100, y: 400});
      lib.workspace.connectBoxes('con0', 'graph', 'train0', 'graph');
      lib.workspace.openBoxEditor('train0').populateOperation({
        'name': 'age_from_yob',
        'label': 'age',
        'features': ['yob']
      });
      lib.workspace.closeLastPopup();
      const state = lib.workspace.openStateView('train0', 'graph');
      expect(state.left.scalarValue('age_from_yob').getText())
        .toBe('Linear regression model predicting age');
      const model = state.left.scalar('age_from_yob');
      const p = model.popup();
      expect(p.$('#model-method').getText()).toBe('Linear regression');
      expect(p.$('#model-label').getText()).toBe('age');
      expect(p.$('#model-features').getText()).toBe('yob');
      expect(p.$('#model-details').getText()).toMatch('intercept\\s*2015');
      model.popoff();
      lib.workspace.closeLastPopup();
    },
    function() {}
  );

  fw.transitionTest(
    'trained regression model',
    'prediction from regression model',
    function() {
      lib.workspace.addBox({
        id: 'pred0',
        name: 'Predict with model',
        x: 300, y: 430});
      lib.workspace.connectBoxes('train0', 'graph', 'pred0', 'graph');
      // The default value for feature is good this time.
      lib.workspace.openBoxEditor('pred0').populateOperation({
        'name': 'age_prediction',
      });
      lib.workspace.closeLastPopup();
      // Convert the predictions to a more convenient format to test.
      lib.workspace.addBox({
        id: 'derive0',
        name: 'Derive vertex attribute',
        x: 450, y: 430});
      lib.workspace.connectBoxes('pred0', 'graph', 'derive0', 'graph');
      lib.workspace.openBoxEditor('derive0').populateOperation({
        'output': 'age_prediction_string',
        'expr': 'age_prediction.toInt.toString'
      });
      lib.workspace.closeLastPopup();
      const ap = lib.workspace.openStateView('derive0', 'graph');
      const attr = ap.left.vertexAttribute('age_prediction_string');
      expect(attr.getHistogramValues()).toEqual([
        { title: '25', size: 100, value: 1 },
        { title: '35', size: 100, value: 1 },
        { title: '40', size: 100, value: 1 },
        { title: '49', size: 100, value: 1 },
        { title: '59', size: 100, value: 1 }
      ]);
    },
    function() {}
  );
};
