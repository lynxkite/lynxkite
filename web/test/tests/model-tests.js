'use strict';

module.exports = function() {};

/*
module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');
  var importPath = path.resolve(__dirname, 'data/regression_data.csv');

  fw.transitionTest(
    'empty test-example project',
    'regression data imported as vertices',
    function() {
      lib.left.runOperation('Import vertices', {table: importPath});
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(5);
    }
  );

  fw.transitionTest(
    'regression data imported as vertices',
    'trained regression model',
    function() {
      lib.left.runOperation('Convert vertex attribute to double', {attr: 'age'});
      lib.left.runOperation('Convert vertex attribute to double', {attr: 'yob'});
      lib.left.runOperation('Train linear regression model', {
        name: 'age_from_yob',
        label: 'age',
        features: 'yob',
        method: 'Linear regression',
      });
      expect(lib.left.scalarValue('age_from_yob').getText())
       .toBe('Linear regression model predicting age');
      var model = lib.left.scalar('age_from_yob');
      var p = model.popup();
      expect(p.$('#model-method').getText()).toBe('Linear regression');
      expect(p.$('#model-label').getText()).toBe('age');
      expect(p.$('#model-features').getText()).toBe('yob');
      expect(p.$('#model-details').getText()).toMatch('intercept\\s*2015');
      model.popoff();
    },
    function() {}
  );

  fw.transitionTest(
    'trained regression model',
    'prediction from regression model',
    function() {
      lib.left.openOperation('Predict from model');
      lib.left.populateOperation(lib.left.toolbox, { name: 'age_prediction'} );
      lib.left.populateOperationInput('model-name', 'age_from_yob');
      lib.left.populateOperationInput('model-feature-yob', 'yob');
      lib.left.submitOperation(lib.left.toolbox);
      // Convert the predictions to a more convenient format to test.
      lib.left.runOperation('Derive vertex attribute', {
        output: 'age_prediction',
        type: 'double',
        expr: 'age_prediction | 0'});
      lib.left.runOperation('Convert vertex attribute to string', {attr: 'age_prediction'});
      expect(lib.left.vertexAttribute('age_prediction').getHistogramValues()).toEqual([
        { title: '25.0', size: 100, value: 1 },
        { title: '35.0', size: 100, value: 1 },
        { title: '40.0', size: 100, value: 1 },
        { title: '49.0', size: 100, value: 1 },
        { title: '59.0', size: 100, value: 1 }
      ]);
    },
    function() {}
  );

  fw.statePreservingTest(
    'prediction from regression model',
    'history editor is okay',
    function() {
      lib.left.history.open();
      var predict = lib.left.history.getOperation(4);
      function get(id) {
        return predict.element(by.id(id)).$('option:checked').getText();
      }
      expect(get('model-name')).toBe('age_from_yob');
      expect(get('model-feature-yob')).toBe('yob');
      lib.left.history.close();
    });
 };
*/
