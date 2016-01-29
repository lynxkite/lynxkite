'use strict';

module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');
  var importPath = path.resolve(__dirname, 'regression_data.csv');

  fw.transitionTest(
    'empty test-example project',
    'regression data imported as vertices',
    function() {
      lib.left.runOperation('Import vertices from CSV files', {files: importPath});
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(5);
    }
  );

  fw.transitionTest(
    'regression data imported as vertices',
    'trained regression model',
    function() {
      lib.left.runOperation('Vertex attribute to double', {attr: 'age'});
      lib.left.runOperation('Vertex attribute to double', {attr: 'yob'});
      lib.left.runOperation('Train linear regression model', {
        name: 'age_from_yob',
        label: 'age',
        features: 'yob',
        method: 'R' // Typing in a combo box doesn't work. Select Ridge regression by first character.
      });
      expect(lib.left.scalar('age_from_yob').getText())
       .toBe('Ridge regression model predicting age');
    },
    function() {}
  );

  fw.transitionTest(
    'trained regression model',
    'prediction from regression model',
    function() {
      lib.left.openOperation('Predict from model');
      lib.left.populateOperationParameter(lib.left.toolbox, 'name', 'age_prediction');
      lib.left.populateInput('model-parameters-model-name', 'age_from_yob');
      lib.left.populateInput('model-parameters-model-yob', 'yob');
      lib.left.submitOperation(lib.left.toolbox);
      expect(lib.left.getHistogramValues('age_prediction')).toEqual('abc');
    },
    function() {},'solo'
  );
 };
