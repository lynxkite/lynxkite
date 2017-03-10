'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  var name = lib.state.vertexAttribute('name');
  var income = lib.state.vertexAttribute('income');
  var weight = lib.state.edgeAttribute('weight');

  fw.statePreservingTest(
    'test-example workspace with example graph state selected',
    'string vertex histogram looks good',
    function() {
      expect(name.getHistogramValues().then(lib.sortHistogramValues)).toEqual([
        { title: 'Adam', size: 100, value: 1 },
        { title: 'Bob', size: 100, value: 1 },
        { title: 'Eve', size: 100, value: 1 },
        { title: 'Isolated Joe', size: 100, value: 1 },
      ]);
    });
  fw.statePreservingTest(
    'test-example workspace with example graph state selected',
    'double vertex histogram looks good',
    function() {
      expect(income.getHistogramValues()).toEqual([
        { title : '1000.0-1050.0', size : 100, value : 1 },
        { title : '1050.0-1100.0', size : 0, value : 0 },
        { title : '1100.0-1150.0', size : 0, value : 0 },
        { title : '1150.0-1200.0', size : 0, value : 0 },
        { title : '1200.0-1250.0', size : 0, value : 0 },
        { title : '1250.0-1300.0', size : 0, value : 0 },
        { title : '1300.0-1350.0', size : 0, value : 0 },
        { title : '1350.0-1400.0', size : 0, value : 0 },
        { title : '1400.0-1450.0', size : 0, value : 0 },
        { title : '1450.0-1500.0', size : 0, value : 0 },
        { title : '1500.0-1550.0', size : 0, value : 0 },
        { title : '1550.0-1600.0', size : 0, value : 0 },
        { title : '1600.0-1650.0', size : 0, value : 0 },
        { title : '1650.0-1700.0', size : 0, value : 0 },
        { title : '1700.0-1750.0', size : 0, value : 0 },
        { title : '1750.0-1800.0', size : 0, value : 0 },
        { title : '1800.0-1850.0', size : 0, value : 0 },
        { title : '1850.0-1900.0', size : 0, value : 0 },
        { title : '1900.0-1950.0', size : 0, value : 0 },
        { title : '1950.0-2000.0', size : 100, value : 1 },
      ]);
    });
  fw.statePreservingTest(
    'test-example workspace with example graph state selected',
    'double edge histogram looks good',
    function() {
      expect(weight.getHistogramValues()).toEqual([
        { title : '1.00-1.15', size : 100, value : 1 },
        { title : '1.15-1.30', size : 0, value : 0 },
        { title : '1.30-1.45', size : 0, value : 0 },
        { title : '1.45-1.60', size : 0, value : 0 },
        { title : '1.60-1.75', size : 0, value : 0 },
        { title : '1.75-1.90', size : 0, value : 0 },
        { title : '1.90-2.05', size : 100, value : 1 },
        { title : '2.05-2.20', size : 0, value : 0 },
        { title : '2.20-2.35', size : 0, value : 0 },
        { title : '2.35-2.50', size : 0, value : 0 },
        { title : '2.50-2.65', size : 0, value : 0 },
        { title : '2.65-2.80', size : 0, value : 0 },
        { title : '2.80-2.95', size : 0, value : 0 },
        { title : '2.95-3.10', size : 100, value : 1 },
        { title : '3.10-3.25', size : 0, value : 0 },
        { title : '3.25-3.40', size : 0, value : 0 },
        { title : '3.40-3.55', size : 0, value : 0 },
        { title : '3.55-3.70', size : 0, value : 0 },
        { title : '3.70-3.85', size : 0, value : 0 },
        { title : '3.85-4.00', size : 100, value : 1 },
      ]);
    });
/*
  fw.statePreservingTest(
    'example graph with filters set',
    'soft filters are applied to string vertex histogram',
    function() {
      expect(name.getHistogramValues().then(lib.sortHistogramValues)).toEqual([
        { title: 'Adam', size: 100, value: 1 },
        { title: 'Bob', size: 0, value: 0 },
        { title: 'Eve', size: 100, value: 1 },
        { title: 'Isolated Joe', size: 0, value: 0 },
      ]);
    });
  fw.statePreservingTest(
    'example graph with filters set',
    'soft filters are applied to double edge histogram',
    function() {
      expect(weight.getHistogramValues()).toEqual([
        { title : '1.00-1.15', size : 0, value : 0 },
        { title : '1.15-1.30', size : 0, value : 0 },
        { title : '1.30-1.45', size : 0, value : 0 },
        { title : '1.45-1.60', size : 0, value : 0 },
        { title : '1.60-1.75', size : 0, value : 0 },
        { title : '1.75-1.90', size : 0, value : 0 },
        { title : '1.90-2.05', size : 100, value : 1 },
        { title : '2.05-2.20', size : 0, value : 0 },
        { title : '2.20-2.35', size : 0, value : 0 },
        { title : '2.35-2.50', size : 0, value : 0 },
        { title : '2.50-2.65', size : 0, value : 0 },
        { title : '2.65-2.80', size : 0, value : 0 },
        { title : '2.80-2.95', size : 0, value : 0 },
        { title : '2.95-3.10', size : 0, value : 0 },
        { title : '3.10-3.25', size : 0, value : 0 },
        { title : '3.25-3.40', size : 0, value : 0 },
        { title : '3.40-3.55', size : 0, value : 0 },
        { title : '3.55-3.70', size : 0, value : 0 },
        { title : '3.70-3.85', size : 0, value : 0 },
        { title : '3.85-4.00', size : 0, value : 0 },
      ]);
    });
  fw.statePreservingTest(
    'example graph with filters applied',
    'hard filters are applied to string vertex histogram',
    function() {
      expect(name.getHistogramValues().then(lib.sortHistogramValues)).toEqual([
        { title: 'Adam', size: 100, value: 1 },
        { title: 'Eve', size: 100, value: 1 },
      ]);
    });
  fw.statePreservingTest(
    'example graph with filters applied',
    'hard filters are applied to double edge histogram',
    function() {
      expect(weight.getHistogramValues()).toEqual([
        { title : '2.00-2.00', size : 100, value : 1 },
      ]);
    });*/

  fw.transitionTest(
    'empty test-example workspace',
    'precise mode histogram has precise number for large datasets',
    function() {
      lib.workspace.addBox('Create vertices', 100, 100);
      lib.workspace.editBox(0, {size: '123456'});
      lib.workspace.addBox('Add constant vertex attribute', 100, 200);
      lib.workspace.connectBoxes(0, 'project', 1, 'project');
      lib.workspace.editBox(1, {name: 'c'});
      lib.workspace.getOutputPlug(1, 'project').click();  // select state
      expect(lib.state.vertexAttribute('c').getHistogramValues(true)).toEqual([
        { title : '1.00-1.00', size: 100, value: 123456 },
      ]);
    },
    function() {});

};
