'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example workspace with example graph',
    'metaeditor can change box kind and id',
    function() {
      let box = lib.workspace.openBoxEditor('eg0');
      const flip = () => box.popup.$('#flip-popup').click();
      const id = () => box.popup.$('#id input');
      const operation = () => box.popup.$('#operation-id input');

      // Check.
      flip();
      expect(id().getAttribute('value')).toBe('eg0');
      expect(operation().getAttribute('value')).toBe('Create example graph');

      // Change.
      id().sendKeys(lib.selectAllKey + 'new-id' + protractor.Key.ENTER);
      box = lib.workspace.getBoxEditor('new-id');
      operation().sendKeys(lib.selectAllKey + 'Import CSV');
      flip();
      expect(box.operationId()).toBe('Import CSV');
      box.expectParameter('delimiter', ',');

      // Restore.
      flip();
      expect(id().getAttribute('value')).toBe('new-id');
      expect(operation().getAttribute('value')).toBe('Import CSV');
      id().sendKeys(lib.selectAllKey + 'eg0' + protractor.Key.ENTER);
      box = lib.workspace.getBoxEditor('eg0');
      operation().sendKeys(lib.selectAllKey + 'Create example graph');
      flip();
      expect(box.operationId()).toBe('Create example graph');
    });
};
