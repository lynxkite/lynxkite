'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'SQL box added',
    function() {
      lib.workspace.addBox({
        id: 'sql', name: 'SQL1', x: 100, y: 200 });
      lib.workspace.connectBoxes('eg0', 'project', 'sql', 'input');
      var table = lib.workspace.openStateView('sql', 'table');
      table.expect(
        ['age', 'gender', 'id', 'income', 'location', 'name'],
        ['Double', 'String', 'Long', 'Double', '(Double, Double)', 'String'],
        [
          ['20.3', 'Male', '0', '1000', '(40.71448,-74.00598)', 'Adam'],
          ['18.2', 'Female', '1', 'null', '(47.5269674,19.0323968)', 'Eve'],
          ['50.3', 'Male', '2', '2000', '(1.352083,103.819836)', 'Bob'],
          ['2', 'Male', '3', 'null', '(-33.8674869,151.2069902)', 'Isolated Joe'],
        ]);
    }, function() {});

  function runSQL(query) {
    lib.workspace.editBox('sql', { sql: query });
    return lib.workspace.getStateView('sql', 'table');
  }

  function sqlTest(name, query, checks, solo) {
    // These tests do not fully preserve the state, as they change the query.
    // But they can be run in any order, since they do not depend on the currently set query.
    fw.statePreservingTest(
      'SQL box added',
      name,
      function() {
        checks(runSQL(query));
      }, solo);
  }

  function sqlSimpleTest(name, query, resultNames, resultTypes, resultRows, solo) {
    sqlTest(name, query, function(table) {
      table.expect(resultNames, resultTypes, resultRows);
    }, solo);
  }

  sqlSimpleTest(
    'SQL works for edge attributes',
    'select edge_comment, src_name from edges order by edge_comment',
    ['edge_comment', 'src_name'],
    ['String', 'String'],
    [
      [ 'Adam loves Eve', 'Adam' ],
      [ 'Bob envies Adam', 'Bob' ],
      [ 'Bob loves Eve', 'Bob' ],
      [ 'Eve loves Adam', 'Eve' ],
    ]);

  sqlSimpleTest(
    '"order by" works right',
    'select id, name from vertices order by name',
    ['id', 'name'],
    ['Long', 'String'],
    [
      [ '0', 'Adam' ],
      [ '2', 'Bob' ],
      [ '1', 'Eve' ],
      [ '3', 'Isolated Joe' ],
    ]);

  sqlTest(
    'sql result table ordering works right with numbers',
    'select age, name from vertices',
    function(table) {
      table.clickColumn('age');
      table.expect(
        ['age', 'name'],
        ['Double', 'String'],
        [
          [ '2', 'Isolated Joe' ],
          [ '18.2', 'Eve' ],
          [ '20.3', 'Adam' ],
          [ '50.3', 'Bob' ],
        ]);
      table.clickColumn('name');
      table.expect(
        ['age', 'name'],
        ['Double', 'String'],
        [
          [ '20.3', 'Adam' ],
          [ '50.3', 'Bob' ],
          [ '18.2', 'Eve' ],
          [ '2', 'Isolated Joe' ],
        ]);
      table.clickColumn('age');
      table.clickColumn('age');
      table.expect(
        ['age', 'name'],
        ['Double', 'String'],
        [
          [ '50.3', 'Bob' ],
          [ '20.3', 'Adam' ],
          [ '18.2', 'Eve' ],
          [ '2', 'Isolated Joe' ],
        ]);
    });

  sqlTest(
    'sql result table ordering works right with nulls',
    'select name, income from vertices',
    function(table) {
      table.clickColumn('income');
      table.expect(
        ['name', 'income'],
        ['String', 'Double'],
        [
          [ 'Eve', 'null' ],
          [ 'Isolated Joe', 'null' ],
          [ 'Adam', '1000' ],
          [ 'Bob', '2000' ],
        ]);
      table.clickColumn('income');
      table.expect(
        ['name', 'income'],
        ['String', 'Double'],
        [
          [ 'Bob', '2000' ],
          [ 'Adam', '1000' ],
          [ 'Isolated Joe', 'null' ],
          [ 'Eve', 'null' ],
        ]);
      table.close();
      lib.workspace.addBox({
        id: 'new_attr', name: 'Derive vertex attribute', x: 400, y: 150, after: 'eg0', params: {
          expr: 'income === 1000 ? \'apple\' : \'orange\'',
          output: 'new_attr',
          type: 'String',
        }
      });
      lib.workspace.connectBoxes('new_attr', 'project', 'sql', 'input');
      table = lib.workspace.openStateView('sql', 'table');
      table = runSQL('select new_attr from vertices');
      table.clickColumn('new_attr');
      table.expect(
        ['new_attr'],
        ['String'],
        [
          [ 'null' ],
          [ 'null' ],
          [ 'apple' ],
          [ 'orange' ],
        ]);
      table.close();
      lib.workspace.deleteBoxes(['new_attr']);
      lib.workspace.connectBoxes('eg0', 'project', 'sql', 'input');
      lib.workspace.openStateView('sql', 'table');
    });

  fw.transitionTest(
    'empty test-example workspace',
    'SQL runs nice on belongs to reached from project and segmentation',
    function() {
      lib.workspace.addBox({
        id: 'vs', name: 'Create vertices', x: 100, y: 100, params: { size: '100' }});
      lib.workspace.addBox({
        after: 'vs',
        id: 'rnd', name: 'Add random vertex attribute', x: 100, y: 200, params: { seed: '1' }});
      lib.workspace.addBox({
        after: 'rnd',
        id: 'copy', name: 'Copy graph into a segmentation', x: 100, y: 300 });
      lib.workspace.addBox({
        id: 'sql', name: 'SQL1', x: 100, y: 400 });
      lib.workspace.connectBoxes('copy', 'project', 'sql', 'input');
      lib.workspace.openStateView('sql', 'table');
      var table = runSQL(
        'select sum(base_random / segment_random) as sum from `self_as_segmentation|belongs_to`');
      table.expect(['sum'], ['Double'], [['100']]);
    }, function() {});

  /*
  fw.statePreservingTest(
    'test-example project with example graph',
    'Save SQL result as CSV works',
    function() {
      left.setSql('select name, age, income from vertices order by name');

      left.startSqlSaving();

      // Choose csv format.
      left.side.$('#exportFormat option[value="csv"]').click();

      // And go.
      lib.startDownloadWatch();
      left.executeSqlSaving();
      var downloadedFileName = lib.waitForNewDownload(/\.csv$/);
      lib.expectFileContents(
        downloadedFileName,
        'name,age,income\n' +
        'Adam,20.3,1000.0\n' +
        'Bob,50.3,2000.0\n' +
        'Eve,18.2,\n' +
        'Isolated Joe,2.0,\n');
    });

  fw.transitionTest(
    'empty test-example project',
    'table export and reimport',
    function() {
      left.runOperation('Create vertices', { size: '100' });
      left.runOperation('Add random vertex attribute', { name: 'random1', seed: '1' });
      left.runOperation('Add random vertex attribute', { name: 'random2', seed: '2' });
      left.runOperation('Add rank attribute', { keyattr: 'random1', rankattr: 'rank1' });
      left.runOperation('Add rank attribute', { keyattr: 'random2', rankattr: 'rank2' });

      left.setSql(
        'select cast(rank1 as string), cast(rank2 as string) from vertices');
      left.startSqlSaving();
      left.side.$('#exportFormat option[value="table"]').click();
      left.side.$('#exportKiteTable').sendKeys('Random Edges');
      left.executeSqlSaving();
      // Test overwriting.
      left.startSqlSaving();
      left.executeSqlSaving();
      lib.confirmSweetAlert('Entry already exists');

      left.runOperation('Convert vertex attribute to Double', { attr: 'ordinal' });
      left.runOperation('Convert vertex attribute to String', { attr: 'ordinal' });
      left.runOperation(
        'Import edges for existing vertices',
        {
          table: 'Random Edges',
          attr: 'ordinal',
          src: 'rank1',
          dst: 'rank2',
        });

      left.runSql('select sum(rank1) as r1sum, sum(rank2) as r2sum from edge_attributes');
      left.expectSqlResult(['r1sum', 'r2sum'], ['Double', 'Double'], [['4950', '4950']]);

      left.runSql(
        'select min(edge_rank1 = src_ordinal) as srcgood, min(edge_rank2 = dst_ordinal) as dstgood from edges');
      left.expectSqlResult(['srcgood', 'dstgood'], ['Boolean', 'Boolean'], [['true', 'true']]);
    },
    function() {
    });

  fw.transitionTest(
    'table export and reimport',
    'exported table can be edited',
    function() {
      lib.left.close();
      lib.splash.editTable('Random Edges');
      element(by.id('save-results-opener')).click();
      element(by.id('save-results')).click();
    },
    function() {
      expect(lib.errors()).toEqual([]);
    });

  fw.transitionTest(
    'test-example project with example graph',
    'parquet export and reimport right from the operation',
    function() {
      left.setSql(
        'select name, age, gender, income from vertices');
      left.startSqlSaving();
      left.side.$('#exportFormat option[value="parquet"]').click();
      var fileName = 'UPLOAD$/example.' + process.pid + '.parquet';
      left.side.$('#export-parquet-path').sendKeys(fileName);
      left.executeSqlSaving();

      left.runOperation('Discard vertices');
      left.openOperation('Import vertices');
      var tableKind = left.operationParameter(left.toolbox, 'table');
      tableKind.element(by.id('import-new-table-button')).click();
      tableKind.$('#table-name input').sendKeys('example reloaded as parquet');
      tableKind.element(by.cssContainingText('#datatype option', 'Parquet files')).click();
      tableKind.$('#parquet-filename input[ng-model="filename"]').sendKeys(fileName);
      tableKind.element(by.id('import-parquet-button')).click();
      left.submitOperation(left.toolbox);
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(4);
      // id, name, age, gender, income
      expect(lib.left.attributeCount()).toEqual(5);
    });

  fw.transitionTest(
    'empty test-example project',
    'test-example project with 100 vertices',
    function() {
      left.runOperation('Create vertices', { size: '100'});
      var maxRows = left.side.element(by.css('#max-rows'));

      maxRows.clear().sendKeys('1000');
      left.side.element(by.id('run-sql-button')).click();
      expect(left.side.all(by.css('#sql-result table tbody tr')).count()).toEqual(100);
      maxRows.clear().sendKeys('100');
      left.side.element(by.id('run-sql-button')).click();
      expect(left.side.all(by.css('#sql-result table tbody tr')).count()).toEqual(100);
      maxRows.clear().sendKeys('17');
      left.side.element(by.id('run-sql-button')).click();
      expect(left.side.all(by.css('#sql-result table tbody tr')).count()).toEqual(17);

      // Test "Show more" button.
      var showMore = left.side.element(by.css('#show-more'));
      showMore.click();
      expect(left.side.all(by.css('#sql-result table tbody tr')).count()).toEqual(27);
      showMore.click();
      expect(left.side.all(by.css('#sql-result table tbody tr')).count()).toEqual(37);
    },
    function() {
    });

     fw.transitionTest(
        'segmentation opens',
        'tables and views are saved in the project directory',
        function() {
          right.close();
          left.close();
          lib.splash.openProject('test-example');
          left.saveProjectAs('somesubdir/someproject');

          // Create new table
          left.side.element(by.id('save-results-opener')).click();
          left.side.element(by.css('#exportFormat > option[value=table]')).click();
          left.side.element(by.id('exportKiteTable')).sendKeys('somesubdirtable');
          left.side.element(by.id('save-results')).click();

          left.openSegmentation('bucketing');

          // Create view for segmentation
          right.side.element(by.id('save-results-opener')).click();
          right.side.element(by.css('#exportFormat > option[value=view]')).click();
          right.side.element(by.id('exportKiteTable')).sendKeys('segmview');
          right.side.element(by.id('save-results')).click();
        },
        function() {
          right.close();
          lib.showSelector();
          right.side.$('#directory-somesubdir').click();
          lib.splash.expectTableListed('somesubdirtable');
          lib.splash.expectViewListed('segmview');
          left.close();
          lib.splash.popDirectory();
        });

  fw.transitionTest(
    'test-example project with 100 vertices',
    'table and view export of 100 vertices',
    function() {
      // Create new table
      left.side.element(by.id('save-results-opener')).click();
      left.side.element(by.css('#exportFormat > option[value=table]')).click();
      left.side.element(by.id('exportKiteTable')).clear().sendKeys('exportedtable');
      left.side.element(by.id('save-results')).click();

      // Create new view
      left.side.element(by.id('save-results-opener')).click();
      left.side.element(by.css('#exportFormat > option[value=view]')).click();
      left.side.element(by.id('exportKiteTable')).clear().sendKeys('exportedview');
      left.side.element(by.id('save-results')).click();
    },
    function() {
      lib.showSelector();
      lib.splash.expectTableListed('exportedtable');
      lib.splash.expectViewListed('exportedview');
    });

  fw.transitionTest(
    'empty test-example project',
    'test-example project with sql history',
    function() {
      lib.showSelector();
      right.side.$('#global-sql-box').click();

      left.runSql('0');
      right.runSql('1');
      left.runSql('2');
      right.runSql('3');

      // Close then reopen global sql box to synchronize its query history
      right.side.element(by.css('#global-sql-box > .glyphicon-minus')).click();
      right.side.element(by.id('global-sql-box')).click();
    },
    function() {
      var K = protractor.Key;

      // Test synchronized sql box
      var editor = right.sqlEditor();
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.ARROW_UP)]);
      expect(lib.getACEText(editor)).toBe('3');
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.ARROW_UP)]);
      expect(lib.getACEText(editor)).toBe('2');
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.ARROW_UP)]);
      expect(lib.getACEText(editor)).toBe('1');
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.ARROW_UP)]);
      expect(lib.getACEText(editor)).toBe('0');
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.DOWN)]);
      expect(lib.getACEText(editor)).toBe('1');

      // Test non-synchronized sql box
      editor = left.sqlEditor();
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.ARROW_UP)]);
      expect(lib.getACEText(editor)).toBe('2');
      lib.sendKeysToACE(editor, [K.chord(K.CONTROL, K.ARROW_UP)]);
      expect(lib.getACEText(editor)).toBe('0');
    });
*/
};
