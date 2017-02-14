'use strict';

var testLib; // Forward declarations.
var History; // Forward declarations.
var TableBrowser; // Forward declarations.
var request = require('request');
var fs = require('fs');

var K = protractor.Key;  // Short alias.

// Mirrors the "id" filter.
function toID(x) {
  return x.toLowerCase().replace(/ /g, '-');
}


function Entity(side, kind, name) {
  this.side = side;
  this.kind = kind;
  this.name = name;
  this.kindName = kind + '-' + name;
  this.element = this.side.$('#' + this.kindName);
  this.menu = $('#menu-' + this.kindName);
}

Entity.prototype = {

  isPresent: function() {
    return this.element.isPresent();
  },

  isDisplayed: function() {
    return this.element.isDisplayed();
  },

  popup: function() {
    this.menu.isPresent().then(present => {
      if (!present) { this.element.click(); }
    });
    return this.menu;
  },

  popoff: function() {
    this.element.isPresent().then(present => {
      if (present) { this.element.evaluate('closeMenu()'); }
    });
  },

  setFilter: function(filterValue) {
    var filterBox = this.popup().$('#filter');
    filterBox.clear();
    filterBox.sendKeys(filterValue).submit();
    this.popoff();
  },

  getHistogramValues: function(precise) {
    precise = precise || false;
    var popup = this.popup();
    var histogram = popup.$('histogram');
    // The histogram will be automatically displayed if the attribute is already computed.
    // Click the menu item otherwise.
    histogram.isDisplayed().then(displayed => {
      if (!displayed) { popup.$('#show-histogram').click(); }
    });
    expect(histogram.isDisplayed()).toBe(true);
    if (precise) {
      popup.$('#precise-histogram-calculation').click();
    }
    function allFrom(td) {
      var toolTip = td.getAttribute('drop-tooltip');
      var style = td.$('.bar').getAttribute('style');
      return protractor.promise.all([toolTip, style]).then(function(results) {
        var toolTipMatch = results[0].match(/^(.*): (\d+)$/);
        var styleMatch = results[1].match(/^height: (\d+)%;$/);
        return {
          title: toolTipMatch[1],
          size: parseInt(styleMatch[1]),
          value: parseInt(toolTipMatch[2]),
        };
      });
    }
    var tds = histogram.$$('.bar-container');
    var res = tds.then(function(tds) {
      var res = [];
      for (var i = 0; i < tds.length; i++) {
        res.push(allFrom(tds[i]));
      }
      return protractor.promise.all(res);
    });

    var total = histogram.$('#histogram-total');
    protractor.promise.all([total.getText(), res]).then(function(results) {
      var totalValue = results[0].match(/histogram total: ([0-9,]+)/)[1];
      var values = results[1];
      var total = parseInt(totalValue.replace(/,/g, ''));
      var sum = 0;
      for (var j = 0; j < values.length; j++) {
        sum += values[j].value;
      }
      expect(total).toEqual(sum);
    });
    this.popoff();
    return res;
  },

  visualizeAs: function(visualization) {
    this.popup().$('#visualize-as-' + visualization).click();
    testLib.expectElement(this.visualizedAs(visualization));
    this.popoff();
  },

  visualizedAs: function(visualization) {
    return this.element.$('#visualized-as-' + visualization);
  },

  doNotVisualizeAs: function(visualization) {
    this.popup().$('#visualize-as-' + visualization).click();
    testLib.expectNotElement(this.visualizedAs(visualization));
    this.popoff();
  },

  slider: function() {
    return this.popup().$('#slider');
  },

  clickMenu: function(id) {
    this.popup().$('#' + id).click();
    this.popoff();
  },
};


function Side(direction) {
  this.direction = direction;
  this.side = element(by.id('side-' + direction));
  this.toolbox = element(by.id('operation-toolbox-' + direction));
  this.history = new History(this);
  this.tableBrowser = new TableBrowser(this.side);
}

Side.prototype = {
  expectCurrentProjectIs: function(name) {
    expect(this.side.evaluate('side.project.$error')).toBeFalsy();
    expect(this.side.evaluate('side.state.projectName')).toBe(name);
  },

  expectCurrentProjectIsError: function() {
    expect(this.side.evaluate('side.project.$error')).toBeTruthy();
  },

  // Only for opening the second project next to an already open project.
  openSecondProject: function(project) {
    testLib.showSelector();
    this.side.$('#project-' + toID(project)).click();
  },

  close: function() {
    this.side.element(by.id('close-project')).click();
  },

  evaluate: function(expr) {
    return this.side.evaluate(expr);
  },

  applyFilters: function() {
    return this.side.element(by.id('apply-filters-button')).click();
  },

  getCategorySelector: function(categoryTitle) {
    return this.toolbox.$('div.category[tooltip="' + categoryTitle + '"]');
  },

  getProjectHistory: function() {
    return this.side.$('div.project.history');
  },

  getValue: function(id) {
    var asStr = this.side.$('value#' + id + ' span.value').getText();
    return asStr.then(function(asS) { return parseInt(asS); });
  },

  getWorkflowCodeEditor: function() {
    return this.side.element(by.id('workflow-code-editor'));
  },

  getPythonWorkflowCodeEditor: function() {
    return this.side.element(by.id('python-code-editor'));
  },

  getWorkflowDescriptionEditor: function() {
    return this.side.element(by.id('workflow-description'));
  },

  getWorkflowNameEditor: function() {
    return this.side.element(by.id('workflow-name'));
  },

  clickWorkflowEditButton: function() {
    return this.toolbox.element(by.id('edit-operation-button')).click();
  },

  getWorkflowSaveButton: function() {
    return this.side.element(by.id('save-workflow-button'));
  },

  edgeCount: function() {
    return this.getValue('edge-count');
  },

  vertexCount: function() {
    return this.getValue('vertex-count');
  },

  segmentCount: function() {
    return this.getValue('segment-count');
  },

  openOperation: function(name) {
    this.toolbox.element(by.id('operation-search')).click();
    this.toolbox.element(by.id('filter')).sendKeys(name, K.ENTER);
  },

  closeOperation: function() {
    this.toolbox.$('div.category.active').click();
  },

  openWorkflowSavingDialog: function() {
    this.side.element(by.id('save-as-workflow-button')).click();
  },

  closeWorkflowSavingDialog: function() {
    this.side.element(by.id('close-workflow-button')).click();
  },

  openSegmentation: function(segmentationName) {
    this.segmentation(segmentationName).clickMenu('open-segmentation');
  },

  redoButton: function() {
    return this.side.element(by.id('redo-button'));
  },

  operationParameter: function(opElement, param) {
    return opElement.$('operation-parameters #' + param + ' .operation-attribute-entry');
  },

  populateOperation: function(parentElement, params) {
    params = params || {};
    for (var key in params) {
      testLib.setParameter(this.operationParameter(parentElement, key), params[key]);
    }
  },

  populateOperationInput: function(parameterId, param) {
    this.toolbox.element(by.id(parameterId)).sendKeys(testLib.selectAllKey + param);
  },

  submitOperation: function(parentElement) {
    var button = parentElement.$('.ok-button');
    // Wait for uploads or whatever.
    testLib.wait(protractor.ExpectedConditions.textToBePresentInElement(button, 'OK'));
    button.click();
  },

  runOperation: function(name, params) {
    this.openOperation(name);
    this.populateOperation(this.toolbox, params);
    this.submitOperation(this.toolbox);
  },

  expectOperationScalar: function(name, text) {
    var cssSelector = 'value[ref="scalars[\'' + name + '\']"';
    var valueElement = this.toolbox.$(cssSelector);
    expect(valueElement.getText()).toBe(text);
  },

  toggleSampledVisualization: function() {
    this.side.element(by.id('sampled-mode-button')).click();
  },

  toggleBucketedVisualization: function() {
    this.side.element(by.id('bucketed-mode-button')).click();
  },

  undoButton: function() {
    return this.side.element(by.id('undo-button'));
  },

  attributeCount: function() {
    return this.side.$$('entity[kind="vertex-attribute"], entity[kind="edge-attribute"]').count();
  },

  setSampleRadius: function(radius) {
    this.side.$('#setting-sample-radius').click();
    var slider = $('#sample-radius-slider');
    slider.getAttribute('value').then(function(value) {
      var diff = radius - value;
      while (diff > 0) {
        slider.sendKeys(K.RIGHT);
        diff -= 1;
      }
      while (diff < 0) {
        slider.sendKeys(K.LEFT);
        diff += 1;
      }
    });
  },

  scalarValue: function(name) {
    return this.side.element(by.id('scalar-value-' + toID(name)));
  },

  saveProjectAs: function(newName) {
    this.side.element(by.id('save-as-starter-button')).click();
    this.side.element(by.id('save-as-input')).sendKeys(testLib.selectAllKey + newName);
    this.side.element(by.id('save-as-button')).click();
  },

  sqlEditor: function() {
    return this.side.element(by.id('sql-editor'));
  },

  setSql: function(sql) {
    testLib.sendKeysToACE(this.sqlEditor(), sql);
  },

  // If sql is left undefined then we run whatever is already in the query box.
  runSql: function(sql) {
    if (sql !== undefined) {
      this.setSql(sql);
    }
    this.side.element(by.id('run-sql-button')).click();
  },

  expectSqlResult: function(names, types, rows) {
    var res = this.side.$('#sql-result');
    expect(res.$$('thead tr th span.sql-column-name').map(e => e.getText())).toEqual(names);
    expect(res.$$('thead tr th span.sql-type').map(e => e.getText())).toEqual(types);
    expect(res.$$('tbody tr').map(e => e.$$('td').map(e => e.getText()))).toEqual(rows);
  },

  startSqlSaving: function() {
    this.side.element(by.id('save-results-opener')).click();
  },

  executeSqlSaving: function() {
    this.side.element(by.id('save-results')).click();
  },

  vertexAttribute: function(name) { return new Entity(this.side, 'vertex-attribute', name); },
  edgeAttribute: function(name) { return new Entity(this.side, 'edge-attribute', name); },
  scalar: function(name) { return new Entity(this.side, 'scalar', name); },
  segmentation: function(name) { return new Entity(this.side, 'segmentation', name); },
};

function TableBrowser(root) {
  this.root = root;
}

TableBrowser.prototype = {
  toggle: function() {
    this.root.element(by.id('toggle-table-browser')).click();
  },

  getNode: function(posList) {
    var pos = posList[0];
    var node = this.root.$$('#table-browser-tree > ul > li').get(pos);
    for (var i = 1; i < posList.length; ++i) {
      pos = posList[i];
      node = node.$$(node.locator().value + ' > ul > li').get(pos);
    }
    return node;
  },

  expectNode: function(posList, expectedName, expectedDragText) {
    var li = this.getNode(posList);
    expect(li.getText()).toBe(expectedName);
    if (expectedDragText) {
      this.expectDragText(li, expectedDragText);
    }
  },

  toggleNode: function(posList) {
    var li = this.getNode(posList);
    li.$(li.locator().value + ' > span').click();
  },

  getColumn: function(tablePos, columnPos) {
    var tableLi = this.getTable(tablePos);
    return tableLi.$$('ul > li').get(columnPos + 1);
  },

  expectColumn: function(tablePos, columnPos, name) {
    var columnLi = this.getColumn(tablePos, columnPos);
    expect(columnLi.getText()).toBe(name);
  },

  searchTable: function(searchText) {
    var searchBox = this.root.$('#search-for-tables');
    searchBox.sendKeys(searchText);
  },

  expectDragText: function(li, expected) {
    // We cannot do a real drag-and-drop workflow here
    // because of:
    // https://github.com/angular/protractor/issues/583
    // Just doing a simple check for now.
    var span = li.$(li.locator().value +
        ' > span > table-browser-entry > span');
    expect(span.evaluate('draggableText')).toBe(expected);
  },

  toggleFullyQualify: function() {
    this.root.$('#use-fully-qualified-names').click();
  },

  enterSearchQuery: function(query) {
    element(by.id('table-browser-search-box'))
        .sendKeys(testLib.selectAllKey + query);
  },

};

function History(side) {
  this.side = side;
}

History.prototype = {
  open: function() {
    this.side.side.$('.history-button').click();
  },

  close: function(discardChanges) {
    if (discardChanges) {
      testLib.expectDialogAndRespond(true);
    }
    this.side.side.element(by.id('close-history-button')).click();
    if (discardChanges) {
      testLib.checkAndCleanupDialogExpectation();
    }
  },

  save: function(name) {
    this.side.side.$('.save-history-button').click();
    if (name !== undefined) {
      var inputBox = this.side.side.$('.save-as-history-box input');
      inputBox.sendKeys(testLib.selectAllKey + name);
    }
    this.side.side.$('.save-as-history-box .glyphicon-floppy-disk').click();
  },

  expectSaveable: function(saveable) {
    expect(this.side.side.$('.save-history-button').isPresent()).toBe(saveable);
  },

  // Get an operation from the history. position is a zero-based index.
  getOperation: function(position) {
    var list = this.side.side.
      $$('project-history div.list-group > li.history-operation-item');
    return list.get(position);
  },

  // Beware, the category is left open, so calling this the second time for the same category
  // does not work.
  getOperationInCategoryByName: function(operation, tooltip, name) {
    operation.$('operation-toolbox').$('div[drop-tooltip="' + tooltip + '"]').click();
    var ops = operation.$('operation-toolbox').$$('div.operation');
    return ops.filter(function(element) {
        return element.getText().then(function(text) { return text === name; });
      }).get(0);
  },

  getInsertMenu: function(position) {
    var list = this.side.side.
      $$('project-history div.list-group > li > project-history-adder');
    return list.get(position);
  },

  getOperationName: function(position) {
    return this.getOperation(position).$('h1').getText();
  },

  getOperationSegmentation: function(position) {
    return this.getOperation(position).$('div.affected-segmentation').getText();
  },

  openDropdownMenu: function(operation) {
    var menu = operation.$('.history-options');
    menu.$('a.dropdown-toggle').click();
    return menu;
  },

  clickDropDownMenuItem: function(position, itemId) {
    var operation = this.getInsertMenu(position);
    this.openDropdownMenu(operation).$('a#dropdown-menu-' + itemId).click();
  },

  selectOperation: function(op, name) {
    op.element(by.id('operation-search')).click();
    op.element(by.id('filter')).sendKeys(name, K.ENTER);
  },

  deleteOperation: function(position) {
    this.getOperation(position).$('#operation-discard').click();
  },

  enterEditMode: function(op) {
    op.$('#operation-edit').click();
  },

  discardEdits: function(op) {
    op.$('#operation-discard-changes').click();
  },

  initInsertedOperation: function(newPos, name, params) {
    var newOp = this.getOperation(newPos);
    this.selectOperation(newOp, name);
    this.side.populateOperation(newOp, params);
    this.side.submitOperation(newOp);
  },

  insertOperationSimple: function(pos, name, params) {
    this.getInsertMenu(pos).click();
    this.initInsertedOperation(pos, name, params);
  },

  insertOperationForSegmentation: function(pos, name, params, segmentation) {
    var menuItemId = 'add';
    if (segmentation) {
      menuItemId += '-for-' + segmentation;
    }
    this.clickDropDownMenuItem(pos, menuItemId);
    this.initInsertedOperation(pos, name, params);
  },

  numOperations: function() {
    return this.side.side.
      $$('project-history div.list-group > li.history-operation-item').
      count();
  },

  expectOperationParameter: function(opPosition, paramName, expectedValue) {
    var param = this.getOperation(opPosition).$('div#' + paramName + ' input');
    expect(param.getAttribute('value')).toBe(expectedValue);
  },

  expectOperationSelectParameter: function(opPosition, paramName, expectedValue) {
    var param = this.getOperation(opPosition).$('div#' + paramName + ' select');
    expect(param.getAttribute('value')).toBe(expectedValue);
  }
};

var visualization = {
  svg: $('svg.graph-view'),

  elementByLabel: function(label) {
    return this.svg.element(by.xpath('.//*[contains(text(),"' + label + '")]/..'));
  },

  clickMenu: function(item) {
    $('.context-menu #menu-' + item).click();
  },

  asTSV: function() {
    var copyButton = $('.graph-sidebar [data-clipboard-text');
    // It would be too complicated to test actual copy & paste. We just trust Clipboard.js instead.
    return copyButton.getAttribute('data-clipboard-text');
  },

  // The visualization response received from the server.
  graphView: function() {
    return visualization.svg.evaluate('graph.view');
  },

  // The currently visualized graph data extracted from the SVG DOM.
  graphData: function() {
    browser.waitForAngular();
    return browser.executeScript(function() {

      // Vertices as simple objects.
      function vertexData(svg) {
        var vertices = svg.querySelectorAll('g.vertex');
        var result = [];
        for (var i = 0; i < vertices.length; ++i) {
          var v = vertices[i];
          var touch = v.querySelector('circle.touch');
          var x = touch.getAttribute('cx');
          var y = touch.getAttribute('cy');
          var icon = v.querySelector('path.icon');
          var label = v.querySelector('text');
          var image = v.querySelector('image');
          result.push({
            pos: { x: parseFloat(x), y: parseFloat(y), string: x + ' ' + y },
            label: label.innerHTML,
            icon: image ? null : icon.id,
            color: image ? null : icon.style.fill,
            size: touch.getAttribute('r'),
            opacity: v.getAttribute('opacity'),
            labelSize: label.getAttribute('font-size').slice(0, -2), // Drop "px".
            labelColor: label.style.fill,
            image: image ? image.getAttribute('href') : null,
          });
        }
        result.sort();
        return result;
      }

      // Edges as simple objects.
      function edgeData(svg, vertices) {
        // Build an index by position, so edges can be resolved to vertices.
        var i, byPosition = {};
        for (i = 0; i < vertices.length; ++i) {
          byPosition[vertices[i].pos.string] = i;
        }

        // Collect edges.
        var result = [];
        var edges = svg.querySelectorAll('g.edge');
        function arcStart(d) {
          return d.match(/M (.*? .*?) /)[1];
        }
        for (i = 0; i < edges.length; ++i) {
          var e = edges[i];
          var first = e.querySelector('path.first');
          var second = e.querySelector('path.second');
          var srcPos = arcStart(first.getAttribute('d'));
          var dstPos = arcStart(second.getAttribute('d'));
          result.push({
            src: byPosition[srcPos],
            dst: byPosition[dstPos],
            label: e.querySelector('text').innerHTML,
            color: first.style.stroke,
            width: first.getAttribute('stroke-width'),
          });
        }
        result.sort(function(a, b) {
          return a.src * vertices.length + a.dst - b.src * vertices.length - b.dst;
        });
        return result;
      }

      var svg = document.querySelector('svg.graph-view');
      var vertices = vertexData(svg);
      var edges = edgeData(svg, vertices);
      return { vertices: vertices, edges: edges };
    });
  },

  vertexCounts: function(index) {
    return visualization.graphView().then(function(gv) {
      return gv.vertexSets[index].vertices.length;
    });
  },
};

function Selector(root) {
  this.root = root;
  this.tableBrowser = new TableBrowser(this.root);
}

Selector.prototype = {
  project: function(name) {
    return element(by.id('project-' + toID(name)));
  },

  directory: function(name) {
    return element(by.id('directory-' + toID(name)));
  },

  table: function(name) {
    return element(by.id('table-' + toID(name)));
  },

  view: function(name) {
    return element(by.id('view-' + toID(name)));
  },

  expectNumProjects: function(n) {
    return expect($$('.project-entry').count()).toEqual(n);
  },

  expectNumDirectories: function(n) {
    return expect($$('.directory-entry').count()).toEqual(n);
  },

  expectNumTables: function(n) {
    return expect($$('.table-entry').count()).toEqual(n);
  },

  expectNumViews: function(n) {
    return expect($$('.view-entry').count()).toEqual(n);
  },

  computeTable: function(name) {
    this.table(name).element(by.css('.value-retry')).click();
  },

  // Verifies that a computed table exists by the name 'name' and contains 'n' rows.
  expectTableWithNumRows: function(name, n) {
    var table = this.table(name);
    // Look up the number of rows shown inside a <value>
    // element.
    return expect(table.$('value').getText()).toEqual(n.toString());
  },

  openNewProject: function(name) {
    element(by.id('new-project')).click();
    element(by.id('new-project-name')).sendKeys(name);
    $('#new-project button[type=submit]').click();
    this.hideFloatingElements();
  },

  startTableImport: function() {
    element(by.id('import-table')).click();
  },

  clickAndWaitForCsvImport: function() {
    var importCsvButton = element(by.id('import-csv-button'));
    // Wait for the upload to finish.
    testLib.wait(protractor.ExpectedConditions.elementToBeClickable(importCsvButton));
    importCsvButton.click();
  },

  importLocalCSVFile: function(tableName, localCsvFile, csvColumns, columnsToImport, view, limit) {
    this.root.$('import-wizard #table-name input').sendKeys(tableName);
    if (columnsToImport) {
      this.root.$('import-wizard #columns-to-import input').sendKeys(columnsToImport);
    }
    this.root.$('#datatype select option[value="csv"]').click();
    if (csvColumns) {
      this.root.$('import-wizard #csv-column-names input').sendKeys(csvColumns);
    }
    var csvFileParameter = $('#csv-filename file-parameter');
    testLib.uploadIntoFileParameter(csvFileParameter, localCsvFile);
    if (view) {
      this.root.$('import-wizard #as-view input').click();
    }
    if (limit) {
      this.root.$('import-wizard #limit input').sendKeys(limit.toString());
    }
    this.clickAndWaitForCsvImport();
  },

  importJDBC: function(tableName, jdbcUrl, jdbcTable, jdbcKeyColumn, view) {
    this.root.$('import-wizard #table-name input').sendKeys(tableName);
    this.root.$('#datatype select option[value="jdbc"]').click();
    this.root.$('#jdbc-url input').sendKeys(jdbcUrl);
    this.root.$('#jdbc-table input').sendKeys(jdbcTable);
    this.root.$('#jdbc-key-column input').sendKeys(jdbcKeyColumn);
    if (view) {
      this.root.$('import-wizard #as-view input').click();
    }
    this.root.$('#import-jdbc-button').click();
  },

  newDirectory: function(name) {
    element(by.id('new-directory')).click();
    element(by.id('new-directory-name')).sendKeys(name);
    $('#new-directory button[type=submit]').click();
  },

  openProject: function(name) {
    this.project(name).click();
    this.hideFloatingElements();
  },

  hideFloatingElements: function() {
    // Floating elements can overlap buttons and block clicks.
    browser.executeScript(`
      document.styleSheets[0].insertRule(
        '.spark-status, .bottom-links { position: static !important; }');
        `);
      },

  openDirectory: function(name) {
    this.directory(name).click();
  },

  popDirectory: function() {
    element(by.id('pop-directory-icon')).click();
  },

  renameProject: function(name, newName) {
    var project = this.project(name);
    testLib.menuClick(project, 'rename');
    project.element(by.id('renameBox')).sendKeys(testLib.selectAllKey, newName).submit();
  },

  deleteProject: function(name) {
    testLib.menuClick(this.project(name), 'discard');
  },

  deleteDirectory: function(name) {
    testLib.menuClick(this.directory(name), 'discard');
  },

  editTable: function(name) {
    testLib.menuClick(this.table(name), 'edit-import');
  },

  editView: function(name) {
    testLib.menuClick(this.view(name), 'edit-import');
  },

  expectProjectListed: function(name) {
    testLib.expectElement(this.project(name));
  },

  expectProjectNotListed: function(name) {
    testLib.expectNotElement(this.project(name));
  },

  expectDirectoryListed: function(name) {
    testLib.expectElement(this.directory(name));
  },

  expectDirectoryNotListed: function(name) {
    testLib.expectNotElement(this.directory(name));
  },

  expectTableListed: function(name) {
    testLib.expectElement(this.table(name));
  },

  expectTableNotListed: function(name) {
    testLib.expectNotElement(this.table(name));
  },

  expectViewListed: function(name) {
    testLib.expectElement(this.view(name));
  },

  enterSearchQuery: function(query) {
    element(by.id('project-search-box')).sendKeys(testLib.selectAllKey + query);
  },

  clearSearchQuery: function() {
    element(by.id('project-search-box')).sendKeys(testLib.selectAllKey + K.BACK_SPACE);
  },

  globalSqlEditor: function() {
    return element(by.id('sql-editor'));
  },
  setGlobalSql: function(sql) {
    testLib.sendKeysToACE(this.globalSqlEditor(), sql);
  },

  openGlobalSqlBox: function() {
    element(by.id('global-sql-box')).click();
  },

  runGlobalSql: function(sql) {
    this.openGlobalSqlBox();
    this.setGlobalSql(sql);
    element(by.id('run-sql-button')).click();
  },


  expectGlobalSqlResult: function(names, types, rows) {
    var res = element(by.id('sql-result'));
    expect(res.$$('thead tr th span.sql-column-name').map(e => e.getText())).toEqual(names);
    expect(res.$$('thead tr th span.sql-type').map(e => e.getText())).toEqual(types);
    expect(res.$$('tbody tr').map(e => e.$$('td').map(e => e.getText()))).toEqual(rows);
  },

  saveGlobalSqlToCSV: function() {
    element(by.id('save-results-opener')).click();
    this.root.$('#exportFormat option[value="csv"]').click();
    element(by.id('save-results')).click();
  },

  saveGlobalSqlToTable: function(name) {
    element(by.id('save-results-opener')).click();
    this.root.$('#exportFormat option[value="table"]').click();
    this.root.$('#exportKiteTable').sendKeys(name);
    element(by.id('save-results')).click();
  },

  saveGlobalSqlToView: function(name) {
    element(by.id('save-results-opener')).click();
    this.root.$('#exportFormat option[value="view"]').click();
    this.root.$('#exportKiteTable').sendKeys(name);
    element(by.id('save-results')).click();
  },
};

var splash = new Selector(element(by.id('splash')));

function randomPattern () {
  /* jshint bitwise: false */
  var crypto = require('crypto');
  var buf = crypto.randomBytes(16);
  var sixteenLetters = 'abcdefghijklmnop';
  var r = '';
  for (var i = 0; i < buf.length; i++) {
    var v = buf[i];
    var lo =  (v & 0xf);
    var hi = (v >> 4);
    r += sixteenLetters[lo] + sixteenLetters[hi];
  }
  return r;
}

var lastDownloadList;

testLib = {
  theRandomPattern: randomPattern(),
  left: new Side('left'),
  right: new Side('right'),
  visualization: visualization,
  splash: splash,
  selectAllKey: K.chord(K.CONTROL, 'a'),
  protractorDownloads: '/tmp/protractorDownloads.' + process.pid,

  expectElement: function(e) {
    expect(e.isDisplayed()).toBe(true);
  },

  expectNotElement: function(e) {
    expect(e.isPresent()).toBe(false);
  },

  // Deletes all projects and directories.
  discardAll: function() {
    function discard(defer) {
      var req = request.defaults({ jar: true });
      req.post(
        browser.baseUrl + 'ajax/discardAllReallyIMeanIt',
        { json: { fake: 1 } },
        (error, message) => {
          if (error || message.statusCode >= 400) {
            defer.reject(new Error(error));
          } else {
            defer.fulfill();
      }});
    }
    this.authenticateAndPost('admin', 'adminpw', 'lynxkite', discard);
  },

  authenticateAndPost: function(username, password, method, func) {
    function sendRequest() {
      var defer = protractor.promise.defer();
      if (!process.env.HTTPS_PORT) {
        return func(defer);
      }
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
      var req = request.defaults({ jar: true });
      req.post(
        browser.baseUrl + 'passwordLogin',
        { json : {
          'username': username,
          'password': password,
          'method': method
        }}, (error, message) => {
          if (error || message.statusCode >= 400) {
            defer.reject(new Error(error));  // TODO: include message?
          } else {
            func(defer);
          }});
      return defer.promise;
    }
    browser.controlFlow().execute(sendRequest);
  },

  navigateToProject: function(name) {
    browser.get('/#/project/' + name);
  },

  helpPopup: function(helpId) {
    return $('div[help-id="' + helpId + '"]');
  },

  getACEText: function(e) {
    // getText() drops text in hidden elements. "innerText" to the rescue!
    // https://github.com/angular/protractor/issues/1794
    return e.$('.ace_content').getAttribute('innerText').then(text => text.trim());
  },

  sendKeysToACE: function(e, keys) {
    var aceContent = e.$('div.ace_content');
    var aceInput = e.$('textarea.ace_text-input');
    // The double click on the text area focuses it properly.
    browser.actions().doubleClick(aceContent).perform();
    aceInput.sendKeys(testLib.selectAllKey + keys);
  },

  setParameter: function(e, value) {
    // Special parameter types need different handling.
    e.evaluate('param.kind').then(
        function(kind) {
          if (kind === 'code') {
            testLib.sendKeysToACE(e, testLib.selectAllKey + value);
          } else if (kind === 'file') {
            testLib.uploadIntoFileParameter(e, value);
          } else if (kind === 'tag-list') {
            var values = value.split(',');
            for (var i = 0; i < values.length; ++i) {
              e.$('.dropdown-toggle').click();
              e.$('.dropdown-menu #' + values[i]).click();
            }
          } else if (kind === 'table') {
            // You can specify a CSV file to be uploaded, or the name of an existing table.
            if (value.indexOf('.csv') !== -1) { // CSV file.
              e.element(by.id('import-new-table-button')).click();
              var s = new Selector(e.element(by.id('import-wizard')));
              s.importLocalCSVFile('test-table', value);
            } else { // Table name.
              // Table name options look like 'name of table (date of table creation)'.
              // The date is unpredictable, but we are going to match to the ' (' part
              // to minimize the chance of mathcing an other table.
              var optionLabelPattern = value + ' (';
              e.element(by.cssContainingText('option', optionLabelPattern)).click();
            }
          } else if (kind === 'choice') {
            e.element(by.cssContainingText('option', value)).click();
          } else {
            e.sendKeys(testLib.selectAllKey + value);
          }
        });
  },

  // Expects a window.confirm call from the client code and overrides the user
  // response.
  expectDialogAndRespond: function(responseValue) {
    // I am not particularly happy with this solution. The problem with the nice
    // solution is that there is a short delay before the alert actually shows up
    // and protractor does not wait for it. (Error: NoSuchAlertError: no alert open)
    // See: https://github.com/angular/protractor/issues/1486
    // Other possible options:
    // 1. browser.wait for the alert to appear. This introduces a hard timout
    // and potential flakiness.
    // 2. Use Jasmine's spyOn. The difficulty there is in getting hold of a
    // window object from inside the browser, if at all ppossible.
    // 3. Use a mockable Angular module for window.confirm from our app.
    browser.executeScript(
        'window.confirm0 = window.confirm;' +
        'window.confirm = function() {' +
        '  window.confirm = window.confirm0;' +
        '  return ' + responseValue+ ';' +
        '}');
  },

  checkAndCleanupDialogExpectation: function() {
    // Fail if there was no alert.
    expect(browser.executeScript('return window.confirm === window.confirm0')).toBe(true);
    browser.executeScript('window.confirm = window.confirm0;');
  },

  // Warning, this also sorts the given array parameter in place.
  sortHistogramValues: function(values) {
    return values.sort(function(b1, b2) {
      if (b1.title < b2.title) {
        return -1;
      } else if (b1.title > b2.title) {
        return 1;
      } else {
        return 0;
      }
    });
  },

  // A promise of the list of error messages.
  errors: function() {
    return $$('.top-alert-message').map(function(e) { return e.getText(); });
  },

  // Expects that there will be a single error message and returns it as a promise.
  error: function() {
    return testLib.errors().then(function(errors) {
      expect(errors.length).toBe(1);
      return errors[0];
    });
  },

  closeErrors: function() {
    $$('.top-alert').each(function(e) {
      e.element(by.id('close-alert-button')).click();
    });
  },

  // Wait indefinitely.
  // WebDriver 2.45 changed browser.wait() to default to a 0 timeout. This was reverted in 2.46.
  // But the current Protractor version uses 2.45, so we have this wrapper.
  wait: function(condition) {
    return browser.wait(condition, 99999999);
  },

  expectModal: function(title) {
    var t = $('.modal-title');
    testLib.expectElement(t);
    expect(t.getText()).toEqual(title);
  },

  closeModal: function() {
    element(by.id('close-modal-button')).click();
  },

  setEnablePopups: function(enable) {
    browser.executeScript(
      'angular.element(document.body).injector()' +
      '.get("dropTooltipConfig").enabled = ' + enable);

  },

  uploadIntoFileParameter: function(fileParameterElement, fileName) {
    var input = fileParameterElement.element(by.id('file'));
    // Need to unhide flowjs's secret file uploader.
    browser.executeScript(
      function(input) {
        input.style.visibility = 'visible';
        input.style.height = '1px';
        input.style.width = '1px';
        input.style.opacity = 1;
      },
      input.getWebElement());
    input.sendKeys(fileName);
  },

  startDownloadWatch: function() {
    browser.controlFlow().execute(function() {
      expect(lastDownloadList).toBe(undefined);
      lastDownloadList = fs.readdirSync(testLib.protractorDownloads);
    });
  },

  // Waits for a new downloaded file matching regex and returns its name.
  // Pattern match is needed as chrome first creates some weird temp file.
  waitForNewDownload: function(regex) {
    return testLib.wait(function() {
      var newList = fs.readdirSync(testLib.protractorDownloads).filter(function(fn) {
        return fn.match(regex);
      });
      // this will be undefined if no new element was found.
      var result = newList.filter(function(f) { return lastDownloadList.indexOf(f) < 0; })[0];
      if (result) {
        lastDownloadList = undefined;
        return testLib.protractorDownloads + '/' + result;
      } else {
        return false;
      }
    });
  },

  expectFileContents: function(filename, expectedContents) {
    filename.then(function(fn) {
      expect(fs.readFileSync(fn, 'utf8')).toBe(expectedContents);
    });
  },

  expectHasClass(element, cls) {
    expect(element.getAttribute('class')).toBeDefined();
    element.getAttribute('class').then(function(classes) {
      expect(classes.split(' ').indexOf(cls)).not.toBe(-1);
    });
  },

  expectNoClass(element, cls) {
    expect(element.getAttribute('class')).toBeDefined();
    element.getAttribute('class').then(function(classes) {
          expect(classes.split(' ').indexOf(cls)).toBe(-1);
    });
  },

  menuClick: function(entry, action) {
    var menu = entry.$('.dropdown');
    menu.$('a.dropdown-toggle').click();
    menu.element(by.id('menu-' + action)).click();
  },

  switchToWindow: function(pos) {
    browser.getAllWindowHandles()
      .then(handles => {
        browser.driver.switchTo().window(handles[pos]);
    });
  },

  showSelector: function() {
    $('#show-selector-button').click();
  },

  confirmSweetAlert: function(expectedMessage) {
    // SweetAlert is not an Angular library. We need to wait until it pops in and out.
    var EC = protractor.ExpectedConditions;
    testLib.wait(EC.visibilityOf($('.sweet-alert.showSweetAlert.visible')));
    expect($('.sweet-alert h2').getText()).toBe(expectedMessage);
    $('.sweet-alert button.confirm').click();
    testLib.wait(EC.stalenessOf($('.sweet-alert.showSweetAlert')));
  },
};

module.exports = testLib;
