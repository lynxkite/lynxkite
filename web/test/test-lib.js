'use strict';


const request = require('request');
const fs = require('fs');

// Forward declarations.
let testLib;

const K = protractor.Key; // Short alias.


// Mirrors the "id" filter.
function toId(x) {
  return x.toLowerCase().replace(/ /g, '-');
}

// Same as element.getText() except work on offscreen elements.
function textOf(element) {
  return element.getAttribute('textContent').then(s => s.trim());
}

// Workaround for flaky typing in frontend tests
function safeSendKeys(input, text) {
  let result;
  text.split('').forEach((c) => result = input.sendKeys(c));
  return result;
}

function safeSelectAndSendKeys(input, text) {
  input.sendKeys(testLib.selectAllKey);
  return safeSendKeys(input, text);
}

function Entity(side, kind, name) {
  this.side = side;
  this.kind = kind;
  this.name = name;
  this.kindName = kind + '-' + name;
  this.element = this.side.$('#' + this.kindName);
  this.menu = $('#menu-' + this.kindName);
}

function isMacOS() {
  // Mac is 'darwin': https://nodejs.org/api/process.html#process_process_platform
  return process.platform === 'darwin';
}
const CTRL = isMacOS() ? K.META : K.CONTROL;

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
      if (present) { this.element.evaluate('closeMenu()'); }
    });
  },

  setFilter: function(filterValue) {
    const filterBox = this.popup().$('#filter');
    filterBox.clear();
    safeSendKeys(filterBox, filterValue).submit();
    this.popoff();
  },

  getHistogramValues: function(precise) {
    precise = precise || false;
    const popup = this.popup();
    const histogram = popup.$('histogram');
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
      const toolTip = td.getAttribute('drop-tooltip');
      const style = td.$('.bar').getAttribute('style');
      return protractor.promise.all([toolTip, style]).then(function(results) {
        const toolTipMatch = results[0].match(/^(.*): (\d+)$/);
        const styleMatch = results[1].match(/^height: (\d+)%;$/);
        return {
          title: toolTipMatch[1],
          size: parseInt(styleMatch[1]),
          value: parseInt(toolTipMatch[2]),
        };
      });
    }
    const tds = histogram.$$('.bar-container');
    const res = tds.then(function(tds) {
      const res = [];
      for (let i = 0; i < tds.length; i++) {
        res.push(allFrom(tds[i]));
      }
      return protractor.promise.all(res);
    });

    const total = histogram.$('#histogram-total');
    protractor.promise.all([total.getText(), res]).then(function(results) {
      const totalValue = results[0].match(/histogram total: ([0-9,]+)/)[1];
      const values = results[1];
      const total = parseInt(totalValue.replace(/,/g, ''));
      let sum = 0;
      for (let j = 0; j < values.length; j++) {
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

  clickMenu: function(id) {
    this.popup().$('#' + id).click();
    this.popoff();
  },
};

function Workspace() {
  this.main = element(by.id('workspace-entry-point'));
  this.selector = element(by.css('.operation-selector'));
  this.board = element(by.css('#workspace-drawing-board'));
}

Workspace.prototype = {
  expectCurrentWorkspaceIs: function(name) {
    expect(this.main.element(by.id('workspace-name')).getText()).toBe(name);
    // TODO: check that workspace is error-free
  },

  close: function() {
    this.main.element(by.id('close-workspace')).click();
  },

  openOperation: function(name) {
    this.selector.element(by.id('operation-search')).click();
    safeSendKeys(this.selector.element(by.id('filter')), name + K.ENTER);
    return this.selector.$$('operation-selector-entry').get(0);
  },

  closeOperationSelector: function() {
    this.selector.element(by.id('operation-search')).click();
  },

  closeLastPopup: function() {
    browser.actions().sendKeys(K.ESCAPE).perform();
  },

  duplicate: function() {
    browser.actions()
      .sendKeys(K.chord(CTRL, 'c'))
      .sendKeys(K.chord(CTRL, 'v'))
      .perform();
  },

  addBoxFromSelector: function(boxName) {
    browser.actions()
      .sendKeys('/' + boxName + K.ENTER + K.ESCAPE)
      .perform();
  },

  addBox: function(boxData) {
    const id = boxData.id;
    const after = boxData.after;
    const inputs = boxData.inputs;
    const params = boxData.params;
    browser.waitForAngular();
    browser.executeScript(`
        $(document.querySelector('#workspace-drawing-board')).scope().workspace.addBox(
          '${ boxData.name }',
          { logicalX: ${ boxData.x }, logicalY: ${ boxData.y } },
          { boxId: '${ boxData.id }' });
        `);
    if (after) {
      this.connectBoxes(after, 'graph', id, 'graph');
    }
    if (inputs) {
      for (let i = 0; i < inputs.length; ++i) {
        const input = inputs[i];
        this.connectBoxes(input.boxId, input.srcPlugId, id, input.dstPlugId);
      }
    }
    if (params) {
      this.editBox(id, params);
    }
  },

  selectBoxes: function(boxIds) {
    browser.actions().keyDown(CTRL).perform();
    // Unselect all.
    $$('g.box.selected').click();
    // Select given boxes.
    for (let i = 0; i < boxIds.length; ++i) {
      this.clickBox(boxIds[i]);
    }
    browser.actions().keyUp(CTRL).perform();
  },

  // Protractor mouseMove only takes offsets, so first we set the mouse position to a box based on
  // its id, and then move it to 2 other points on the screen.
  selectArea: function(startBoxId, point1, point2) {
    let box = this.getBox(startBoxId);
    browser.actions()
      .mouseMove(box, point1)
      .keyDown(K.SHIFT)
      .mouseDown()
      .mouseMove(point2)
      .mouseUp()
      .keyUp(K.SHIFT)
      .perform();
  },

  expectNumSelectedBoxes: function(n) {
    return expect($$('g.box.selected').count()).toEqual(n);
  },

  expectNumBoxes: function(n) {
    return expect($$('g.box').count()).toEqual(n);
  },

  deleteBoxes: function(boxIds) {
    this.selectBoxes(boxIds);
    this.main.$('#delete-selected-boxes').click();
  },

  editBox: function(boxId, params) {
    const boxEditor = this.openBoxEditor(boxId);
    boxEditor.populateOperation(params);
    boxEditor.close();
  },

  addWorkspaceParameter: function(name, kind, defaultValue) {
    const boxEditor = this.openBoxEditor('anchor');
    boxEditor.element.$('#add-parameter').click();
    const keys = name.split('');
    let prefix = '';
    for (let k of keys) {
      boxEditor.element.$('#' + prefix + '-id').sendKeys(k);
      prefix += k;
    }
    safeSendKeys(boxEditor.element.$('#' + name + '-type'), kind);
    safeSendKeys(boxEditor.element.$('#' + name + '-default'), defaultValue);
    boxEditor.close();
  },

  boxExists(boxId) {
    return this.board.$('.box#' + boxId).isPresent();
  },

  boxPopupExists(boxId) {
    return this.board.$('.popup#' + boxId).isPresent();
  },

  getBox(boxId) {
    return this.board.$('.box#' + boxId);
  },

  getInputPlug: function(boxId, plugId) {
    let box = this.getBox(boxId);
    if (plugId) {
      return box.$('#inputs #' + plugId + ' circle');
    } else {
      return box.$$('#inputs circle').get(0);
    }
  },

  getOutputPlug: function(boxId, plugId) {
    let box = this.getBox(boxId);
    if (plugId) {
      return box.$('#outputs #' + plugId + ' circle');
    } else {
      return box.$$('#outputs circle').get(0);
    }
  },

  toggleStateView: function(boxId, plugId) {
    this.getOutputPlug(boxId, plugId).click();
  },

  clickBox: function(boxId) {
    this.getBox(boxId).$('#click-target').click();
  },

  selectBox: function(boxId) {
    this.openBoxEditor(boxId).close();
  },

  getBoxEditor: function(boxId) {
    const popup = this.board.$('.popup#' + boxId);
    return new BoxEditor(popup);
  },

  openBoxEditor: function(boxId) {
    this.clickBox(boxId);
    const editor = this.getBoxEditor(boxId);
    testLib.expectElement(editor.popup);
    return editor;
  },

  openStateView: function(boxId, plugId) {
    const popup = this.board.$('.popup#' + boxId + '_' + plugId);
    testLib.expectNotElement(popup); // If it is already open, use getStateView() instead.
    this.toggleStateView(boxId, plugId);
    return new State(popup);
  },

  getStateView: function(boxId, plugId) {
    const popup = this.board.$('.popup#' + boxId + '_' + plugId);
    return new State(popup);
  },

  getVisualizationEditor(boxId) {
    const editor = this.getBoxEditor(boxId);
    return new State(editor.popup);
  },

  expectConnected: function(srcBoxId, srcPlugId, dstBoxId, dstPlugId) {
    const arrow = this.board.$(`path#${srcBoxId}-${srcPlugId}-${dstBoxId}-${dstPlugId}`);
    expect(arrow.isPresent()).toBe(true);
  },

  connectBoxes: function(srcBoxId, srcPlugId, dstBoxId, dstPlugId) {
    const src = this.getOutputPlug(srcBoxId, srcPlugId);
    const dst = this.getInputPlug(dstBoxId, dstPlugId);
    expect(src.isDisplayed()).toBe(true);
    expect(dst.isDisplayed()).toBe(true);
    browser.actions()
      .mouseDown(src)
      .mouseMove(dst)
      .mouseUp()
      .perform();
    this.expectConnected(srcBoxId, srcPlugId, dstBoxId, dstPlugId);
  },

  getCustomBoxBrowserTree: function() {
    this.selector.element(by.css('div[drop-tooltip="Custom boxes"]')).click();
    return this.selector
      .element(by.css('operation-tree'))
      .element(by.css('operation-tree-node[id="root"]'));
  },

  saveWorkspaceAs: function(newName) {
    this.main.$('#save-workspace-as-starter-button').click();
    safeSelectAndSendKeys(this.main.$('#save-workspace-as-input input'), newName);
    this.main.$('#save-workspace-as-input #ok').click();
  },

};

function PopupBase() {
}

PopupBase.prototype = {
  close: function() {
    this.popup.$('#close-popup').click();
  },

  moveTo: function(x, y) {
    const head = this.popup.$('div.popup-head');
    browser.actions()
      .mouseDown(head)
    // Absolute positioning of mouse. If we don't specify the first
    // argument then this becomes a relative move. If the first argument
    // is this.board, then protractor scrolls the element of this.board
    // to the top of the page, even though scrolling is not enabled.
      .mouseMove($('body'), {x: x, y: y})
      .mouseUp(head)
      .perform();
    return this;
  },
};

function BoxEditor(popup) {
  this.popup = popup;
  this.element = popup.$('box-editor');
}

BoxEditor.prototype = {
  __proto__: PopupBase.prototype, // inherit PopupBase's methods

  operationId: function() {
    return this.popup.$('.popup-head').getText();
  },

  operationParameter: function(param) {
    return this.element.$(
      'operation-parameters #param-' + param + ' .operation-attribute-entry');
  },

  parametricSwitch: function(param) {
    return this.element.$('operation-parameters #param-' + param + ' .parametric-switch');
  },

  removeParameter: function(param) {
    return this.element.$('operation-parameters #param-' + param + ' .remove-parameter').click();
  },

  openGroup: function(group) {
    this.element.element(by.xpath(`//a[contains(.,"${group}")]`)).click();
    return this;
  },

  populateOperation: function(params) {
    params = params || {};
    for (const key in params) {
      testLib.setParameter(this.operationParameter(key), params[key]);
    }
    $('#workspace-name').click(); // Make sure the parameters are not focused.
  },

  expectParameter: function(paramName, expectedValue) {
    const param = this.element.$('div#param-' + paramName + ' input');
    expect(param.getAttribute('value')).toBe(expectedValue);
  },

  expectSelectParameter: function(paramName, expectedValue) {
    const param = this.element.$('div#param-' + paramName + ' select');
    expect(param.getAttribute('value')).toBe(expectedValue);
  },

  expectCodeParameter: function(paramName, expectedValue) {
    const param = this.element.$('div#param-' + paramName);
    expect(testLib.getACEText(param)).toBe(expectedValue);
  },

  getTableBrowser: function() {
    return new TableBrowser(this.popup);
  },

  isPresent: function() {
    return this.element.isPresent();
  },
};

function State(popup) {
  this.popup = popup;
  this.left = new Side(this.popup, 'left');
  this.right = new Side(this.popup, 'right');
  this.table = new TableState(this.popup);
  this.plot = new PlotState(this.popup);
  this.visualization = new VisualizationState(this.popup);
}

State.prototype = {
  __proto__: PopupBase.prototype, // inherit PopupBase's methods

  setInstrument: function(index, name, params) {
    const state = this.popup.$(`#state-${index}`);
    state.$(`#instrument-with-${name}`).click();
    params = params || {};
    for (const key in params) {
      const param = state.$(`operation-parameters #param-${key} .operation-attribute-entry`);
      testLib.setParameter(param, params[key]);
    }
    $('#workspace-name').click(); // Make sure the parameters are not focused.
  },

  clearInstrument: function(index) {
    this.popup.$(`#state-${index} #clear-instrument`).click();
  },
};

function PlotState(popup) {
  this.popup = popup;
  this.canvas = popup.$('#plot-div .vega svg');
}

PlotState.prototype = {
  __proto__: PopupBase.prototype, // inherit PopupBase's methods

  barHeights: function() {
    return this.canvas.$$('g.mark-rect.marks rect').map(e => e.getAttribute('height'));
  },

  expectBarHeightsToBe: function(expected) {
    // The heights from local runs and Jenkins do not match. Allow 1% flexibility.
    this.barHeights().then(heights => {
      expect(heights.length).toEqual(expected.length);
      for (let i = 0; i < heights.length; ++i) {
        expect(heights[i]).toBeGreaterThanOrEqual(0.99 * heights[i]);
        expect(heights[i]).toBeLessThanOrEqual(1.01 * heights[i]);
      }
    });
  }
};


function TableState(popup) {
  this.popup = popup;
  this.sample = popup.$('#table-sample');
  this.control = popup.$('#table-control');
}

TableState.prototype = {
  __proto__: PopupBase.prototype, // inherit PopupBase's methods

  expect: function(names, types, rows) {
    this.expectColumnNamesAre(names);
    this.expectColumnTypesAre(types);
    this.expectRowsAre(rows);
  },

  rowCount: function() {
    return this.sample.$$('tbody tr').count();
  },

  expectRowCountIs: function(number) {
    expect(this.rowCount()).toBe(number);
  },

  columnNames: function() {
    return this.sample.$$('thead tr th span.column-name').map(e => e.getText());
  },

  expectColumnNamesAre(columnNames) {
    expect(this.columnNames()).toEqual(columnNames);
  },

  columnTypes: function() {
    return this.sample.$$('thead tr th span.column-type').map(e => e.getText());
  },

  expectColumnTypesAre(columnTypes) {
    expect(this.columnTypes()).toEqual(columnTypes);
  },

  getRowAsArray: function(row) {
    return row.$$('td').map(textOf);
  },

  rows: function() {
    return this.sample.$$('tbody tr').map(e => this.getRowAsArray(e));
  },

  expectRowsAre(rows) {
    expect(this.rows()).toEqual(rows);
  },

  firstRow: function() {
    const row = this.sample.$$('tbody tr').get(0);
    return this.getRowAsArray(row);
  },

  expectFirstRowIs: function(row) {
    expect(this.firstRow()).toEqual(row);
  },

  clickColumn(columnId) { // for sorting
    const header = this.sample.$$('thead tr th#' + columnId);
    header.click();
  },

  clickShowMoreRows: function() {
    const button = this.control.$('#more-rows-button');
    button.click();
  },

  setRowCount: function(num) {
    const input = this.control.$('#sample-rows');
    safeSelectAndSendKeys(input, num.toString());
  },

  clickShowSample: function() {
    const button = this.control.$('#get-sample-button');
    button.click();
  },


};

function Side(popup, direction) {
  this.direction = direction;
  this.side = popup.$('#side-' + direction);
}

Side.prototype = {
  expectCurrentProjectIs: function(name) {
    expect(this.side.$('.project-name').getText()).toBe(name);
  },

  close: function() {
    this.side.$('#close-project').click();
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

  getValue: function(id) {
    const asStr = this.side.$('value#' + id + ' span.value').getText();
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
    safeSendKeys(this.toolbox.element(by.id('filter')), name, K.ENTER);
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

  populateOperationInput: function(parameterId, param) {
    safeSelectAndSendKeys(this.toolbox.element(by.id(parameterId)), param);
  },

  expectOperationScalar: function(name, text) {
    const cssSelector = 'value[ref="scalars[\'' + name + '\']"';
    const valueElement = this.toolbox.$(cssSelector);
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
    const slider = $('#sample-radius-slider');
    slider.getAttribute('value').then(function(value) {
      let diff = radius - value;
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
    return this.side.element(by.id('scalar-value-' + toId(name)));
  },

  saveProjectAs: function(newName) {
    this.side.element(by.id('save-as-starter-button')).click();
    safeSelectAndSendKeys(this.side.element(by.id('save-as-input')), newName);
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
    const res = this.side.$('#sql-result');
    expect(res.$$('thead tr th span.sql-column-name').map(e => e.getText())).toEqual(names);
    expect(res.$$('thead tr th span.sql-type').map(e => e.getText())).toEqual(types);
    expect(res.$$('tbody tr').map(e => e.$$('td').map(e => e.getText()))).toEqual(rows);
  },

  startSqlSaving: function() {
    this.side.element(by.id('save-results-opener')).click();
  },

  clickSqlSort(colId) {
    const res = this.side.$('#sql-result');
    const header = res.$$('thead tr th').get(colId);
    header.click();
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
    let pos = posList[0];
    let node = this.root.$$('#table-browser-tree > ul > li').get(pos);
    for (let i = 1; i < posList.length; ++i) {
      pos = posList[i];
      node = node.$$(node.locator().value + ' > ul > li').get(pos);
    }
    return node;
  },

  expectNode: function(posList, expectedName, expectedDragText) {
    const li = this.getNode(posList);
    expect(li.getText()).toBe(expectedName);
    if (expectedDragText) {
      this.expectDragText(li, expectedDragText);
    }
  },

  toggleNode: function(posList) {
    const li = this.getNode(posList);
    li.$(li.locator().value + ' > span').click();
  },

  getColumn: function(tablePos, columnPos) {
    const tableLi = this.getTable(tablePos);
    return tableLi.$$('ul > li').get(columnPos + 1);
  },

  expectColumn: function(tablePos, columnPos, name) {
    const columnLi = this.getColumn(tablePos, columnPos);
    expect(columnLi.getText()).toBe(name);
  },

  searchTable: function(searchText) {
    const searchBox = this.root.$('#search-for-tables');
    safeSendKeys(searchBox, searchText);
  },

  expectDragText: function(li, expected) {
    // We cannot do a real drag-and-drop workflow here
    // because of:
    // https://github.com/angular/protractor/issues/583
    // Just doing a simple check for now.
    const span = li.$(li.locator().value +
        ' > span > table-browser-entry > span');
    expect(span.evaluate('draggableText')).toBe(expected);
  },

  toggleFullyQualify: function() {
    this.root.$('#use-fully-qualified-names').click();
  },

  enterSearchQuery: function(query) {
    safeSelectAndSendKeys(element(by.id('table-browser-search-box')), query);
  },

};

function VisualizationState(popup) {
  this.popup = popup;
  this.svg = popup.$('svg.graph-view');
}

VisualizationState.prototype = {

  elementByLabel: function(label) {
    return this.svg.element(by.xpath('.//*[contains(text(),"' + label + '")]/..'));
  },

  clickMenu: function(item) {
    $('.context-menu #menu-' + item).click();
  },

  asTSV: function() {
    const copyButton = $('.graph-sidebar [data-clipboard-text');
    // It would be too complicated to test actual copy & paste. We just trust Clipboard.js instead.
    return copyButton.getAttribute('data-clipboard-text');
  },

  // The visualization response received from the server.
  graphView: function() {
    return this.svg.evaluate('graph.view');
  },

  // The currently visualized graph data extracted from the SVG DOM.
  graphData: function() {
    browser.waitForAngular();
    //browser.pause();
    return browser.executeScript(function() {

      // Vertices as simple objects.
      function vertexData(svg) {
        const vertices = svg.querySelectorAll('g.vertex');
        const result = [];
        for (let i = 0; i < vertices.length; ++i) {
          const v = vertices[i];
          const touch = v.querySelector('circle.touch');
          const x = touch.getAttribute('cx');
          const y = touch.getAttribute('cy');
          const icon = v.querySelector('path.icon');
          const label = v.querySelector('text');
          const image = v.querySelector('image');
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
        let i, byPosition = {};
        for (i = 0; i < vertices.length; ++i) {
          byPosition[vertices[i].pos.string] = i;
        }

        // Collect edges.
        const result = [];
        const edges = svg.querySelectorAll('g.edge');
        for (i = 0; i < edges.length; ++i) {
          const e = edges[i];
          const arc = e.querySelector('path.edge-arc');
          const [, srcPos, dstPos] = arc.getAttribute('d').match(/^M (.*? .*?) .* (.*? .*?)$/);
          const label = e.querySelector('text');
          result.push({
            src: byPosition[srcPos],
            dst: byPosition[dstPos],
            label: label ? label.innerHTML : '',
            color: arc.style.stroke,
            width: arc.getAttribute('stroke-width'),
          });
        }
        result.sort(function(a, b) {
          return a.src * vertices.length + a.dst - b.src * vertices.length - b.dst;
        });
        return result;
      }

      const svg = document.querySelector('svg.graph-view');
      const vertices = vertexData(svg);
      const edges = edgeData(svg, vertices);
      return { vertices: vertices, edges: edges };
    });
  },

  vertexCounts: function(index) {
    return this.graphView().then(function(gv) {
      return gv.vertexSets[index].vertices.length;
    });
  },
};

function Selector(root) {
  this.root = root;
  this.tableBrowser = new TableBrowser(this.root);
}

Selector.prototype = {
  workspace: function(name) {
    return element(by.id('workspace-' + toId(name)));
  },

  directory: function(name) {
    return element(by.id('directory-' + toId(name)));
  },

  table: function(name) {
    return element(by.id('table-' + toId(name)));
  },

  view: function(name) {
    return element(by.id('view-' + toId(name)));
  },

  snapshot: function(name) {
    return element(by.id('snapshot-' + toId(name)));
  },

  expectNumWorkspaces: function(n) {
    return expect($$('.workspace-entry').count()).toEqual(n);
  },

  expectNumDirectories: function(n) {
    return expect($$('.directory-entry').count()).toEqual(n);
  },

  expectSelectedCurrentDirectory: function(path) {
    return expect($('#current-directory > span.lead').getText()).toEqual(path);
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
    const table = this.table(name);
    // Look up the number of rows shown inside a <value>
    // element.
    return expect(table.$('value').getText()).toEqual(n.toString());
  },

  openNewWorkspace: function(name) {
    element(by.id('new-workspace')).click();
    safeSendKeys(element(by.id('new-workspace-name')), name);
    $('#new-workspace button[type=submit]').click();
    this.hideFloatingElements();
  },

  startTableImport: function() {
    element(by.id('import-table')).click();
  },

  clickAndWaitForCsvImport: function() {
    const importCsvButton = element(by.id('import-csv-button'));
    // Wait for the upload to finish.
    testLib.waitUntilClickable(importCsvButton);
    importCsvButton.click();
  },

  importLocalCSVFile: function(tableName, localCsvFile, csvColumns, columnsToImport, view, limit) {
    safeSendKeys(this.root.$('import-wizard #table-name input'), tableName);
    if (columnsToImport) {
      safeSendKeys(this.root.$('import-wizard #columns-to-import input'), columnsToImport);
    }
    this.root.$('#datatype select option[value="csv"]').click();
    if (csvColumns) {
      safeSendKeys(this.root.$('import-wizard #csv-column-names input'), csvColumns);
    }
    const csvFileParameter = $('#csv-filename file-parameter');
    testLib.uploadIntoFileParameter(csvFileParameter, localCsvFile);
    if (view) {
      this.root.$('import-wizard #as-view input').click();
    }
    if (limit) {
      safeSendKeys(this.root.$('import-wizard #limit input'), limit.toString());
    }
    this.clickAndWaitForCsvImport();
  },

  importJDBC: function(tableName, jdbcUrl, jdbcTable, jdbcKeyColumn, view) {
    safeSendKeys(this.root.$('import-wizard #table-name input'), tableName);
    this.root.$('#datatype select option[value="jdbc"]').click();
    safeSendKeys(this.root.$('#jdbc-url input'), jdbcUrl);
    safeSendKeys(this.root.$('#jdbc-table input'), jdbcTable);
    safeSendKeys(this.root.$('#jdbc-key-column input'), jdbcKeyColumn);
    if (view) {
      this.root.$('import-wizard #as-view input').click();
    }
    this.root.$('#import-jdbc-button').click();
  },

  newDirectory: function(name) {
    element(by.id('new-directory')).click();
    safeSendKeys(element(by.id('new-directory-name')), name);
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
        '.spark-status, .user-menu { position: static !important; }');
        `);
  },

  openDirectory: function(name) {
    this.directory(name).click();
  },

  popDirectory: function() {
    element(by.id('pop-directory-icon')).click();
  },

  setDirectory: function(n) {
    $('#current-directory > span.lead > span:nth-child(' + n + ') > span.path-element').click();
  },

  renameWorkspace: function(name, newName) {
    const workspace = this.workspace(name);
    testLib.menuClick(workspace, 'rename');
    safeSelectAndSendKeys(workspace.element(by.id('renameBox')), newName).submit();
  },

  deleteWorkspace: function(name) {
    testLib.menuClick(this.workspace(name), 'discard');
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

  expectWorkspaceListed: function(name) {
    testLib.expectElement(this.workspace(name));
  },

  expectWorkspaceNotListed: function(name) {
    testLib.expectNotElement(this.workspace(name));
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

  expectSnapshotListed: function(name) {
    testLib.expectElement(this.snapshot(name));
  },

  enterSearchQuery: function(query) {
    safeSelectAndSendKeys(element(by.id('search-box')), query);
  },

  clearSearchQuery: function() {
    safeSelectAndSendKeys(element(by.id('search-box')), K.BACK_SPACE);
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
    const res = element(by.id('sql-result'));
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
    safeSendKeys(this.root.$('#exportKiteTable'), name);
    element(by.id('save-results')).click();
  },

  saveGlobalSqlToView: function(name) {
    element(by.id('save-results-opener')).click();
    this.root.$('#exportFormat option[value="view"]').click();
    safeSendKeys(this.root.$('#exportKiteTable'), name);
    element(by.id('save-results')).click();
  },
};

const splash = new Selector(element(by.id('splash')));

function randomPattern () {
  /* eslint-disable no-bitwise */
  const crypto = require('crypto');
  const buf = crypto.randomBytes(16);
  const sixteenLetters = 'abcdefghijklmnop';
  let r = '';
  for (let i = 0; i < buf.length; i++) {
    const v = buf[i];
    const lo = (v & 0xf);
    const hi = (v >> 4);
    r += sixteenLetters[lo] + sixteenLetters[hi];
  }
  return r;
}

let lastDownloadList;

function getSelectAllKey() {
  if (isMacOS()) {
    // The command key is not supported properly, so we work around with Shift+HOME etc.
    // and Delete. https://github.com/angular/protractor/issues/690
    return K.END + K.PAGE_DOWN + K.chord(K.SHIFT, K.HOME) + K.chord(K.SHIFT, K.PAGE_UP) + K.DELETE;
  } else {
    return K.chord(K.CONTROL, 'a');
  }
}

testLib = {
  theRandomPattern: randomPattern(),
  workspace: new Workspace(),
  splash: splash,
  selectAllKey: getSelectAllKey(),
  protractorDownloads: '/tmp/protractorDownloads.' + process.pid,

  viewerState: function(name) {
    const container = $(`snapshot-viewer[path="${name}"]`);
    return new State(container);
  },

  expectElement: function(e) {
    expect(e.isDisplayed()).toBe(true);
  },

  expectNotElement: function(e) {
    expect(e.isPresent()).toBe(false);
  },

  // Deletes all projects and directories.
  discardAll: function() {
    function discard(defer) {
      const req = request.defaults({ jar: true });
      req.post(
        browser.baseUrl + 'ajax/discardAllReallyIMeanIt',
        { json: { fake: 1 } },
        (error, message) => {
          if (error || message.statusCode >= 400) {
            defer.reject(new Error(error));
          } else {
            defer.fulfill();
          }
        });
    }
    this.authenticateAndPost('admin', 'adminpw', 'lynxkite', discard);
  },

  authenticateAndPost: function(username, password, method, func) {
    function sendRequest() {
      const defer = protractor.promise.defer();
      if (!process.env.HTTPS_PORT) {
        return func(defer);
      }
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
      const req = request.defaults({ jar: true });
      req.post(
        browser.baseUrl + 'passwordLogin',
        { json: {
          'username': username,
          'password': password,
          'method': method
        }}, (error, message) => {
          if (error || message.statusCode >= 400) {
            defer.reject(new Error(error)); // TODO: include message?
          } else {
            func(defer);
          }
        });
      return defer.promise;
    }
    return browser.controlFlow().execute(sendRequest);
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
    const aceContent = e.$('div.ace_content');
    const aceInput = e.$('textarea.ace_text-input');
    // The double click on the text area focuses it properly.
    browser.actions().doubleClick(aceContent).perform();
    aceInput.sendKeys(testLib.selectAllKey + keys);
  },

  setParameter: function(e, value) {
    // Special parameter types need different handling.
    e.evaluate('(param.multipleChoice ? "multi-" : "") + param.kind').then(
      function(kind) {
        if (kind === 'code') {
          testLib.sendKeysToACE(e, testLib.selectAllKey + value);
        } else if (kind === 'file') {
          testLib.uploadIntoFileParameter(e, value);
        } else if (kind === 'tag-list') {
          const values = value.split(',');
          for (let i = 0; i < values.length; ++i) {
            e.$('.dropdown-toggle').click();
            e.$('.dropdown-menu #' + values[i]).click();
          }
        } else if (kind === 'table') {
          // You can specify a CSV file to be uploaded, or the name of an existing table.
          if (value.indexOf('.csv') !== -1) { // CSV file.
            e.element(by.id('import-new-table-button')).click();
            const s = new Selector(e.element(by.id('import-wizard')));
            s.importLocalCSVFile('test-table', value);
          } else { // Table name.
            // Table name options look like 'name of table (date of table creation)'.
            // The date is unpredictable, but we are going to match to the ' (' part
            // to minimize the chance of mathcing an other table.
            const optionLabelPattern = value + ' (';
            e.element(by.cssContainingText('option', optionLabelPattern)).click();
          }
        } else if (kind === 'choice') {
          e.$('option[label="' + value + '"]').click();
        } else if (kind === 'multi-choice') {
          // The mouse events through Protractor give different results than the real
          // mouse clicks. Keyboard selection is not flexible enough.
          // (https://bugs.chromium.org/p/chromium/issues/detail?id=125585)
          // So we select the requested items by injecting this script.
          browser.executeScript(`
            for (let opt of arguments[0].querySelectorAll('option')) {
              opt.selected = arguments[1].includes(opt.label);
            }
            arguments[0].dispatchEvent(new Event('change'));
            `, e, value);
        } else if (kind === 'multi-tag-list') {
          for (let i = 0; i < value.length; ++i) {
            e.$('.glyphicon-plus').click();
            e.$('a#' + value[i]).click();
          }
        } else {
          safeSelectAndSendKeys(e, value);
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
        '  return ' + responseValue + ';' +
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
    const t = $('.modal-title');
    testLib.expectElement(t);
    expect(t.getText()).toEqual(title);
  },

  closeModal: function() {
    element(by.id('close-modal-button')).click();
  },

  pythonPopup: function() {
    element(by.id('save-boxes-as-python')).click();
  },

  expectPythonCode: function(expectedCode) {
    let pythonCode = $('#python-code').getText();
    expect(pythonCode).toEqual(expectedCode);
  },

  setEnablePopups: function(enable) {
    browser.executeScript(
      'angular.element(document.body).injector()' +
      '.get("dropTooltipConfig").enabled = ' + enable);

  },

  uploadIntoFileParameter: function(fileParameterElement, fileName) {
    const input = fileParameterElement.element(by.id('file'));
    // Need to unhide flowjs's secret file uploader.
    browser.executeScript(
      function(input) {
        input.style.visibility = 'visible';
        input.style.height = '1px';
        input.style.width = '1px';
        input.style.opacity = 1;
      },
      input.getWebElement());
    // Special parameter?
    // Does not work with safeSendKeys.
    input.sendKeys(fileName);
  },

  loadImportedTable: function() {
    const loadButton = $('#param-imported_table button');
    loadButton.click();
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
      const newList = fs.readdirSync(testLib.protractorDownloads).filter(function(fn) {
        return fn.match(regex);
      });
      // this will be undefined if no new element was found.
      const result = newList.filter(function(f) { return lastDownloadList.indexOf(f) < 0; })[0];
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

  expectHasText(element, text) {
    testLib.expectElement(element);
    expect(element.getText()).toBe(text);
  },

  menuClick: function(entry, action) {
    const menu = entry.$('.dropdown');
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
    const EC = protractor.ExpectedConditions;
    testLib.wait(EC.visibilityOf($('.sweet-alert.showSweetAlert.visible')));
    expect($('.sweet-alert h2').getText()).toBe(expectedMessage);
    $('.sweet-alert button.confirm').click();
    testLib.wait(EC.stalenessOf($('.sweet-alert.showSweetAlert')));
  },

  waitUntilClickable: function(element) {
    testLib.wait(protractor.ExpectedConditions.elementToBeClickable(element));
  },

  submitInlineInput: function(element, text) {
    const inputBox = element.$('input');
    const okButton = element.$('#ok');
    safeSelectAndSendKeys(inputBox, text);
    okButton.click();
  },

  // A matcher for lists of objects that ignores fields not present in the reference.
  // Example use:
  //   expect([{ a: 1, b: 1234 }, { a: 2, b: 2345 }]).toConcur([{ a: 1 }, { a: 2 }]);
  // Constraints in strings are also accepted for numerical values. E.g. '<5'.
  // Objects are recursively checked.
  addConcurMatcher: function() {
    jasmine.addMatchers({
      toConcur: function(util, customEqualityTesters) {
        return { compare: function(actual, expected) {
          function match(actual, expected) {
            if (expected === null) {
              return actual === null;
            } else if (typeof expected === 'object') {
              const keys = Object.keys(expected);
              for (let i = 0; i < keys.length; ++i) {
                const av = actual[keys[i]];
                const ev = expected[keys[i]];
                if (!match(av, ev)) {
                  return false;
                }
              }
              return true;
            } else if (typeof expected === 'string' && expected[0] === '<') {
              return actual < parseFloat(expected.slice(1));
            } else if (typeof expected === 'string' && expected[0] === '>') {
              return actual > parseFloat(expected.slice(1));
            } else {
              return util.equals(actual, expected, customEqualityTesters);
            }
          }

          if (actual.length !== expected.length) {
            return { pass: false };
          }
          for (let i = 0; i < actual.length; ++i) {
            if (!match(actual[i], expected[i])) {
              return { pass: false };
            }
          }
          return { pass: true };
        }};
      }});
  },
};

module.exports = testLib;
