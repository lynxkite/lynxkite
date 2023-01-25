// Shared testing utilities.
// TODO: This is being migrated from test-lib.js. We will clean it up at the end.
import { expect, Locator, Browser, Page } from '@playwright/test';

// Mirrors the "id" filter.
function toId(x) {
  return x.toLowerCase().replace(/ /g, '-');
}

const numberFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 5 });
// Same number formatting as used by LynxKite.
export function humanize(x) {
  return numberFormat.format(x);
}

async function clickAll(elements: Locator, opts) {
  const n = await elements.count();
  // Clicking may remove them from the selector. So we start from the end.
  for (let i = n - 1; i >= 0; --i) {
    await elements.nth(i).click(opts);
  }
}

export class Entity {
  side: Locator;
  kind: string;
  name: string;
  kindName: string;
  element: Locator;
  menu: Locator;
  constructor(side, kind, name) {
    this.side = side;
    this.kind = kind;
    this.name = name;
    this.kindName = kind + '-' + name;
    this.element = side.locator('#' + this.kindName);
    this.menu = side.page().locator('#menu-' + this.kindName);
  }

  async popup() {
    if ((await this.menu.count()) == 0) {
      await this.element.click();
    }
    await expect(this.menu).toBeVisible();
    return this.menu;
  }

  async popoff() {
    if ((await this.element.count()) > 0) {
      await angularEval(this.element, 'closeMenu()');
    }
    await expect(this.menu).not.toBeVisible();
  }

  async setFilter(filterValue) {
    const filterBox = (await this.popup()).locator('#filter');
    await filterBox.fill(filterValue);
    await this.popoff();
  }

  async openHistogram(opts?: { precise?: boolean }) {
    const popup = await this.popup();
    const histogram = popup.locator('histogram');
    // The histogram will be automatically displayed if the attribute is already computed.
    // Click the menu item otherwise.
    if (await popup.locator('#show-histogram').isVisible()) {
      await popup.locator('#show-histogram').click();
    }
    await expect(histogram).toBeVisible();
    if (opts?.precise) {
      await popup.locator('#precise-histogram-calculation').click();
    }
    // Wait for the histogram to be loaded.
    await expect(histogram.locator('.loading')).toHaveCount(0, { timeout: 30_000 });
    return histogram;
  }

  async getHistogramValues(opts?: { precise?: boolean }) {
    const histogram = await this.openHistogram(opts);
    const tds = histogram.locator('.bar-container');
    const tdCount = await tds.count();
    let total = 0;
    const res = [] as { title: string; value: number; size: number }[];
    for (let i = 0; i < tdCount; ++i) {
      const td = tds.nth(i);
      const toolTip = await td.getAttribute('drop-tooltip');
      const style = await td.locator('.bar').getAttribute('style');
      const [, title, value] = toolTip!.match(/^(.*): (\d+)$/)!;
      const [, size] = style!.match(/^height: (\d+)%;$/)!;
      total += parseInt(value);
      res.push({ title, value: parseInt(value), size: parseInt(size) });
    }
    await expect(histogram.locator('#histogram-total')).toContainText(humanize(total));
    await this.popoff();
    return res;
  }

  async visualizeAs(visualization) {
    this.popup()
      .locator('#visualize-as-' + visualization)
      .click();
    testLib.expectElement(this.visualizedAs(visualization));
    this.popoff();
  }

  async visualizedAs(visualization) {
    return this.element.locator('#visualized-as-' + visualization);
  }

  async doNotVisualizeAs(visualization) {
    this.popup()
      .locator('#visualize-as-' + visualization)
      .click();
    testLib.expectNotElement(this.visualizedAs(visualization));
    this.popoff();
  }

  async clickMenu(id) {
    this.popup()
      .locator('#' + id)
      .click();
    this.popoff();
  }
}

export class Workspace {
  readonly page: Page;
  readonly main: Locator;
  readonly selector: Locator;
  readonly board: Locator;
  constructor(page: Page) {
    this.page = page;
    this.main = page.locator('#workspace-entry-point');
    this.selector = page.locator('.operation-selector');
    this.board = page.locator('#workspace-drawing-board');
  }

  // Starts with a brand new workspace.
  static async empty(page: Page): Promise<Workspace> {
    const splash = await Splash.open(page);
    const workspace = await splash.openNewWorkspace('test-example');
    await workspace.expectCurrentWorkspaceIs('test-example');
    return workspace;
  }

  async expectCurrentWorkspaceIs(name) {
    await expect(this.main.locator('#workspace-name')).toHaveText(name);
    // TODO: check that workspace is error-free
  }

  async close() {
    await this.main.locator('#close-workspace').click();
  }

  async openOperation(name) {
    this.selector.element(by.id('operation-search')).click();
    safeSendKeys(this.selector.element(by.id('filter')), name + K.ENTER);
    return this.selector.locator('operation-selector-entry').get(0);
  }

  async closeOperationSelector() {
    this.selector.element(by.id('operation-search')).click();
  }

  async closeLastPopup() {
    browser.actions().sendKeys(K.ESCAPE).perform();
  }

  async addBoxFromSelector(boxName) {
    browser
      .actions()
      .sendKeys('/' + boxName + K.ENTER + K.ESCAPE)
      .perform();
  }

  async addBox(boxData) {
    const id = boxData.id;
    const after = boxData.after;
    const inputs = boxData.inputs;
    const params = boxData.params;
    await this.page.evaluate(boxData => {
      $(document.querySelector('#workspace-drawing-board')).scope().workspace.addBox(
        boxData.name,
        { logicalX: boxData.x, logicalY: boxData.y },
        { boxId: boxData.id });
    }, boxData);
    // Wait for the backend to save this box.
    await expect(this.getOutputPlug(id)).not.toHaveClass(/plug-progress-unknown/);
    if (after) {
      await this.connectBoxes(
        after, await this.getOnlyOutputPlugId(after), id, await this.getOnlyInputPlugId(id));
    }
    if (inputs) {
      for (let i = 0; i < inputs.length; ++i) {
        const input = inputs[i];
        await this.connectBoxes(input.boxId, input.srcPlugId, id, input.dstPlugId);
      }
    }
    if (params) {
      await this.editBox(id, params);
    }
  }

  async clear() {
    // Select all.
    await clickAll(this.board.locator('g.box:not(.selected)'), { modifiers: ['Control'] });
    await this.board.press('Backspace');
  }

  async selectBoxes(boxIds) {
    // Unselect all.
    await clickAll(this.board.locator('g.box.selected'), { modifiers: ['Control'] });
    // Select given boxes.
    for (let i = 0; i < boxIds.length; ++i) {
      await this.clickBox(boxIds[i], { modifiers: ['Control'] });
    }
  }

  async expectNumSelectedBoxes(n) {
    await expect(this.page.locator('g.box.selected')).toHaveCount(n);
  }

  async expectNumBoxes(n) {
    await expect(this.root.locator('g.box')).toHaveCount(n);
  }

  async deleteBoxes(boxIds) {
    await this.selectBoxes(boxIds);
    await this.main.locator('#delete-selected-boxes').click();
  }

  async editBox(boxId, params) {
    const boxEditor = await this.openBoxEditor(boxId);
    await boxEditor.populateOperation(params);
    await boxEditor.close();
  }

  async addWorkspaceParameter(name, kind, defaultValue) {
    const boxEditor = await this.openBoxEditor('anchor');
    await boxEditor.element.locator('#add-parameter').click();
    await boxEditor.element.locator('#-id').fill(name);
    await boxEditor.element.locator('#' + name + '-type').selectOption({ label: kind });
    await this.page.pause();
    await boxEditor.element.locator('#' + name + '-default').fill(defaultValue);
    await boxEditor.close();
  }

  getBox(boxId) {
    return this.board.locator('.box#' + boxId);
  }

  getInputPlug(boxId, plugId) {
    let box = this.getBox(boxId);
    if (plugId) {
      return box.locator('#inputs #' + plugId + ' circle');
    } else {
      return box.locator('#inputs circle');
    }
  }

  getOutputPlug(boxId, plugId?) {
    let box = this.getBox(boxId);
    if (plugId) {
      return box.locator('#outputs #' + plugId + ' circle');
    } else {
      return box.locator('#outputs circle');
    }
  }

  async toggleStateView(boxId, plugId) {
    this.getOutputPlug(boxId, plugId).click();
  }

  async clickBox(boxId, opts = {}) {
    await this.getBox(boxId).locator('#click-target').click(opts);
  }

  async selectBox(boxId) {
    const box = await this.openBoxEditor(boxId);
    await box.close();
  }

  getBoxEditor(boxId) {
    const popup = this.board.locator('.popup#' + boxId);
    return new BoxEditor(popup);
  }

  async openBoxEditor(boxId) {
    await this.clickBox(boxId);
    const editor = this.getBoxEditor(boxId);
    await expect(editor.popup).toBeVisible();
    return editor;
  }

  async openStateView(boxId, plugId) {
    const popup = this.board.locator('.popup#' + boxId + '_' + plugId);
    await expect(popup).not.toBeVisible(); // If it is already open, use getStateView() instead.
    await this.toggleStateView(boxId, plugId);
    await expect(popup).toBeVisible();
    return new State(popup);
  }

  getStateView(boxId, plugId) {
    const popup = this.board.locator('.popup#' + boxId + '_' + plugId);
    return new State(popup);
  }

  getVisualizationEditor(boxId) {
    const editor = this.getBoxEditor(boxId);
    return new State(editor.popup);
  }

  async expectConnected(srcBoxId, srcPlugId, dstBoxId, dstPlugId) {
    const arrow = this.board.locator(`path#${srcBoxId}-${srcPlugId}-${dstBoxId}-${dstPlugId}`);
    await expect(arrow).toBeVisible();
  }

  async getOnlyInputPlugId(box) {
    const p = this.getInputPlug(box);
    return await p.locator('xpath=..').getAttribute('id');
  }

  async getOnlyOutputPlugId(box) {
    const p = this.getOutputPlug(box);
    return await p.locator('xpath=..').getAttribute('id');
  }

  async connectBoxes(srcBoxId, srcPlugId, dstBoxId, dstPlugId) {
    const src = this.getOutputPlug(srcBoxId, srcPlugId);
    const dst = this.getInputPlug(dstBoxId, dstPlugId);
    await expect(src).toBeVisible();
    await expect(dst).toBeVisible();
    await src.dragTo(dst);
    await this.expectConnected(srcBoxId, srcPlugId, dstBoxId, dstPlugId);
  }

  async getCustomBoxBrowserTree() {
    this.selector.element(by.css('div[drop-tooltip="Custom boxes"]')).click();
    return this.selector.element(by.css('operation-tree')).element(by.css('operation-tree-node[id="root"]'));
  }

  async saveWorkspaceAs(newName) {
    this.main.locator('#save-workspace-as-starter-button').click();
    safeSelectAndSendKeys(this.main.locator('#save-workspace-as-input input'), newName);
    this.main.locator('#save-workspace-as-input #ok').click();
  }
}

class PopupBase {
  popup: Locator;

  async close() {
    await this.popup.locator('#close-popup').click();
  }

  async moveTo(x, y) {
    const head = this.popup.locator('div.popup-head');
    const workspace = this.popup.page().locator('#workspace-drawing-board');
    await head.dragTo(workspace, { targetPosition: { x, y } });
  }

  head() {
    return this.popup.locator('.popup-head');
  }
}

export class BoxEditor extends PopupBase {
  element: Locator;
  constructor(popup) {
    super();
    this.popup = popup;
    this.element = popup.locator('box-editor');
  }

  operationParameter(param) {
    return this.element.locator('operation-parameters #param-' + param + ' .operation-attribute-entry');
  }

  parametricSwitch(param) {
    return this.element.locator('operation-parameters #param-' + param + ' .parametric-switch');
  }

  async removeParameter(param) {
    await this.element.locator('operation-parameters #param-' + param + ' .remove-parameter').click();
  }

  async openGroup(group) {
    await this.element.getByText(group).click();
  }

  async populateOperation(params) {
    params = params || {};
    for (const key in params) {
      await setParameter(this.operationParameter(key), params[key]);
    }
    this.head().click(); // Make sure the parameters are not focused.
  }

  getParameter(paramName, tag = 'input') {
    return this.element.locator(`div#param-${paramName} ${tag}`);
  }

  getCodeParameter(paramName) {
    return this.getParameter(paramName, '.ace_content');
  }

  async loadImportedTable() {
    await this.element.locator('#param-imported_table button').click();
    // TODO: Must we wait? Would users wait?
    await expect(this.element.locator('#param-imported_table button')).toContainText('Reimport');
  }

  getTableBrowser() {
    return new TableBrowser(this.popup);
  }
}

export class State extends PopupBase {
  popup: Locator;
  left: Side;
  right: Side;
  table: TableState;
  plot: PlotState;
  visualization: VisualizationState;
  constructor(popup) {
    super();
    this.popup = popup;
    this.left = new Side(this.popup, 'left');
    this.right = new Side(this.popup, 'right');
    this.table = new TableState(this.popup);
    this.plot = new PlotState(this.popup);
    this.visualization = new VisualizationState(this.popup);
  }

  async setInstrument(index, name, params) {
    const toolbar = this.popup.locator(`#state-toolbar-${index}`);
    const editor = this.popup.locator(`#state-editor-${index}`);
    toolbar.locator(`#instrument-with-${name}`).click();
    params = params || {};
    for (const key in params) {
      const param = editor.locator(`operation-parameters #param-${key} .operation-attribute-entry`);
      await setParameter(param, params[key]);
    }
    $('#workspace-name').click(); // Make sure the parameters are not focused.
  }

  clearInstrument(index) {
    this.popup.locator(`#state-toolbar-${index} #clear-instrument`).click();
  }
}

class PlotState extends PopupBase {
  canvas: Locator;
  constructor(popup) {
    super();
    this.popup = popup;
    this.canvas = popup.locator('#plot-div svg');
  }

  async barHeights() {
    const bars = this.canvas.locator('g.mark-rect.marks path');
    await expect(bars).not.toHaveCount(0);
    const heights: number[] = [];
    const count = await bars.count();
    for (let i = 0; i < count; ++i) {
      const path = await bars.nth(i).getAttribute('d')!;
      // The bars are rectangles with paths like "M1,144h18v56h-18Z", which would be 56 pixels tall.
      heights.push(parseFloat(path.match(/v([0-9.]+)h/)![1]));
    }
    return heights;
  }

  async expectBarHeightsToBe(expected) {
    // The heights from local runs and Jenkins do not match. Allow 1% flexibility.
    const heights = await this.barHeights()
    await expect(heights.length).toEqual(expected.length);
    for (let i = 0; i < heights.length; ++i) {
      expect(heights[i]).toBeCloseTo(expected[i]);
    }
  }
}

export class TableState extends PopupBase {
  constructor(popup) {
    super();
    this.popup = popup;
    this.sample = popup.locator('#table-sample');
    this.control = popup.locator('#table-control');
  }

  async expect(names, types, rows) {
    await this.expectColumnNamesAre(names);
    await this.expectColumnTypesAre(types);
    await this.expectRowsAre(rows);
  }

  columnNames() {
    return this.sample.locator('thead tr th span.column-name');
  }

  async expectColumnNamesAre(columnNames) {
    await expect(this.columnNames()).toHaveText(columnNames, { timeout: 30_000 });
  }

  columnTypes() {
    return this.sample.locator('thead tr th span.column-type');
  }

  async expectColumnTypesAre(columnTypes) {
    await expect(this.columnTypes()).toHaveText(columnTypes);
  }

  rows() {
    return this.sample.locator('tbody tr');
  }

  async expectRowsAre(rows) {
    const r = this.rows();
    await expect(r).toHaveCount(rows.length);
    for (let i = 0; i < rows.length; ++i) {
      await expect(r.nth(i).locator('td')).toHaveText(rows[i]);
    }
  }

  firstRow() {
    const row = this.sample.locator('tbody tr').first();
    return row.locator('td');
  }

  expectFirstRowIs(row) {
    expect(this.firstRow()).toEqual(row);
  }

  clickColumn(columnId) {
    // for sorting
    const header = this.sample.locator('thead tr th#' + columnId);
    header.click();
  }

  clickShowMoreRows() {
    const button = this.control.locator('#more-rows-button');
    button.click();
  }

  setRowCount(num) {
    const input = this.control.locator('#sample-rows');
    safeSelectAndSendKeys(input, num.toString());
  }

  clickShowSample() {
    const button = this.control.locator('#get-sample-button');
    button.click();
  }
}

class Side {
  direction: string;
  side: Locator;
  edgeCount: Locator;
  vertexCount: Locator;
  segmentCount: Locator;
  attributes: Locator;
  constructor(popup, direction) {
    this.direction = direction;
    this.side = popup.locator('#side-' + direction);

    this.edgeCount = this.getValue('edge-count');
    this.vertexCount = this.getValue('vertex-count');
    this.segmentCount = this.getValue('segment-count');
    this.attributes = this.side.locator('entity[kind="vertex-attribute"], entity[kind="edge-attribute"]');
  }

  expectCurrentProjectIs(name) {
    expect(this.side.locator('.project-name').getText()).toBe(name);
  }

  close() {
    this.side.locator('#close-project').click();
  }

  evaluate(expr) {
    return this.side.evaluate(expr);
  }

  applyFilters() {
    return this.side.element(by.id('apply-filters-button')).click();
  }

  getCategorySelector(categoryTitle) {
    return this.toolbox.locator('div.category[tooltip="' + categoryTitle + '"]');
  }

  getValue(id) {
    return this.side.locator('value#' + id + ' span.value');
  }

  getWorkflowCodeEditor() {
    return this.side.element(by.id('workflow-code-editor'));
  }

  getPythonWorkflowCodeEditor() {
    return this.side.element(by.id('python-code-editor'));
  }

  getWorkflowDescriptionEditor() {
    return this.side.element(by.id('workflow-description'));
  }

  getWorkflowNameEditor() {
    return this.side.element(by.id('workflow-name'));
  }

  clickWorkflowEditButton() {
    return this.toolbox.element(by.id('edit-operation-button')).click();
  }

  getWorkflowSaveButton() {
    return this.side.element(by.id('save-workflow-button'));
  }

  openOperation(name) {
    this.toolbox.element(by.id('operation-search')).click();
    safeSendKeys(this.toolbox.element(by.id('filter')), name, K.ENTER);
  }

  closeOperation() {
    this.toolbox.locator('div.category.active').click();
  }

  openWorkflowSavingDialog() {
    this.side.element(by.id('save-as-workflow-button')).click();
  }

  closeWorkflowSavingDialog() {
    this.side.element(by.id('close-workflow-button')).click();
  }

  openSegmentation(segmentationName) {
    this.segmentation(segmentationName).clickMenu('open-segmentation');
  }

  redoButton() {
    return this.side.element(by.id('redo-button'));
  }

  populateOperationInput(parameterId, param) {
    safeSelectAndSendKeys(this.toolbox.element(by.id(parameterId)), param);
  }

  expectOperationScalar(name, text) {
    const cssSelector = 'value[ref="scalars[\'' + name + '\']"';
    const valueElement = this.toolbox.locator(cssSelector);
    expect(valueElement.getText()).toBe(text);
  }

  toggleSampledVisualization() {
    this.side.element(by.id('sampled-mode-button')).click();
  }

  toggleBucketedVisualization() {
    this.side.element(by.id('bucketed-mode-button')).click();
  }

  undoButton() {
    return this.side.element(by.id('undo-button'));
  }

  setSampleRadius(radius) {
    this.side.locator('#setting-sample-radius').click();
    const slider = $('#sample-radius-slider');
    slider.getAttribute('value').then(function (value) {
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
  }

  scalarValue(name) {
    return this.side.element(by.id('scalar-value-' + toId(name)));
  }

  saveProjectAs(newName) {
    this.side.element(by.id('save-as-starter-button')).click();
    safeSelectAndSendKeys(this.side.element(by.id('save-as-input')), newName);
    this.side.element(by.id('save-as-button')).click();
  }

  sqlEditor() {
    return this.side.element(by.id('sql-editor'));
  }

  setSql(sql) {
    testLib.sendKeysToACE(this.sqlEditor(), sql);
  }

  // If sql is left undefined then we run whatever is already in the query box.
  runSql(sql) {
    if (sql !== undefined) {
      this.setSql(sql);
    }
    this.side.element(by.id('run-sql-button')).click();
  }

  expectSqlResult(names, types, rows) {
    const res = this.side.locator('#sql-result');
    expect(res.locator('thead tr th span.sql-column-name').map(e => e.getText())).toEqual(names);
    expect(res.locator('thead tr th span.sql-type').map(e => e.getText())).toEqual(types);
    expect(res.locator('tbody tr').map(e => e.locator('td').map(e => e.getText()))).toEqual(rows);
  }

  startSqlSaving() {
    this.side.element(by.id('save-results-opener')).click();
  }

  clickSqlSort(colId) {
    const res = this.side.locator('#sql-result');
    const header = res.locator('thead tr th').get(colId);
    header.click();
  }

  executeSqlSaving() {
    this.side.element(by.id('save-results')).click();
  }

  vertexAttribute(name) {
    return new Entity(this.side, 'vertex-attribute', name);
  }
  edgeAttribute(name) {
    return new Entity(this.side, 'edge-attribute', name);
  }
  scalar(name) {
    return new Entity(this.side, 'scalar', name);
  }
  segmentation(name) {
    return new Entity(this.side, 'segmentation', name);
  }
}

export class TableBrowser {
  root: Locator;
  constructor(root) {
    this.root = root;
  }

  async toggle() {
    await this.root.locator('#toggle-table-browser').click();
  }

  getNode(posList) {
    let pos = posList[0];
    let node = this.root.locator('#table-browser-tree > ul > li').nth(pos);
    for (let i = 1; i < posList.length; ++i) {
      pos = posList[i];
      node = node.locator('ul > li').nth(pos);
    }
    return node;
  }

  async expectNode(posList, expectedName, expectedDragText?) {
    const li = this.getNode(posList);
    await expect(li).toHaveText(expectedName);
    if (expectedDragText) {
      await this.expectDragText(li, expectedDragText);
    }
  }

  async toggleNode(posList) {
    const li = this.getNode(posList);
    await li.locator('.glyphicon').click();
  }

  getColumn(tablePos, columnPos) {
    const tableLi = this.getTable(tablePos);
    return tableLi.locator('ul > li').nth(columnPos + 1);
  }

  async expectColumn(tablePos, columnPos, name) {
    const columnLi = this.getColumn(tablePos, columnPos);
    await expect(columnLi).toHaveText(name);
  }

  async searchTable(searchText) {
    const searchBox = this.root.locator('#search-for-tables');
    await searchBox.fill(searchText);
  }

  async expectDragText(li, expected) {
    // We cannot do a real drag-and-drop workflow here
    // because of:
    // https://github.com/angular/protractor/issues/583
    // Just doing a simple check for now.
    // TODO: We're no longer on Protractor! Let's try a drag & drop!
    const span = li.locator('[draggable]');
    expect(await angularEval(span, 'draggableText')).toBe(expected);
  }

  async toggleFullyQualify() {
    await this.root.locator('#use-fully-qualified-names').click();
  }

  async enterSearchQuery(query) {
    await this.root.locator('#table-browser-search-box').fill(query);
  }
}

class VisualizationState {
  constructor(popup) {
    this.popup = popup;
    this.svg = popup.locator('svg.graph-view');
  }

  elementByLabel(label) {
    return this.svg.element(by.xpath('.//*[contains(text(),"' + label + '")]/..'));
  }

  clickMenu(item) {
    $('.context-menu #menu-' + item).click();
  }

  asTSV() {
    const copyButton = $('.graph-sidebar [data-clipboard-text');
    // It would be too complicated to test actual copy & paste. We just trust Clipboard.js instead.
    return copyButton.getAttribute('data-clipboard-text');
  }

  // The visualization response received from the server.
  graphView() {
    return this.svg.evaluate('graph.view');
  }

  // The currently visualized graph data extracted from the SVG DOM.
  graphData() {
    browser.waitForAngular();
    //browser.pause();
    return browser.executeScript(function () {
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
        let i,
          byPosition = {};
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
        result.sort(function (a, b) {
          return a.src * vertices.length + a.dst - b.src * vertices.length - b.dst;
        });
        return result;
      }

      const svg = document.querySelector('svg.graph-view');
      const vertices = vertexData(svg);
      const edges = edgeData(svg, vertices);
      return { vertices: vertices, edges: edges };
    });
  }

  vertexCounts(index) {
    return this.graphView().then(function (gv) {
      return gv.vertexSets[index].vertices.length;
    });
  }
}

export class Splash {
  page: Page;
  root: Locator;
  constructor(page) {
    this.page = page;
    this.root = page.locator('#splash');
  }

  // Opens the LynxKite directory browser in the root.
  static async open(page: Page): Promise<Splash> {
    await page.goto('/#/');
    await page.evaluate(() => {
      window.sessionStorage.clear();
      window.localStorage.clear();
      window.localStorage.setItem('workspace-drawing-board tutorial done', 'true');
      window.localStorage.setItem('entry-selector tutorial done', 'true');
      window.localStorage.setItem('allow data collection', 'false');
      // Floating elements can overlap buttons and block clicks.
      document.styleSheets[0].insertRule('.spark-status, .user-menu { position: static !important; }');
    });
    await page.goto('/#/dir/');
    const splash = new Splash(page);
    await splash.expectDirectoryListed('built-ins'); // Make sure the page is loaded.
    if (await splash.directory('automated-tests').isVisible()) {
      await splash.deleteDirectory('automated-tests');
    }
    await splash.newDirectory('automated-tests');
    await splash.expectNumWorkspaces(0);
    await splash.expectNumDirectories(0);
    return splash;
  }

  workspace(name) {
    return this.root.locator('#workspace-' + toId(name));
  }

  directory(name) {
    return this.root.locator('#directory-' + toId(name));
  }

  table(name) {
    return this.root.locator('#table-' + toId(name));
  }

  view(name) {
    return this.root.locator('#view-' + toId(name));
  }

  snapshot(name) {
    return this.root.locator('#snapshow-' + toId(name));
  }

  async expectNumWorkspaces(n) {
    await expect(this.root.locator('.workspace-entry')).toHaveCount(n);
  }

  async expectNumDirectories(n) {
    await expect(this.root.locator('.directory-entry')).toHaveCount(n);
  }

  async expectCurrentDirectory(path) {
    await expect(this.root.locator('#current-directory > span.lead')).toHaveText(path);
  }

  async expectNumTables(n) {
    return expect($$('.table-entry')).toHaveCount(n);
  }

  async expectNumViews(n) {
    return expect($$('.view-entry')).toHaveCount(n);
  }

  async computeTable(name) {
    this.table(name).element(by.css('.value-retry')).click();
  }

  // Verifies that a computed table exists by the name 'name' and contains 'n' rows.
  async expectTableWithNumRows(name, n) {
    const table = this.table(name);
    // Look up the number of rows shown inside a <value>
    // element.
    return expect(table.locator('value').getText()).toEqual(n.toString());
  }

  async openNewWorkspace(name) {
    await this.expectWorkspaceNotListed(name);
    await this.root.locator('#new-workspace').click();
    await this.root.locator('#new-workspace-name').fill(name);
    await this.root.locator('#new-workspace button[type=submit]').click();
    const ws = new Workspace(this.page);
    // This expect() waits for the workspace to load.
    await expect(ws.getBox('anchor')).toBeVisible();
    return ws;
  }

  async startTableImport() {
    element(by.id('import-table')).click();
  }

  async clickAndWaitForCsvImport() {
    const importCsvButton = element(by.id('import-csv-button'));
    // Wait for the upload to finish.
    testLib.waitUntilClickable(importCsvButton);
    importCsvButton.click();
  }

  async newDirectory(name) {
    await this.expectDirectoryNotListed(name);
    await expect(this.root.locator('#new-directory')).toHaveText(/New folder/);
    await this.root.locator('#new-directory').click();
    await this.root.locator('#new-directory-name').fill(name);
    await this.root.locator('#new-directory button[type=submit]').click();
    // The terminal slash is not visible. https://github.com/microsoft/playwright/issues/19072
    await this.expectCurrentDirectory(new RegExp('\\b' + name + '/\\s*$'));
  }

  async openProject(name) {
    this.project(name).click();
    this.hideFloatingElements();
  }

  async openDirectory(name) {
    await this.directory(name).click();
  }

  async popDirectory() {
    await this.root.locator('#pop-directory-icon').click();
  }

  async clickBreadcrumb(name) {
    await this.root.locator('#current-directory').getByText(name).click();
  }

  async renameWorkspace(name, newName) {
    const workspace = this.workspace(name);
    await menuClick(workspace, 'rename');
    await workspace.locator('#renameBox').fill('automated-tests/' + newName);
    await workspace.locator('#renameBox').press('Enter');
  }

  async deleteWorkspace(name) {
    await menuClick(this.workspace(name), 'discard');
  }

  async deleteDirectory(name) {
    await menuClick(this.directory(name), 'discard');
  }

  async editTable(name) {
    testLib.menuClick(this.table(name), 'edit-import');
  }

  async editView(name) {
    testLib.menuClick(this.view(name), 'edit-import');
  }

  async expectWorkspaceListed(name) {
    await expect(this.workspace(name)).toBeVisible();
  }

  async expectWorkspaceNotListed(name) {
    await expect(this.workspace(name)).not.toBeVisible();
  }

  async expectDirectoryListed(name) {
    await expect(this.directory(name)).toBeVisible();
  }

  async expectDirectoryNotListed(name) {
    await expect(this.directory(name)).not.toBeVisible();
  }

  async expectTableListed(name) {
    testLib.expectElement(this.table(name));
  }

  async expectTableNotListed(name) {
    testLib.expectNotElement(this.table(name));
  }

  async expectViewListed(name) {
    testLib.expectElement(this.view(name));
  }

  async expectSnapshotListed(name) {
    testLib.expectElement(this.snapshot(name));
  }

  async enterSearchQuery(query) {
    await this.root.locator('#search-box').fill(query);
    await expect(this.root.locator('.progress.active')).not.toBeVisible();
  }

  async clearSearchQuery() {
    await this.enterSearchQuery('');
  }

  async globalSqlEditor() {
    return element(by.id('sql-editor'));
  }
  async setGlobalSql(sql) {
    testLib.sendKeysToACE(this.globalSqlEditor(), sql);
  }

  async openGlobalSqlBox() {
    element(by.id('global-sql-box')).click();
  }

  async runGlobalSql(sql) {
    this.openGlobalSqlBox();
    this.setGlobalSql(sql);
    element(by.id('run-sql-button')).click();
  }

  async expectGlobalSqlResult(names, types, rows) {
    const res = element(by.id('sql-result'));
    expect(res.locator('thead tr th span.sql-column-name').map(e => e.getText())).toEqual(names);
    expect(res.locator('thead tr th span.sql-type').map(e => e.getText())).toEqual(types);
    expect(res.locator('tbody tr').map(e => e.locator('td').map(e => e.getText()))).toEqual(rows);
  }

  async saveGlobalSqlToCSV() {
    element(by.id('save-results-opener')).click();
    this.root.locator('#exportFormat option[value="csv"]').click();
    element(by.id('save-results')).click();
  }

  async saveGlobalSqlToTable(name) {
    element(by.id('save-results-opener')).click();
    this.root.locator('#exportFormat option[value="table"]').click();
    safeSendKeys(this.root.locator('#exportKiteTable'), name);
    element(by.id('save-results')).click();
  }

  async saveGlobalSqlToView(name) {
    element(by.id('save-results-opener')).click();
    this.root.locator('#exportFormat option[value="view"]').click();
    safeSendKeys(this.root.locator('#exportKiteTable'), name);
    element(by.id('save-results')).click();
  }
}

function randomPattern() {
  /* eslint-disable no-bitwise */
  const crypto = require('crypto');
  const buf = crypto.randomBytes(16);
  const sixteenLetters = 'abcdefghijklmnop';
  let r = '';
  for (let i = 0; i < buf.length; i++) {
    const v = buf[i];
    const lo = v & 0xf;
    const hi = v >> 4;
    r += sixteenLetters[lo] + sixteenLetters[hi];
  }
  return r;
}

let lastDownloadList;

export async function menuClick(entry, action) {
  const menu = entry.locator('.dropdown');
  await menu.locator('a.dropdown-toggle').click();
  await menu.locator('#menu-' + action).click();
}

const theRandomPattern = randomPattern();
const protractorDownloads = '/tmp/protractorDownloads.' + process.pid;

function viewerState(name) {
  const container = $(`snapshot-viewer[path="${name}"]`);
  return new State(container);
}

function expectElement(e) {
  expect(e.isDisplayed()).toBe(true);
}

function expectNotElement(e) {
  expect(e.isPresent()).toBe(false);
}

function helpPopup(helpId) {
  return $('div[help-id="' + helpId + '"]');
}

async function sendKeysToACE(e, text) {
  await e.click();
  await e.page().keyboard.press('Control+a');
  await e.page().keyboard.type(text);
}

async function angularEval(e: Locator, expr: string) {
  return await e.evaluate((e, expr) => $(e).scope().$eval(expr), expr);
}

async function setParameter(e: Locator, value) {
  // Special parameter types need different handling.
  const kind = await angularEval(e, '(param.multipleChoice ? "multi-" : "") + param.kind');
  if (kind === 'code') {
    await sendKeysToACE(e, value);
  } else if (kind === 'file') {
    e.locator('input.form-control').fill(value);
  } else if (kind === 'tag-list') {
    const values = value.split(',');
    for (let i = 0; i < values.length; ++i) {
      e.locator('.dropdown-toggle').click();
      e.locator('.dropdown-menu #' + values[i]).click();
    }
  } else if (kind === 'choice') {
    await e.selectOption({ label: value });
  } else if (kind === 'multi-choice') {
    await e.selectOption(value.map(label => ({ label })));
  } else if (kind === 'multi-tag-list') {
    for (let i = 0; i < value.length; ++i) {
      await e.locator('.glyphicon-plus').click();
      await e.locator('a#' + value[i]).click();
    }
  } else {
    await e.fill(value);
  }
}

// Expects a window.confirm call from the client code and overrides the user
// response.
function expectDialogAndRespond(responseValue) {
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
    '  return ' +
    responseValue +
    ';' +
    '}'
  );
}

function checkAndCleanupDialogExpectation() {
  // Fail if there was no alert.
  expect(browser.executeScript('return window.confirm === window.confirm0')).toBe(true);
  browser.executeScript('window.confirm = window.confirm0;');
}

// Warning, this also sorts the given array parameter in place.
function sortHistogramValues(values) {
  return values.sort(function (b1, b2) {
    if (b1.title < b2.title) {
      return -1;
    } else if (b1.title > b2.title) {
      return 1;
    } else {
      return 0;
    }
  });
}

// A promise of the list of error messages.
function errors() {
  return $$('.top-alert-message').map(function (e) {
    return e.getText();
  });
}

// Expects that there will be a single error message and returns it as a promise.
function error() {
  return errors().then(function (errors) {
    expect(errors.length).toBe(1);
    return errors[0];
  });
}

function closeErrors() {
  $$('.top-alert').each(function (e) {
    e.element(by.id('close-alert-button')).click();
  });
}

// Wait indefinitely.
// WebDriver 2.45 changed browser.wait() to default to a 0 timeout. This was reverted in 2.46.
// But the current Protractor version uses 2.45, so we have this wrapper.
function wait(condition) {
  return browser.wait(condition, 99999999);
}

function expectModal(title) {
  const t = $('.modal-title');
  testLib.expectElement(t);
  expect(t.getText()).toEqual(title);
}

function closeModal() {
  element(by.id('close-modal-button')).click();
}

function pythonPopup() {
  element(by.id('save-boxes-as-python')).click();
}

function expectPythonCode(expectedCode) {
  let pythonCode = $('#python-code').getText();
  expect(pythonCode).toEqual(expectedCode);
}

function setEnablePopups(enable) {
  browser.executeScript(
    'angular.element(document.body).injector()' + '.get("dropTooltipConfig").enabled = ' + enable
  );
}

function startDownloadWatch() {
  browser.controlFlow().execute(function () {
    expect(lastDownloadList).toBe(undefined);
    lastDownloadList = fs.readdirSync(testLib.protractorDownloads);
  });
}

// Waits for a new downloaded file matching regex and returns its name.
// Pattern match is needed as chrome first creates some weird temp file.
function waitForNewDownload(regex) {
  return testLib.wait(function () {
    const newList = fs.readdirSync(testLib.protractorDownloads).filter(function (fn) {
      return fn.match(regex);
    });
    // this will be undefined if no new element was found.
    const result = newList.filter(function (f) {
      return lastDownloadList.indexOf(f) < 0;
    })[0];
    if (result) {
      lastDownloadList = undefined;
      return testLib.protractorDownloads + '/' + result;
    } else {
      return false;
    }
  });
}

function expectFileContents(filename, expectedContents) {
  filename.then(function (fn) {
    expect(fs.readFileSync(fn, 'utf8')).toBe(expectedContents);
  });
}

function expectHasClass(element, cls) {
  expect(element.getAttribute('class')).toBeDefined();
  element.getAttribute('class').then(function (classes) {
    expect(classes.split(' ').indexOf(cls)).not.toBe(-1);
  });
}

function expectNoClass(element, cls) {
  expect(element.getAttribute('class')).toBeDefined();
  element.getAttribute('class').then(function (classes) {
    expect(classes.split(' ').indexOf(cls)).toBe(-1);
  });
}

function expectHasText(element, text) {
  testLib.expectElement(element);
  expect(element.getText()).toBe(text);
}

function switchToWindow(pos) {
  browser.getAllWindowHandles().then(handles => {
    browser.driver.switchTo().window(handles[pos]);
  });
}

function showSelector() {
  $('#show-selector-button').click();
}

function confirmSweetAlert(expectedMessage) {
  // SweetAlert is not an Angular library. We need to wait until it pops in and out.
  const EC = protractor.ExpectedConditions;
  testLib.wait(EC.visibilityOf($('.sweet-alert.showSweetAlert.visible')));
  expect($('.sweet-alert h2').getText()).toBe(expectedMessage);
  $('.sweet-alert button.confirm').click();
  testLib.wait(EC.stalenessOf($('.sweet-alert.showSweetAlert')));
}

function waitUntilClickable(element) {
  testLib.wait(protractor.ExpectedConditions.elementToBeClickable(element));
}

function submitInlineInput(element, text) {
  const inputBox = element.locator('input');
  const okButton = element.locator('#ok');
  safeSelectAndSendKeys(inputBox, text);
  okButton.click();
}

// A matcher for lists of objects that ignores fields not present in the reference.
// Example use:
//   expect([{ a: 1, b: 1234 }, { a: 2, b: 2345 }]).toConcur([{ a: 1 }, { a: 2 }]);
// Constraints in strings are also accepted for numerical values. E.g. '<5'.
// Objects are recursively checked.
function addConcurMatcher() {
  jasmine.addMatchers({
    toConcur: function (util, customEqualityTesters) {
      return {
        compare: function (actual, expected) {
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
        },
      };
    },
  });
}
