// Shared testing utilities.
import { expect, Locator, Page } from '@playwright/test';

// Mirrors the "id" filter.
export function toId(x) {
  return x.toLowerCase().replace(/[ !?,./]/g, '-');
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

export const ROOT = 'automated-tests';
const isMacOS = process.platform === 'darwin';
export const CTRL = isMacOS ? 'Meta+' : 'Control+';

export class Entity {
  side: Locator;
  kind: string;
  name: string;
  kindName: string;
  element: Locator;
  menu: Locator;
  value: Locator;
  constructor(side: Locator, kind: string, name: string) {
    this.side = side;
    this.kind = kind;
    this.name = name;
    this.kindName = kind + '-' + name;
    this.element = side.locator('#' + this.kindName);
    this.menu = side.page().locator('#menu-' + this.kindName);
    this.value = this.element.locator('span.value');
  }

  async popup() {
    if ((await this.menu.count()) === 0) {
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
    await popup.locator('#precise-histogram-calculation').setChecked(opts?.precise ?? false);
    // Wait for the histogram to be loaded.
    await expect(histogram.locator('.loading')).toHaveCount(0, { timeout: 60_000 });
    return histogram;
  }

  async expectHistogramValues(
    expected: { title: string, size: number, value: number }[], opts?: { precise?: boolean }) {
    const histogram = await this.openHistogram(opts);
    const tds = histogram.locator('.bar-container');
    await expect(tds).toHaveCount(expected.length);
    let total = 0;
    for (let i = 0; i < expected.length; ++i) {
      const td = tds.nth(i);
      const exp = expected[i];
      await expect(td).toHaveAttribute('drop-tooltip', `${exp.title}: ${exp.value}`);
      await expect(td.locator('.bar')).toHaveAttribute('style', `height: ${exp.size}%;`);
      total += exp.value;
    }
    await expect(histogram.locator('#histogram-total')).toContainText(humanize(total));
    await this.popoff();
  }

  async visualizeAs(visualization: string) {
    const p = await this.popup();
    await p.locator('#visualize-as-' + visualization).click();
    await expect(this.visualizedAs(visualization)).toBeVisible();
    await this.popoff();
  }

  visualizedAs(visualization: string): Locator {
    return this.element.locator('#visualized-as-' + visualization);
  }

  async doNotVisualizeAs(visualization) {
    const p = await this.popup();
    await p.locator('#visualize-as-' + visualization).click();
    await expect(this.visualizedAs(visualization)).not.toBeVisible();
    await this.popoff();
  }

  async clickMenu(id: string) {
    const p = await this.popup();
    await p.locator('#' + id).click();
    await this.popoff();
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
  static async empty(page: Page, workspaceName?: string): Promise<Workspace> {
    const splash = await Splash.open(page);
    return await splash.openNewWorkspace(workspaceName ?? 'test-example');
  }

  async expectCurrentWorkspaceIs(name) {
    await expect(this.main.locator('#workspace-name')).toHaveText(name);
    // TODO: check that workspace is error-free
  }

  async close() {
    await this.main.locator('#close-workspace').click();
  }

  async addBox(boxData) {
    const id = boxData.id;
    const after = boxData.after;
    const inputs = boxData.inputs;
    const params = boxData.params;
    await this.page.evaluate(boxData => {
      /* global jQuery */
      jQuery(document.querySelector('#workspace-drawing-board')).scope().workspace.addBox(
        boxData.name,
        { logicalX: boxData.x, logicalY: boxData.y },
        { boxId: boxData.id });
    }, boxData);
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

  async deleteBoxes(boxIds) {
    await this.selectBoxes(boxIds);
    await this.main.locator('#delete-selected-boxes').click();
  }

  async editBox(boxId, params) {
    const boxEditor = await this.openBoxEditor(boxId);
    await boxEditor.populateOperation(params);
    await boxEditor.close();
  }

  getBox(boxId: string) {
    return this.board.locator('.box#' + boxId);
  }

  getInputPlug(boxId: string, plugId?: string) {
    const box = this.getBox(boxId);
    if (plugId) {
      return box.locator('#inputs #' + plugId + ' circle');
    } else {
      return box.locator('#inputs circle');
    }
  }

  getOutputPlug(boxId: string, plugId?: string) {
    const box = this.getBox(boxId);
    if (plugId) {
      return box.locator('#outputs #' + plugId + ' circle');
    } else {
      return box.locator('#outputs circle');
    }
  }

  async toggleStateView(boxId: string, plugId: string) {
    await this.getOutputPlug(boxId, plugId).click();
  }

  async clickBox(boxId: string, opts = {}) {
    await this.getBox(boxId).locator('#click-target').click(opts);
  }

  async selectBox(boxId: string) {
    const box = await this.openBoxEditor(boxId);
    await box.close();
  }

  getBoxEditor(boxId: string) {
    const popup = this.board.locator('.popup#' + boxId);
    return new BoxEditor(popup);
  }

  async openBoxEditor(boxId: string) {
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

  async expectConnected(srcBoxId: string, srcPlugId: string, dstBoxId: string, dstPlugId: string) {
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

  async saveWorkspaceAs(newName: string) {
    await this.main.locator('#save-workspace-as-starter-button').click();
    await this.main.locator('#save-workspace-as-input input').fill(ROOT + '/' + newName);
    await this.main.locator('#save-workspace-as-input #ok').click();
  }

  async submitInlineInput(selector: string, text: string) {
    const element = this.main.locator(selector);
    await element.locator('input').fill(text);
    await element.locator('#ok').click();
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
    await this.head().click(); // Make sure the parameters are not focused.
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
    await toolbar.locator(`#instrument-with-${name}`).click();
    params = params || {};
    for (const key in params) {
      const param = editor.locator(`operation-parameters #param-${key} .operation-attribute-entry`);
      await setParameter(param, params[key]);
    }
  }

  async clearInstrument(index) {
    await this.popup.locator(`#state-toolbar-${index} #clear-instrument`).click();
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
      const path = await bars.nth(i).getAttribute('d');
      // The bars are rectangles with paths like "M1,144h18v56h-18Z", which would be 56 pixels tall.
      heights.push(parseFloat(path!.match(/v([0-9.]+)h/)![1]));
    }
    return heights;
  }

  async expectBarHeightsToBe(expected) {
    const heights = await this.barHeights();
    expect(heights.length).toEqual(expected.length);
    for (let i = 0; i < heights.length; ++i) {
      expect(heights[i]).toBeCloseTo(expected[i], 0);
    }
  }
}

export class TableState extends PopupBase {
  sample: Locator;
  control: Locator;
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

  row(n: number) {
    return this.rows().nth(n).locator('td');
  }

  async expectRowsAre(rows) {
    const r = this.rows();
    await expect(r).toHaveCount(rows.length);
    for (let i = 0; i < rows.length; ++i) {
      await expect(this.row(i)).toHaveText(rows[i]);
    }
  }

  async clickColumn(columnId: string) {
    // for sorting
    const header = this.sample.locator('thead tr th#' + columnId);
    await header.click();
  }

  async clickShowMoreRows() {
    await this.control.locator('#more-rows-button').click();
  }

  async setRowCount(num: number) {
    const input = this.control.locator('#sample-rows');
    await input.fill(num.toString());
  }

  async clickShowSample() {
    await this.control.locator('#get-sample-button').click();
  }
}

class Side {
  direction: string;
  side: Locator;
  edgeCount: Locator;
  vertexCount: Locator;
  segmentCount: Locator;
  vertexAttributes: Locator;
  edgeAttributes: Locator;
  graphAttributes: Locator;
  projectName: Locator;
  constructor(popup, direction) {
    this.direction = direction;
    this.side = popup.locator('#side-' + direction);

    this.edgeCount = this.getValue('edge-count');
    this.vertexCount = this.getValue('vertex-count');
    this.segmentCount = this.getValue('segment-count');
    this.vertexAttributes = this.side.locator('entity[kind="vertex-attribute"]');
    this.edgeAttributes = this.side.locator('entity[kind="edge-attribute"]');
    this.graphAttributes = this.side.locator('entity[kind="scalar"]');
    this.projectName = this.side.locator('.project-name');
  }

  async close() {
    await this.side.locator('#close-project').click();
  }

  getValue(id: string) {
    return this.side.locator('value#' + id + ' span.value');
  }

  async openSegmentation(segmentationName: string) {
    await this.segmentation(segmentationName).clickMenu('open-segmentation');
  }

  async toggleSampledVisualization() {
    await this.side.locator('#sampled-mode-button').click();
  }

  async toggleBucketedVisualization() {
    await this.side.locator('#bucketed-mode-button').click();
  }

  async setSampleRadius(radius: number) {
    await this.side.locator('#setting-sample-radius').click();
    // Playwright can't deal with sliders. https://github.com/microsoft/playwright/issues/4231
    const slider = this.side.page().locator('#sample-radius-slider');
    const value = parseInt(await slider.inputValue());
    let diff = radius - value;
    while (diff > 0) {
      await slider.press('ArrowRight');
      diff -= 1;
    }
    while (diff < 0) {
      await slider.press('ArrowLeft');
      diff += 1;
    }
  }

  vertexAttribute(name: string) {
    return new Entity(this.side, 'vertex-attribute', name);
  }
  edgeAttribute(name: string) {
    return new Entity(this.side, 'edge-attribute', name);
  }
  graphAttribute(name: string) {
    return new Entity(this.side, 'scalar', name);
  }
  segmentation(name: string) {
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

  async expectDragText(li, expected) {
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
  svg: Locator;
  popup: Locator;
  constructor(popup: Locator) {
    this.popup = popup;
    this.svg = popup.locator('svg.graph-view');
  }
  async clickMenu(item: string) {
    await this.popup.locator('.context-menu #menu-' + item).click();
  }

  // The currently visualized graph data extracted from the SVG DOM.
  graphData(): Promise<{ vertices, edges }> {
    return this.popup.evaluate(async function () {
      // Vertices as simple objects.
      async function vertexData(svg) {
        const vertices = svg.querySelectorAll('g.vertex');
        const result: object[] = [];
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
      async function edgeData(svg, vertices) {
        // Build an index by position, so edges can be resolved to vertices.
        const byPosition = {};
        for (let i = 0; i < vertices.length; ++i) {
          byPosition[vertices[i].pos.string] = i;
        }

        // Collect edges.
        const result: object[] = [];
        const edges = svg.querySelectorAll('g.edge');
        for (let i = 0; i < edges.length; ++i) {
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
        result.sort((a: any, b: any) =>
          a.src * vertices.length + a.dst - b.src * vertices.length - b.dst
        );
        return result;
      }

      const svg = document.querySelector('svg.graph-view');
      const vertices = await vertexData(svg);
      const edges = await edgeData(svg, vertices);
      return { vertices: vertices, edges: edges };
    });
  }

  async expect(fn: (graph: { vertices, edges }) => void) {
    await expect(async () => {
      const graph = await this.graphData();
      fn(graph);
    }).toPass();
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
    });
    await page.goto('/#/dir/');
    await page.evaluate(() => {
      // Floating elements can overlap buttons and block clicks.
      document.styleSheets[0].insertRule('.spark-status, .user-menu { position: static !important; }');
      // Playwright won't click on something that is moving. Disable output plug animation.
      document.styleSheets[0].insertRule('.plug-progress-in-progress { animation-name: none !important; }');
    });
    const splash = new Splash(page);
    await splash.expectDirectoryListed('built-ins'); // Make sure the page is loaded.
    if (await splash.directory(ROOT).isVisible()) {
      await splash.deleteDirectory(ROOT);
    }
    await splash.newDirectory(ROOT);
    await splash.expectNumWorkspaces(0);
    await splash.expectNumDirectories(0);
    return splash;
  }

  workspace(name: string) {
    return this.root.locator('#workspace-' + toId(name));
  }

  directory(name: string) {
    return this.root.locator('#directory-' + toId(name));
  }

  snapshot(name: string) {
    return this.root.locator('#snapshot-' + toId(name));
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

  async openNewWorkspace(name: string) {
    await this.expectWorkspaceNotListed(name);
    await this.root.locator('#new-workspace').click();
    await this.root.locator('#new-workspace-name').fill(name);
    await this.root.locator('#new-workspace button[type=submit]').click();
    const ws = new Workspace(this.page);
    // This expect() waits for the workspace to load.
    await expect(ws.getBox('anchor')).toBeVisible();
    await ws.expectCurrentWorkspaceIs(name);
    return ws;
  }

  async openWorkspace(name: string) {
    await this.workspace(name).click();
    const ws = new Workspace(this.page);
    // This expect() waits for the workspace to load.
    await expect(ws.getBox('anchor')).toBeVisible();
    return ws;
  }

  async newDirectory(name: string) {
    await this.expectDirectoryNotListed(name);
    await expect(this.root.locator('#new-directory')).toHaveText(/New folder/);
    await this.root.locator('#new-directory').click();
    await this.root.locator('#new-directory-name').fill(name);
    await this.root.locator('#new-directory button[type=submit]').click();
    // The terminal slash is not visible. https://github.com/microsoft/playwright/issues/19072
    await this.expectCurrentDirectory(new RegExp('\\b' + name + '/\\s*$'));
  }

  async openDirectory(name: string) {
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
    await workspace.locator('#renameBox').fill(ROOT + '/' + newName);
    await workspace.locator('#renameBox').press('Enter');
  }

  async deleteWorkspace(name) {
    await menuClick(this.workspace(name), 'discard');
  }

  async deleteDirectory(name) {
    await menuClick(this.directory(name), 'discard');
  }

  async expectWorkspaceListed(name: string) {
    await expect(this.workspace(name)).toBeVisible();
  }

  async expectWorkspaceNotListed(name: string) {
    await expect(this.workspace(name)).not.toBeVisible();
  }

  async expectDirectoryListed(name: string) {
    await expect(this.directory(name)).toBeVisible();
  }

  async expectDirectoryNotListed(name: string) {
    await expect(this.directory(name)).not.toBeVisible();
  }

  async enterSearchQuery(query) {
    await this.root.locator('#search-box').fill(query);
    await expect(this.root.locator('.progress.active')).not.toBeVisible();
  }

  async clearSearchQuery() {
    await this.enterSearchQuery('');
  }

  snapshotState(name: string) {
    const container = this.root.locator(`snapshot-viewer[path="${name}"]`);
    return new State(container);
  }
}

export async function menuClick(entry: Locator, action: string) {
  const menu = entry.locator('.dropdown');
  await menu.locator('a.dropdown-toggle').click();
  await menu.locator('#menu-' + action).click();
}

async function sendKeysToACE(e, text) {
  await e.click();
  await e.page().keyboard.press(CTRL + 'a');
  await e.page().keyboard.type(text);
}

async function angularEval(e: Locator, expr: string) {
  /* global jQuery */
  return await e.evaluate((e, expr) => jQuery(e).scope().$eval(expr), expr);
}

async function setParameter(e: Locator, value) {
  // Special parameter types need different handling.
  await expect(e).toBeVisible();
  const kind = await angularEval(e, '(param.multipleChoice ? "multi-" : "") + param.kind');
  if (kind === 'code') {
    await sendKeysToACE(e, value);
  } else if (kind === 'file') {
    await e.locator('input.form-control').fill(value);
  } else if (kind === 'tag-list') {
    const values = value.split(',');
    for (let i = 0; i < values.length; ++i) {
      await e.locator('.dropdown-toggle').click();
      await e.locator('.dropdown-menu #' + values[i]).click();
    }
  } else if (kind === 'choice' || kind === 'segmentation') {
    await e.selectOption({ label: value });
  } else if (kind === 'multi-choice') {
    await e.selectOption(value.map(label => ({ label })));
  } else if (kind === 'multi-tag-list') {
    for (let i = 0; i < value.length; ++i) {
      await e.locator('.glyphicon-plus').click();
      await e.locator('a#' + value[i]).click();
    }
  } else if (kind === 'parameters') {
    for (const v of value) {
      await e.locator('#add-parameter').click();
      await e.locator('#-id').fill(v.id);
      await e.locator('#' + v.id + '-type').selectOption({ label: v.kind });
      await e.locator('#' + v.id + '-default').fill(v.defaultValue);
    }
  } else {
    await e.fill(value);
  }
}

export function errors(page: Page) {
  return page.locator('.top-alert-message');
}

export async function closeErrors(page: Page) {
  const n = await errors(page).count();
  for (let i = 0; i < n; ++i) {
    await page.locator('#close-alert-button').first().click();
  }
}

// A matcher for lists of objects that ignores fields not present in the reference.
// Example use:
//   expect([{ a: 1, b: 1234 }, { a: 2, b: 2345 }]).toConcur([{ a: 1 }, { a: 2 }]);
// Constraints in strings are also accepted for numerical values. E.g. '<5'.
// Objects are recursively checked.
expect.extend({
  toConcur: function (actual, expected) {
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
        return actual === expected;
      }
    }
    if (actual.length !== expected.length) {

      return {
        message: () => `Arrays do not have the same length: \n${JSON.stringify(actual)}, \n !=\n${JSON.stringify(expected)}\n`,
        pass: false
      };
    }
    for (let i = 0; i < actual.length; ++i) {
      if (!match(actual[i], expected[i])) {
        return {
          message: () => `Items do not concur: \n${JSON.stringify(actual[i])}, \n !=\n${JSON.stringify(expected[i])}\n`,
          pass: false
        };
      }
    }
    return { pass: true };
  }
});
declare global {
  namespace PlaywrightTest {
    interface Matchers<R> {
      toConcur(expected: any): R;
    }
  }
}
