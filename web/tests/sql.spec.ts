import { test, expect, Browser, Page } from '@playwright/test';
import { Workspace, TableState } from './lynxkite';
import { newSplash } from './splash.spec';

// Starts with a brand new workspace.
export async function newWorkspace(browser: Browser) {
  const splash = await newSplash(browser);
  const workspace = await splash.openNewWorkspace('test-example');
  await workspace.expectCurrentWorkspaceIs('test-example');
  return workspace;
}

//test.describe.configure({ mode: 'serial' });
let workspace: Workspace;
let table: TableState;
test.beforeAll(async ({ browser }) => {
  workspace = await newWorkspace(browser);
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({
    id: 'sql', name: 'SQL1', x: 100, y: 200
  });
  await workspace.connectBoxes('eg0', 'graph', 'sql', 'input');
  const state = await workspace.openStateView('sql', 'table');
  table = state.table;
  await table.expect(
    ['age', 'gender', 'id', 'income', 'location', 'name'],
    ['Double', 'String', 'String', 'Double', 'Array[Double]', 'String'],
    [
      ['20.3', 'Male', '0', '1000', 'WrappedArray(40.71448, -74.00598)', 'Adam'],
      ['18.2', 'Female', '1', 'null', 'WrappedArray(47.5269674, 19.0323968)', 'Eve'],
      ['50.3', 'Male', '2', '2000', 'WrappedArray(1.352083, 103.819836)', 'Bob'],
      ['2', 'Male', '3', 'null', 'WrappedArray(-33.8674869, 151.2069902)', 'Isolated Joe'],
    ]);
});
test.afterAll(async () => {
  await workspace.page.close();
});

async function runSQL(query) {
  await workspace.editBox('sql', { sql: query, persist: 'no' });
  const state = await workspace.getStateView('sql', 'table');
  return state.table;
}

test('SQL works for edge attributes', async () => {
  await runSQL('select edge_comment, src_name from edges order by edge_comment');
  await table.expect(
    ['edge_comment', 'src_name'],
    ['String', 'String'],
    [
      ['Adam loves Eve', 'Adam'],
      ['Bob envies Adam', 'Bob'],
      ['Bob loves Eve', 'Bob'],
      ['Eve loves Adam', 'Eve'],
    ]);
});

test('"order by" works right', async () => {
  await runSQL('select id, name from vertices order by name');
  await table.expect(
    ['id', 'name'],
    ['String', 'String'],
    [
      ['0', 'Adam'],
      ['2', 'Bob'],
      ['1', 'Eve'],
      ['3', 'Isolated Joe'],
    ]);
});

test('sql result table ordering works right with numbers', async () => {
  await runSQL('select age, name from vertices');
  await table.clickColumn('age');
  await table.expect(
    ['age', 'name'],
    ['Double', 'String'],
    [
      ['2', 'Isolated Joe'],
      ['18.2', 'Eve'],
      ['20.3', 'Adam'],
      ['50.3', 'Bob'],
    ]);
  await table.clickColumn('name');
  await table.expect(
    ['age', 'name'],
    ['Double', 'String'],
    [
      ['20.3', 'Adam'],
      ['50.3', 'Bob'],
      ['18.2', 'Eve'],
      ['2', 'Isolated Joe'],
    ]);
  await table.clickColumn('age');
  await table.clickColumn('age');
  await table.expect(
    ['age', 'name'],
    ['Double', 'String'],
    [
      ['50.3', 'Bob'],
      ['20.3', 'Adam'],
      ['18.2', 'Eve'],
      ['2', 'Isolated Joe'],
    ]);
});

test('sql result table ordering works right with nulls', async () => {
  await runSQL('select name, income from vertices');
  await table.clickColumn('income');
  await table.expect(
    ['name', 'income'],
    ['String', 'Double'],
    [
      ['Eve', 'null'],
      ['Isolated Joe', 'null'],
      ['Adam', '1000'],
      ['Bob', '2000'],
    ]);
  await table.clickColumn('income');
  await table.expect(
    ['name', 'income'],
    ['String', 'Double'],
    [
      ['Bob', '2000'],
      ['Adam', '1000'],
      ['Isolated Joe', 'null'],
      ['Eve', 'null'],
    ]);
  await table.close();
  await workspace.addBox({
    id: 'new_attr', name: 'Derive vertex attribute', x: 400, y: 150, after: 'eg0', params: {
      expr: 'if (income == 1000) "apple" else "orange"',
      output: 'new_attr',
    }
  });
  await workspace.connectBoxes('new_attr', 'graph', 'sql', 'input');
  await workspace.openStateView('sql', 'table');
  table = await runSQL('select new_attr from vertices');
  await table.clickColumn('new_attr');
  await table.expect(
    ['new_attr'],
    ['String'],
    [
      ['null'],
      ['null'],
      ['apple'],
      ['orange'],
    ]);
  await table.close();
  await workspace.deleteBoxes(['new_attr']);
  await workspace.connectBoxes('eg0', 'graph', 'sql', 'input');
  await workspace.openStateView('sql', 'table');
});

test('SQL runs nice on belongs to reached from project and segmentation', async () => {
  await workspace.clear();
  await workspace.addBox({
    id: 'vs', name: 'Create vertices', x: 100, y: 100, params: { size: '100' }
  });
  await workspace.addBox({
    after: 'vs',
    id: 'rnd', name: 'Add random vertex attribute', x: 100, y: 200, params: { seed: '1' }
  });
  await workspace.addBox({
    after: 'rnd',
    id: 'copy', name: 'Use base graph as segmentation', x: 100, y: 300
  });
  await workspace.addBox({
    id: 'sql', name: 'SQL1', x: 100, y: 400
  });
  await workspace.connectBoxes('copy', 'graph', 'sql', 'input');
  await workspace.openStateView('sql', 'table');
  const table = await runSQL(
    'select sum(base_random / segment_random) as sum from `self_as_segmentation.belongs_to`');
  await table.expect(['sum'], ['Double'], [['100']]);
});

test('SQL1 box table browser', async () => {
  await workspace.clear();
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({
    id: 'sql', name: 'SQL1', x: 100, y: 200
  });
  await workspace.connectBoxes('eg0', 'graph', 'sql', 'input');
  const se = await workspace.openBoxEditor('sql');
  const tableBrowser = se.getTableBrowser();
  await tableBrowser.toggle();
  await tableBrowser.expectNode([0], 'edge_attributes', '`edge_attributes`');
  await tableBrowser.expectNode([1], 'edges', '`edges`');
  await tableBrowser.expectNode([2], 'graph_attributes', '`graph_attributes`');
  await tableBrowser.expectNode([3], 'input.edge_attributes', '`input.edge_attributes`');
  await tableBrowser.expectNode([4], 'input.edges', '`input.edges`');
  await tableBrowser.expectNode([5], 'input.graph_attributes', '`input.graph_attributes`');
  await tableBrowser.expectNode([6], 'input.vertices', '`input.vertices`');
  await tableBrowser.expectNode([7], 'vertices', '`vertices`');
  await tableBrowser.toggleNode([7]);
  await tableBrowser.expectNode([7, 0], '*ALL*');
  await tableBrowser.expectNode([7, 1], 'age (Double)', '`age`');

  await tableBrowser.toggleFullyQualify();
  await tableBrowser.expectNode([7, 1], 'age (Double)', '`vertices`.`age`');
  await tableBrowser.expectNode(
    [7, 0],
    '*ALL*',
    '`vertices`.`age`,\n' +
    '`vertices`.`gender`,\n' +
    '`vertices`.`id`,\n' +
    '`vertices`.`income`,\n' +
    '`vertices`.`location`,\n' +
    '`vertices`.`name`');
  await tableBrowser.toggleFullyQualify();
});

test('SQL2 box table browser', async () => {
  await workspace.clear();
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({ id: 'eg1', name: 'Create example graph', x: 350, y: 100 });
  await workspace.addBox({
    id: 'sql', name: 'SQL2', x: 100, y: 200
  });
  await workspace.connectBoxes('eg0', 'graph', 'sql', 'one');
  await workspace.connectBoxes('eg1', 'graph', 'sql', 'two');
  const se = await workspace.openBoxEditor('sql');
  const tableBrowser = se.getTableBrowser();
  await tableBrowser.toggle();
  await tableBrowser.expectNode([0], 'one.edge_attributes', '`one.edge_attributes`');
  await tableBrowser.expectNode([1], 'one.edges', '`one.edges`');
  await tableBrowser.expectNode([2], 'one.graph_attributes', '`one.graph_attributes`');
  await tableBrowser.expectNode([3], 'one.vertices', '`one.vertices`');
  await tableBrowser.expectNode([4], 'two.edge_attributes', '`two.edge_attributes`');
  await tableBrowser.expectNode([5], 'two.edges', '`two.edges`');
  await tableBrowser.expectNode([6], 'two.graph_attributes', '`two.graph_attributes`');
  await tableBrowser.expectNode([7], 'two.vertices', '`two.vertices`');
  await tableBrowser.toggleNode([3]);
  await tableBrowser.expectNode([3, 0], '*ALL*');
  await tableBrowser.expectNode([3, 1], 'age (Double)', '`age`');
});
