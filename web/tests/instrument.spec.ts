//Tests instrument usage
import { test, expect } from '@playwright/test';
import { TableBrowser, Workspace } from './lynxkite';

let workspace: Workspace;

async function tableIsGood(t) {
  await t.expect(
    ['age', 'gender', 'id', 'income', 'location', 'name'],
    ['Double', 'String', 'String', 'Double', 'Array[Double]', 'String'],
    [['20.3', 'Male', '0', '1000', 'WrappedArray(40.71448, -74.00598)', 'Adam'],
    ['18.2', 'Female', '1', 'null', 'WrappedArray(47.5269674, 19.0323968)', 'Eve'],
    ['50.3', 'Male', '2', '2000', 'WrappedArray(1.352083, 103.819836)', 'Bob'],
    ['2', 'Male', '3', 'null', 'WrappedArray(-33.8674869, 151.2069902)', 'Isolated Joe']]);
}

test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
  await workspace.main.evaluate(e => { $(e).injector().get('environment').vegaConfig.renderer = 'svg'; });
});

test('visualize with instrument', async function ({ browser }) {
  const popup = await workspace.openStateView('eg0', 'graph');
  await popup.setInstrument(0, 'visualize', {});
  await popup.left.vertexAttribute('name').visualizeAs('label');
  await expect(workspace.page.locator('text=Adam')).toBeVisible();
  const graph = await popup.visualization.graphData();
  expect(graph.vertices).toConcur([
    { label: 'Adam' },
    { label: 'Eve' },
    { label: 'Bob' },
  ]);
  await popup.close();
});

test('sql and plot with instrument', async function ({ browser }) {
  const popup = await workspace.openStateView('eg0', 'graph');
  await popup.setInstrument(0, 'sql', {});
  await tableIsGood(popup.table);
  await popup.setInstrument(1, 'sql', {
    sql: 'select gender, mean(age) as age from input group by gender'
  });
  await popup.setInstrument(2, 'plot', {});
  await popup.plot.expectBarHeightsToBe([140, 186]);
  await popup.clearInstrument(1);
  await tableIsGood(popup.table);
  await popup.close();
});
