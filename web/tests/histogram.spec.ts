// Tests for the histograms displayed for vertex/edge attributes.
import { test, expect } from '@playwright/test';
import { State, Workspace } from './lynxkite';

let workspace: Workspace;
let state: State;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'ex0', name: 'Create example graph', x: 100, y: 100 });
  state = await workspace.openStateView('ex0', 'graph');
});

test('string vertex histogram', async () => {
  await expect(state.left.side).toHaveCount(1);
  await state.left.vertexAttribute('name').expectHistogramValues([
    { title: 'Adam', size: 100, value: 1 },
    { title: 'Bob', size: 100, value: 1 },
    { title: 'Eve', size: 100, value: 1 },
    { title: 'Isolated Joe', size: 100, value: 1 },
  ]);
});
test('double vertex histogram', async () => {
  await state.left.vertexAttribute('income').expectHistogramValues([
    { title: '1000.0-1050.0', size: 100, value: 1 },
    { title: '1050.0-1100.0', size: 0, value: 0 },
    { title: '1100.0-1150.0', size: 0, value: 0 },
    { title: '1150.0-1200.0', size: 0, value: 0 },
    { title: '1200.0-1250.0', size: 0, value: 0 },
    { title: '1250.0-1300.0', size: 0, value: 0 },
    { title: '1300.0-1350.0', size: 0, value: 0 },
    { title: '1350.0-1400.0', size: 0, value: 0 },
    { title: '1400.0-1450.0', size: 0, value: 0 },
    { title: '1450.0-1500.0', size: 0, value: 0 },
    { title: '1500.0-1550.0', size: 0, value: 0 },
    { title: '1550.0-1600.0', size: 0, value: 0 },
    { title: '1600.0-1650.0', size: 0, value: 0 },
    { title: '1650.0-1700.0', size: 0, value: 0 },
    { title: '1700.0-1750.0', size: 0, value: 0 },
    { title: '1750.0-1800.0', size: 0, value: 0 },
    { title: '1800.0-1850.0', size: 0, value: 0 },
    { title: '1850.0-1900.0', size: 0, value: 0 },
    { title: '1900.0-1950.0', size: 0, value: 0 },
    { title: '1950.0-2000.0', size: 100, value: 1 },
  ]);
});
test('double edge histogram', async () => {
  await state.left.edgeAttribute('weight').expectHistogramValues([
    { title: '1.00-1.15', size: 100, value: 1 },
    { title: '1.15-1.30', size: 0, value: 0 },
    { title: '1.30-1.45', size: 0, value: 0 },
    { title: '1.45-1.60', size: 0, value: 0 },
    { title: '1.60-1.75', size: 0, value: 0 },
    { title: '1.75-1.90', size: 0, value: 0 },
    { title: '1.90-2.05', size: 100, value: 1 },
    { title: '2.05-2.20', size: 0, value: 0 },
    { title: '2.20-2.35', size: 0, value: 0 },
    { title: '2.35-2.50', size: 0, value: 0 },
    { title: '2.50-2.65', size: 0, value: 0 },
    { title: '2.65-2.80', size: 0, value: 0 },
    { title: '2.80-2.95', size: 0, value: 0 },
    { title: '2.95-3.10', size: 100, value: 1 },
    { title: '3.10-3.25', size: 0, value: 0 },
    { title: '3.25-3.40', size: 0, value: 0 },
    { title: '3.40-3.55', size: 0, value: 0 },
    { title: '3.55-3.70', size: 0, value: 0 },
    { title: '3.70-3.85', size: 0, value: 0 },
    { title: '3.85-4.00', size: 100, value: 1 },
  ]);
});

test('soft filters are applied to string vertex histogram', async () => {
  await state.left.vertexAttribute('name').setFilter('Adam,Eve,Bob');
  await state.left.vertexAttribute('age').setFilter('<40');
  await state.left.vertexAttribute('name').expectHistogramValues([
    { title: 'Adam', size: 100, value: 1 },
    { title: 'Bob', size: 0, value: 0 },
    { title: 'Eve', size: 100, value: 1 },
    { title: 'Isolated Joe', size: 0, value: 0 },
  ]);
});

test('soft filters are applied to double edge histogram', async () => {
  await state.left.edgeAttribute('weight').setFilter('!1');
  await state.left.edgeAttribute('weight').expectHistogramValues([
    { title: '1.00-1.15', size: 0, value: 0 },
    { title: '1.15-1.30', size: 0, value: 0 },
    { title: '1.30-1.45', size: 0, value: 0 },
    { title: '1.45-1.60', size: 0, value: 0 },
    { title: '1.60-1.75', size: 0, value: 0 },
    { title: '1.75-1.90', size: 0, value: 0 },
    { title: '1.90-2.05', size: 100, value: 1 },
    { title: '2.05-2.20', size: 0, value: 0 },
    { title: '2.20-2.35', size: 0, value: 0 },
    { title: '2.35-2.50', size: 0, value: 0 },
    { title: '2.50-2.65', size: 0, value: 0 },
    { title: '2.65-2.80', size: 0, value: 0 },
    { title: '2.80-2.95', size: 0, value: 0 },
    { title: '2.95-3.10', size: 0, value: 0 },
    { title: '3.10-3.25', size: 0, value: 0 },
    { title: '3.25-3.40', size: 0, value: 0 },
    { title: '3.40-3.55', size: 0, value: 0 },
    { title: '3.55-3.70', size: 0, value: 0 },
    { title: '3.70-3.85', size: 0, value: 0 },
    { title: '3.85-4.00', size: 0, value: 0 },
  ]);
});
test('precise mode histogram has precise number for large datasets', async () => {
  await workspace.addBox({
    id: 'create-vertices',
    name: 'Create vertices',
    x: 100,
    y: 100,
    params: { size: '123456' },
  });
  await workspace.addBox({
    id: 'add-attr',
    name: 'Add constant vertex attribute',
    x: 100,
    y: 200,
    after: 'create-vertices',
    params: { name: 'c' },
  });
  const state = await workspace.openStateView('add-attr', 'graph');
  await state.left.vertexAttribute('c').expectHistogramValues([
    { title: '1.00-1.00', size: 100, value: 123450 },
  ]);
  await state.left.vertexAttribute('c').expectHistogramValues([
    { title: '1.00-1.00', size: 100, value: 123456 },
  ], { precise: true });
});
