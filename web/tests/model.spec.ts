// Tests machine learning boxes.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

test('ML with Spark', async ({ page }) => {
  const ws = await Workspace.empty(page, 'model');
  // Create a graph.
  await ws.addBox({
    id: 'create', name: 'Create graph in Python', x: 100, y: 100, params: {
      outputs: 'vs.age: float, vs.yob: float',
      code: `
    vs = pd.DataFrame({"age": [25, 40, 60, 35, 50], "yob": [1990, 1975, 1955, 1980, 1965]})
    `.trim(),
    }
  });
  let state = await ws.openStateView('create', 'graph');
  await expect(state.left.vertexCount).toHaveText('5');
  await state.close();

  // Train a model.
  await ws.addBox({
    id: 'train', name: 'Train linear regression model',
    x: 100, y: 200, after: 'create', params: {
      name: 'age_from_yob',
      label: 'age',
      features: ['yob'],
    },
  });
  state = await ws.openStateView('train', 'graph');
  const model = state.left.graphAttribute('age_from_yob');
  await expect(model.value).toHaveText('Linear regression model predicting age');
  const p = await model.popup();
  await expect(p.locator('#model-method')).toHaveText('Linear regression');
  await expect(p.locator('#model-label')).toHaveText('age');
  await expect(p.locator('#model-features')).toHaveText('yob');
  await expect(p.locator('#model-details')).toHaveText(/intercept\s*2015/);
  await model.popoff();
  await state.close();

  // Create predictions.
  await ws.addBox({
    id: 'predict', name: 'Predict with model',
    x: 100, y: 300, after: 'train', params: {
      name: 'age_prediction',
    },
  });
  // Convert the predictions to a more convenient format to test.
  await ws.addBox({
    id: 'to-int', name: 'Derive vertex attribute',
    x: 100, y: 400, after: 'predict', params: {
      output: 'age_prediction_string',
      expr: 'age_prediction.toInt.toString',
    },
  });
  state = await ws.openStateView('to-int', 'graph');
  const attr = state.left.vertexAttribute('age_prediction_string');
  await attr.expectHistogramValues([
    { title: '25', size: 100, value: 1 },
    { title: '35', size: 100, value: 1 },
    { title: '40', size: 100, value: 1 },
    { title: '49', size: 100, value: 1 },
    { title: '59', size: 100, value: 1 }
  ]);
});
