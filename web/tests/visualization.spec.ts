// Tests ...
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;

function normalized(positions) {
  let i, minx;
  for (i = 1; i < positions.length; ++i) {
    const p = positions[i];
    minx = (minx === undefined || p.x < minx) ? p.x : minx;
  }
  const result: any[] = [];
  for (i = 0; i < positions.length; ++i) {
    result.push({ x: positions[i].x - minx, y: positions[i].y });
  }
  return result;
}

function positions(graph) {
  const pos: any[] = [];
  for (let i = 0; i < graph.vertices.length; ++i) {
    pos.push(graph.vertices[i].pos);
  }
  return normalized(pos);
}

// Compare the coordinates with given precision. The compared coordinates
// have to match on `precision` digits. For default we use 8 digits.
function checkGraphPositions(saved, graph, precision) {
  precision = precision || 8;
  for (let i = 0; i < saved.length; ++i) {
    expect(saved[i].x).toBeCloseTo(graph[i].x, precision);
    expect(saved[i].y).toBeCloseTo(graph[i].y, precision);
  }
}


test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({
    id: 'sg0',
    name: 'Use base graph as segmentation',
    x: 100, y: 200,
    after: 'eg0',
    params: { name: 'seg' },
  });
  await workspace.addBox({
    id: 'vz0',
    name: 'Graph visualization',
    x: 100, y: 300,
    after: 'sg0',
  });
  await (await workspace.openBoxEditor('vz0')).moveTo(800, 90);
  await (await workspace.openStateView('vz0', 'visualization')).moveTo(270, 90);

});

test('sampled mode attribute visualizations', async () => {
  const expectedEdges = [
    { src: 0, dst: 1 },
    { src: 1, dst: 0 },
    { src: 2, dst: 0 },
    { src: 2, dst: 1 },
  ];
  let savedPositions;
  const DEFAULT = 'rgb(57, 188, 243)'; // Brand color.
  const LOW = 'rgb(68, 1, 84)';
  const HIGH = 'rgb(254, 232, 37)';

  const visualization = workspace.getStateView('vz0', 'visualization').visualization;
  const editor = workspace.getVisualizationEditor('vz0');
  const name = editor.left.vertexAttribute('name');
  const gender = editor.left.vertexAttribute('gender');
  const income = editor.left.vertexAttribute('income');
  const age = editor.left.vertexAttribute('age');
  const location = editor.left.vertexAttribute('location');
  const weight = editor.left.edgeAttribute('weight');
  const comment = editor.left.edgeAttribute('comment');

  // No attributes visualized.
  let graph = await visualization.graphData();
  await expect(workspace.page.locator('text=3 vertices')).toBeVisible();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.edges).toConcur([
    { color: '', label: '', width: '>2' },
    { color: '', label: '', width: '>2' },
    { color: '', label: '', width: '>2' },
    { color: '', label: '', width: '>2' },
  ]);
  await expect(graph.vertices).toConcur([
    { color: DEFAULT, icon: 'circle', label: '' },
    { color: DEFAULT, icon: 'circle', label: '' },
    { color: DEFAULT, icon: 'circle', label: '' },
  ]);
  savedPositions = positions(graph);

  await name.visualizeAs('label');
  await expect(workspace.page.locator('text=Label: name')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { label: 'Adam' },
    { label: 'Eve' },
    { label: 'Bob' },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  await gender.visualizeAs('icon');
  await expect(workspace.page.locator('text=Icon: gender')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { icon: 'male' },
    { icon: 'female' },
    { icon: 'male' },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  await income.visualizeAs('color');
  await expect(workspace.page.locator('text=Vertex Color: income')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { color: LOW },
    { color: DEFAULT },
    { color: HIGH },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  await age.visualizeAs('size');
  await expect(workspace.page.locator('text=Size: age')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { size: '<25' },
    { size: '<25' },
    { size: '>25' },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  await age.visualizeAs('opacity');
  await expect(workspace.page.locator('text=Opacity: age')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { opacity: '<0.5' },
    { opacity: '<0.5' },
    { opacity: '1' },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  await age.visualizeAs('label-size');
  await expect(workspace.page.locator('text=Label Size: age')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { labelSize: '<15' },
    { labelSize: '<15' },
    { labelSize: '>15' },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  await age.visualizeAs('label-color');
  await expect(workspace.page.locator('text=Label Color: age')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { labelColor: 'rgb(70, 21, 102)' },
    { labelColor: LOW },
    { labelColor: HIGH },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  // There is no URL attribute in the example graph. Since we only check the "href"
  // attribute anyway, any string is good enough for the test.
  await name.visualizeAs('image');
  await expect(workspace.page.locator('text=Image: name')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { image: 'Adam' },
    { image: 'Eve' },
    { image: 'Bob' },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  // Try removing some visualizations.
  await age.doNotVisualizeAs('opacity');
  await age.doNotVisualizeAs('label-size');
  await age.doNotVisualizeAs('label-color');
  await name.doNotVisualizeAs('image');
  await expect(workspace.page.locator('text=Image: name')).not.toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.vertices).toConcur([
    { opacity: '1', labelSize: '15', labelColor: 'white', image: null },
    { opacity: '1', labelSize: '15', labelColor: 'white', image: null },
    { opacity: '1', labelSize: '15', labelColor: 'white', image: null },
  ]);
  checkGraphPositions(positions(graph), savedPositions);

  // Edge attributes.
  await weight.visualizeAs('width');
  // await expect(workspace.page.locator('stroke-width')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(async () => {
    expect(graph.edges).toConcur([
      { width: '<10' },
      { width: '<10' },
      { width: '>10' },
      { width: '>10' },
    ]);
  }).toPass();
  checkGraphPositions(positions(graph), savedPositions);

  await weight.visualizeAs('edge-color');
  await expect(workspace.page.locator('text=Adam')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur(expectedEdges);
  await expect(graph.edges).toConcur([
    { color: LOW },
    { color: 'rgb(54, 93, 141)' },
    { color: 'rgb(57, 173, 122)' },
    { color: HIGH },
  ]);
  checkGraphPositions(positions(graph), savedPositions);




});

test('visualize as slider', async () => {
  const visualization = workspace.getStateView('vz0', 'visualization').visualization;
  const editor = workspace.getVisualizationEditor('vz0');
  const name = editor.left.vertexAttribute('name');
  const gender = editor.left.vertexAttribute('gender');
  const income = editor.left.vertexAttribute('income');
  const age = editor.left.vertexAttribute('age');
  const location = editor.left.vertexAttribute('location');
  const weight = editor.left.edgeAttribute('weight');
  const comment = editor.left.edgeAttribute('comment');

  await name.visualizeAs('label');
  await expect(workspace.page.locator('text=Label: name')).toBeVisible();
  await age.visualizeAs('slider');
  await expect(workspace.page.locator('text=Slider for age')).toBeVisible();
  const BLUE = 'rgb(57, 188, 243)';
  const ORANGE = 'rgb(255, 136, 0)';

  let graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: ORANGE },
    { label: 'Eve', color: ORANGE },
    { label: 'Bob', color: BLUE },
  ]);

  const slider = visualization.popup.locator('.slider');
  await slider.click();

  workspace.page.keyboard.press('Home');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: BLUE },
    { label: 'Eve', color: BLUE },
    { label: 'Bob', color: BLUE },
  ]);

  workspace.page.keyboard.press('ArrowRight');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: BLUE },
    { label: 'Eve', color: 'white' },
    { label: 'Bob', color: BLUE },
  ]);

  workspace.page.keyboard.press('ArrowRight');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: BLUE },
    { label: 'Eve', color: 'white' },
    { label: 'Bob', color: BLUE },
  ]);

  workspace.page.keyboard.press('ArrowRight');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: BLUE },
    { label: 'Eve', color: ORANGE },
    { label: 'Bob', color: BLUE },
  ]);

  workspace.page.keyboard.press('End');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: ORANGE },
    { label: 'Eve', color: ORANGE },
    { label: 'Bob', color: ORANGE },
  ]);

  workspace.page.keyboard.press('ArrowLeft');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: ORANGE },
    { label: 'Eve', color: ORANGE },
    { label: 'Bob', color: 'white' },
  ]);

  workspace.page.keyboard.press('ArrowLeft');
  graph = await visualization.graphData();
  await expect(graph.vertices).toConcur([
    { label: 'Adam', color: ORANGE },
    { label: 'Eve', color: ORANGE },
    { label: 'Bob', color: BLUE },
  ]);
});

test('bucketed mode attribute visualizations', async () => {
  const visualization = workspace.getStateView('vz0', 'visualization').visualization;
  const editor = workspace.getVisualizationEditor('vz0');
  const name = editor.left.vertexAttribute('name');
  const gender = editor.left.vertexAttribute('gender');
  const income = editor.left.vertexAttribute('income');
  const age = editor.left.vertexAttribute('age');
  const location = editor.left.vertexAttribute('location');
  const weight = editor.left.edgeAttribute('weight');
  const comment = editor.left.edgeAttribute('comment');

  await editor.left.toggleBucketedVisualization();
  await expect(workspace.page.locator('text=3 vertices')).not.toBeVisible();
  let graph = await visualization.graphData();
  await expect(graph.edges).toConcur([{ src: 0, dst: 0 }]);
  await expect(graph.vertices).toConcur([{ label: '4' }]);

  await gender.visualizeAs('x');
  await expect(visualization.popup.locator('text=gender')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur([
    { src: 0, dst: 0, width: '<10' },
    { src: 0, dst: 1, width: '>10' },
    { src: 1, dst: 0, width: '<10' },
  ]);
  await expect(graph.vertices).toConcur([
    { label: '3' },
    { label: '1' },
  ]);

  await age.visualizeAs('y');
  await expect(visualization.popup.locator('text=age')).toBeVisible();
  graph = await visualization.graphData();
  await expect(graph.edges).toConcur([
    { src: 0, dst: 2, width: '>2' },
    { src: 2, dst: 0, width: '>2' },
    { src: 3, dst: 0, width: '>2' },
    { src: 3, dst: 2, width: '>2' },
  ]);
  await expect(graph.vertices).toConcur([
    { label: '1' },
    { label: '1' },
    { label: '1' },
    { label: '1' },
  ]);
});

test('visualization for two open projects', async () => {
  const visualization = workspace.getStateView('vz0', 'visualization').visualization;
  const editor = workspace.getVisualizationEditor('vz0');
  const name = editor.left.vertexAttribute('name');
  const gender = editor.left.vertexAttribute('gender');
  const income = editor.left.vertexAttribute('income');
  const age = editor.left.vertexAttribute('age');
  const location = editor.left.vertexAttribute('location');
  const weight = editor.left.edgeAttribute('weight');
  const comment = editor.left.edgeAttribute('comment');

  function matchPos(a, b) {
    return a.x.toFixed(3) === b.x.toFixed(3) && a.y.toFixed(3) === b.y.toFixed(3);
  }

  await name.visualizeAs('label');
  let leftPositions;

  let graph = await visualization.graphData();
  leftPositions = positions(graph);
  expect(graph.vertices.length).toBe(3);

  editor.left.openSegmentation('seg');
  editor.right.toggleBucketedVisualization();
  editor.right.vertexAttribute('gender').visualizeAs('y');

  graph = await visualization.graphData();
  await expect(visualization.popup.locator('text=gender')).toBeVisible();

  const pos = positions(graph);
  for (let i = 0; i < leftPositions.length; ++i) {
    let found = false;
    for (let j = 0; j < pos.length; ++j) {
      if (matchPos(pos[j], leftPositions[i])) {
        found = true;
        break;
      }
    }
    expect(found).toBe(true);
  }

  await expect(graph.edges).toConcur([
    { src: 0, dst: 1, width: '<10' },
    { src: 0, dst: 4, width: '<10' },
    { src: 1, dst: 0, width: '<10' },
    { src: 1, dst: 3, width: '<10' },
    { src: 2, dst: 0, width: '<10' },
    { src: 2, dst: 1, width: '<10' },
    { src: 2, dst: 4, width: '<10' },
    { src: 3, dst: 4, width: '<10' },
    { src: 4, dst: 3, width: '>10' },
    { src: 4, dst: 4, width: '<10' },
  ]);
  await expect(graph.vertices).toConcur([
    { label: 'Adam' },
    { label: 'Eve' },
    { label: 'Bob' },
    { label: '1' },
    { label: '3' },
  ]);

  // Check TSV of this complex visualization.
  const expectedTSV = fs.readFileSync(__dirname + '/data/visualization-tsv-data.txt', 'utf8');
  expect(visualization.asTSV()).toEqual(expectedTSV);
});

test('visualization context menu', async () => {
  const visualization = workspace.getStateView('vz0', 'visualization').visualization;
  const editor = workspace.getVisualizationEditor('vz0');
  const name = editor.left.vertexAttribute('name');
  const gender = editor.left.vertexAttribute('gender');
  const income = editor.left.vertexAttribute('income');
  const age = editor.left.vertexAttribute('age');
  const location = editor.left.vertexAttribute('location');
  const weight = editor.left.edgeAttribute('weight');
  const comment = editor.left.edgeAttribute('comment');
});
