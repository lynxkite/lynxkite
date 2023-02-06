// Tests generating Python code from a selection of boxes.
import { test, expect, Browser } from '@playwright/test';
import { Splash, Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  const splash = await Splash.open(await browser.newPage());
  workspace = await splash.openNewWorkspace('test-python-code-generation');
  await workspace.addBox({
    id: 'cv', name: 'Create vertices', x: 100, y: 0, params: { size: '20' },
  });
  await workspace.addBox({
    id: 'ce', name: 'Create random edges', x: 300, y: 10, params: { degree: '4' }, after: 'cv',
  });
  await workspace.addBox({
    id: 'cd', name: 'Compute degree', x: 500, y: 20, after: 'ce',
  });
  await workspace.addBox({
    id: 'fba', name: 'Filter by attributes', x: 700, y: 30, params: { filterva_degree: '<4' }, after: 'cd',
  });
  await workspace.addBox({
    id: 'cpr', name: 'Compute PageRank', x: 700, y: 200, after: 'ce',
  });
  await workspace.addBox({
    id: 'sql', name: 'SQL2', x: 900, y: 100,
  });
  await workspace.connectBoxes('fba', 'graph', 'sql', 'one');
  await workspace.connectBoxes('cpr', 'graph', 'sql', 'two');
  await workspace.editBox('sql', {
    sql: `select ov.ordinal, ov.degree, tv.page_rank
from \`one.vertices\` as ov inner join \`two.vertices\` as tv
on ov.ordinal = tv.ordinal
order by ordinal` });
});

async function expectPython(expected: string) {
  await workspace.main.locator('#save-boxes-as-python').click();
  await expect(workspace.page.locator('#python-code')).toHaveText(expected.trim());
  await workspace.page.locator('#close-modal-button').click();
}


test('code generation with nothing selected', async function () {
  await workspace.selectBoxes([]);
  await expectPython(`
cv = lk.createVertices(size='20')
ce = lk.createRandomEdges(cv, degree='4')
cd = lk.computeDegree(ce)
cpr = lk.computePageRank(ce)
fba = lk.filterByAttributes(cd, filterva_degree='<4')
sql = lk.sql2(fba, cpr, sql='''select ov.ordinal, ov.degree, tv.page_rank
from \`one.vertices\` as ov inner join \`two.vertices\` as tv
on ov.ordinal = tv.ordinal
order by ordinal''')
  `);
});

test('code generation with boxes selected', async function () {
  await workspace.selectBoxes(['ce', 'cpr', 'cd']);
  await expectPython(`
ce = lk.createRandomEdges(input_graph_for_ce, degree='4')
cpr = lk.computePageRank(ce)
cd = lk.computeDegree(ce)
  `);
});
