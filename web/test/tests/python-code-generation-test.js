'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {

  fw.transitionTest(
    'empty splash',
    'boxes placed',
    function() {
      lib.splash.openNewWorkspace('test-python-code-generation');
      lib.workspace.addBox({
        id: 'cv', name: 'Create vertices', x: 100, y: 0, params: { size: '20' } });
      lib.workspace.addBox({
        id: 'ce', name: 'Create random edges', x: 250, y: 0, params: { degree: '4' }, after: 'cv' });
      lib.workspace.addBox({
        id: 'cd', name: 'Compute degree', x: 400, y: 0, after: 'ce' });
      lib.workspace.addBox({
        id: 'fba', name: 'Filter by attributes', x: 550, y: 0, params: { filterva_degree: '<4' }, after: 'cd' });
      lib.workspace.addBox({
        id: 'cpr', name: 'Compute PageRank', x: 600, y: 200, after: 'ce'
      });
      lib.workspace.addBox({
        id: 'sql', name: 'SQL2', x: 700, y: 100 });
      lib.workspace.connectBoxes('fba', 'graph', 'sql', 'one');
      lib.workspace.connectBoxes('cpr', 'graph', 'sql', 'two');
      lib.workspace.editBox('sql', { sql: `select ov.ordinal, ov.degree, tv.page_rank
from \`one.vertices\` as ov inner join \`two.vertices\` as tv
on ov.ordinal = tv.ordinal
order by ordinal` });
    },
    function() {
      // unselect all
      lib.workspace.selectBoxes([]);
    });

  fw.transitionTest(
    'boxes placed',
    'code generated without selecting anything',
    function() {
      lib.pythonPopup();
    },
    function() {
      let expectedCode = `cv = lk.createVertices(size='20')
ce = lk.createRandomEdges(cv, degree='4')
cd = lk.computeDegree(ce)
cpr = lk.computePageRank(ce)
fba = lk.filterByAttributes(cd, filterva_degree='<4')
sql = lk.sql2(fba, cpr, sql='''select ov.ordinal, ov.degree, tv.page_rank
from \`one.vertices\` as ov inner join \`two.vertices\` as tv
on ov.ordinal = tv.ordinal
order by ordinal''')`;
      lib.expectPythonCode(expectedCode);
    });

  fw.transitionTest(
    'boxes placed',
    'code generated from selected boxes',
    function() {
      lib.workspace.selectBoxes(['ce', 'cpr', 'cd']);
      lib.pythonPopup();
    },
    function() {
      let expectedCode = `ce = lk.createRandomEdges(input_project_for_ce, degree='4')
cpr = lk.computePageRank(ce)
cd = lk.computeDegree(ce)`;
      lib.expectPythonCode(expectedCode);
    });
};
