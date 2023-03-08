// Tests for table behavior, not including SQL queries.
import { expect, test } from '@playwright/test';
import { Workspace } from './lynxkite';
import { resolve } from 'path';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({
    id: 'ib0',
    name: 'Import CSV',
    x: 100, y: 100,
    params: { filename: resolve('tests/data/import_large_csv_test.csv') },
  });
  const editor = await workspace.openBoxEditor('ib0');
  await editor.loadImportedTable();
  await editor.close();
});

test('sorting and "show more rows" are working on table state view', async () => {
  const state = await workspace.openStateView('ib0', 'table');
  const table = state.table;
  await table.expectColumnNamesAre(['country', 'country_code', 'population', 'area', 'currency']);
  await table.expectColumnTypesAre(['String', 'String', 'String', 'String', 'String']);
  await expect(table.rows()).toHaveCount(10);
  await expect(table.row(0)).toHaveText(['Afghanistan', 'AF', '33332025', '652230', 'AFN']);
  await table.clickColumn('population');
  await expect(table.row(0)).toHaveText(['Antarctica', 'AQ', '0', '14000000', 'null']);
  await table.clickColumn('country');
  await table.clickColumn('country');
  await expect(table.row(0)).toHaveText(['Åland Islands', 'AX', '29013', '1580', 'EUR']);
  await table.clickShowMoreRows();
  await expect(table.rows()).toHaveCount(20);
  await table.clickShowMoreRows();
  await expect(table.rows()).toHaveCount(30);
  await table.setRowCount(7);
  await table.clickShowSample();
  await expect(table.rows()).toHaveCount(7);
  await state.close();
});

test('editing imported CSV configuration is possible', async () => {
  const editor = await workspace.openBoxEditor('ib0');
  await editor.openGroup('Advanced settings');
  await editor.populateOperation({ infer: 'yes', imported_columns: 'area, country' });
  await editor.loadImportedTable();
  await editor.close();
  const state = await workspace.openStateView('ib0', 'table');
  await state.table.expectColumnNamesAre(['area', 'country']);
  await state.table.expectColumnTypesAre(['Int', 'String']);
  await state.close();
});

test('sqlite file imported via JDBC as table', async () => {
  await workspace.addBox({
    id: 'ijdbc',
    name: 'Import JDBC',
    x: 100, y: 200,
    params: {
      jdbc_url: 'jdbc:sqlite:' + resolve('tests/data/import_jdbc_test.sqlite'),
      jdbc_table: 'table1',
      key_column: 'a',
    },
  });
  const editor = await workspace.openBoxEditor('ijdbc');
  await editor.loadImportedTable();
  await editor.close();
  const state = await workspace.openStateView('ijdbc', 'table');
  await state.table.expect([], [], []);
});
