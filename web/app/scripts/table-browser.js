// Presents the parameters for running SQL scripts.
'use strict';

function charEquals(ch1, ch2) {
  return ch1.toLowerCase() === ch2.toLowerCase();
}

function computeMatch(base, matcher) {
  if (matcher === '') {
    return ['', base];
  }
  var currentMatch = [];
  var matcherPos = 0;
  var basePos = 0;
  while (basePos < base.length) {
    var matching = '';
    while (basePos < base.length &&
           matcherPos < matcher.length &&
           charEquals(base[basePos], matcher[matcherPos])) {
      matching += base[basePos];
      matcherPos++;
      basePos++;
    }

    var nonMatching = '';
    while (basePos < base.length &&
           (matcherPos >= matcher.length ||
            !charEquals(base[basePos], matcher[matcherPos]))) {
      nonMatching += base[basePos];
      basePos++;
    }

    currentMatch.push(matching);
    currentMatch.push(nonMatching);
  }

  if (matcherPos >= matcher.length) {
    // The whole matcher was successfully consumed.
    return currentMatch;
  } else {
    return undefined;
  }
}

angular.module('biggraph').directive('tableBrowser', function(util) {
  return {
    restrict: 'E',
    scope: {
      directory: '=',
      projectState: '=',
    },
    templateUrl: 'table-browser.html',
    link: function(scope) {
      var path = scope.projectState ? scope.projectState.projectName : scope.directory;

      scope.tableResponse = util.nocache(
        '/ajax/getAllTables',
        {
          'path': path
        });
      scope.tableFilter = '';
      scope.columnFilter = '';
      scope.$watchGroup(['tableFilter', 'tableResponse.list'], function() {
        var list = scope.tableResponse.list;
        if (!list) {
          return;
        }
        var filteredList = [];
        for (var i = 0; i < list.length; ++i) {
          list[i].nameMatch = computeMatch(
              list[i].name,
              scope.tableFilter);
          if (list[i].nameMatch) {
            filteredList.push(list[i]);
          }
        }
        // Sort by the length of match arrays.
        // The sorter the array is the least fragmented the match is. (Sort of.)
        filteredList.sort(function(a, b) { return a.nameMatch.length - b.nameMatch.length; });
        scope.list = filteredList.slice(0, 50);
      });
      scope.$watchGroup(['columnFilter', 'tableResponse.list', 'openTable'], function() {
        var list = scope.tableResponse.list;
        if (!list) {
          return;
        }
        for (var i = 0; i < list.length; ++i) {
          if (scope.openTable === list[i].name) {
            scope.computeColumnMatches(list[i]);
          }
        }
      });

      scope.computeColumnMatches = function(table) {
        var columns = table.columns;
        if (columns) {
          for (var j = 0; j < columns.length; ++j) {
            columns[j].nameMatch = computeMatch(
                columns[j].name,
                scope.columnFilter);
          }
        }
      };
      scope.getColumnName = function(table, column) {
        if (scope.fullyQualifyNames) {
          return '`' + table.name + '`.`' + column.name + '`';
        } else {
          return '`' + column.name + '`';
        }
      };
      scope.allColumns = function(table) {
        return table.columns
            .map(function(column) {
                return scope.getColumnName(table, column);
            })
            .join(',\n');
      };
      scope.toggleOpen = function(table) {
        if (scope.openTable) {
          scope.openTable = undefined;
        } else {
          console.log('GETCOLUMNS', table);
          util.nocache(
            '/ajax/getColumns',
            {
              framePath: table.framePath,
              subTablePath: table.subTablePath
            }
          ).then(function(res) {
            table.columns = res.columns;
            scope.openTable = table.name;
          });
        }
      };

    }
  };
});
