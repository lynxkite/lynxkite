// Presents the parameters for running SQL scripts.
'use strict';

var MAX_ENTRIES_TO_SHOW = 300;

function charEquals(ch1, ch2) {
  return ch1.toLowerCase() === ch2.toLowerCase();
}

/**
  Computes wether matcher is a non-contiguous substring of base.
  If it is, then the result is an array of alternating matching
  and non-matching portions of base. Example:

  // There is no I in team:
  computeMatch('team', 'I') = []
  // 'burg' matches 'hamburger'
  computeMatch('hamburger', 'burg') = ['', 'ham', 'burg', 'er']
  // 'apple' matches 'apple'
  computeMatch('apple', 'apple') = ['apple', '']
  // '' matches 'skildpadder'
  computeMatch('', 'skildpadder') = ['', 'skildpadder']
 */
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

      // Returns a table object that can be used in the template for
      // rendering and event handling.
      function createTable(tableData) {
        return {
          absolutePath: tableData.absolutePath,
          relativePath: tableData.relativePath,

          computeColumnMatches: function() {
            if (this.columns) {
              for (var j = 0; j < this.columns.length; ++j) {
                this.columns[j].nameMatch = computeMatch(
                    this.columns[j].name,
                    this.columnFilter);
              }
            }
          },

          computeNameMatch: function(tableFilter) {
            this.nameMatch = computeMatch(this.relativePath, tableFilter);
          },

          getSqlColumnName: function(column, fullyQualifyNames) {
            if (fullyQualifyNames) {
              return '`' + this.relativePath + '`.`' + column.name + '`';
            } else {
              return '`' + column.name + '`';
            }
          },

          allColumnsSQL: function(fullyQualifyNames) {
            var that = this;
            return this.columns
                .map(function(column) {
                    return that.getSqlColumnName(column, fullyQualifyNames);
                })
                .join(',\n');
          },

          toggleOpen: function() {
            var that = this;
            if (that.isOpen) {
              that.isOpen = false;
              that.deregisterWatch();
            } else {
              util.nocache(
                '/ajax/getColumns',
                {
                  absolutePath: that.absolutePath
                }
              ).then(function(res) {
                that.columns = res.columns;
                that.isOpen = true;
                that.columnFilter = '';
                that.deregisterWatch = scope.$watch(
                  function() {  // What to watch for:
                    return that.columnFilter;
                  },
                  function() {  // What to do when change is detected:
                    that.computeColumnMatches();
                  });
              });
            }
          },

        };
      }

      var path = scope.projectState ? scope.projectState.projectName : scope.directory;
      scope.tableResponse = util.nocache(
        '/ajax/getAllTables',
        {
          'path': path
        });
      scope.tableFilter = '';
      scope.$watchGroup(['tableFilter', 'tableResponse.list'], function() {
        var list = scope.tableResponse.list;
        if (!list) {
          return;
        }
        var filteredList = [];
        for (var i = 0; i < list.length; ++i) {
          var table = createTable(list[i]);
          table.computeNameMatch(scope.tableFilter);
          if (table.nameMatch) {
            filteredList.push(table);
          }
        }
        // Sort by the length of match arrays. This estimates the fragmentedness of matches,
        // and moves the non-fragmented results early in the list.
        filteredList.sort(function(a, b) { return a.nameMatch.length - b.nameMatch.length; });
        scope.list = filteredList.slice(0, MAX_ENTRIES_TO_SHOW);
      });

    }
  };
});
