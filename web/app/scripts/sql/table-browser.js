// A tree-view based browser for directories, projects, views,
// tables and their columns.
'use strict';

angular.module('biggraph').directive('tableBrowser', function(util) {
  return {
    restrict: 'E',
    scope: {
      directory: '=',
      projectState: '=',
      box: '=',  // Set box for table browser in the workspace.
    },
    templateUrl: 'scripts/sql/table-browser.html',
    link: function(scope) {
      // Create a root node. Its path is the base path in which this
      // browser is operating. (Same as the path of the SQL box.)
      if (scope.box) {
        scope.node = createNode(
            undefined,
            '',
            '',
            'directory');
      } else if (scope.projectState) {
        scope.node = createNode(
            undefined,
            '',
            scope.projectState.projectName,
            'project');
      } else {
        scope.node = createNode(
            undefined,
            '',
            scope.directory,
            'directory');
      }
      // Trigger loading it's children and open it.
      scope.node.toggle();

      scope.$watch('searchQuery', function() {
        scope.node.fetchList(scope.searchQuery);
      });
      // Creates and returns a JS object representing a node in
      // the treeview.
      function createNode(
          parentNode,
          name,
          absolutePath,
          objectType,
          columnType) {
        return {
          name: name,
          parentNode: parentNode,
          absolutePath: absolutePath,
          objectType: objectType,
          columnType: columnType,

          // User-visible text:
          uiText: name +
              (objectType === 'column' ?
                  ' (' + columnType + ')' : ''),
          // Open/close status of node in the tree.
          isOpen: false,
          // List of children nodes.
          list: undefined,

          // Get path relative to the path of the treenode's
          // root node.
          getRelativePath: function() {
            var browseRootPath = scope.node.absolutePath;
            var offset = 0;
            if (browseRootPath.length > 0) {
              offset = browseRootPath.length + 1;
            }
            return this.absolutePath.substring(offset);
          },

          // SQL-compatible column name (if this node is a column).
          getSQLColumnName: function(fullyQualifyNames) {
            if (!fullyQualifyNames) {
              return '`' + this.name + '`';
            } else {
              return '`' + this.parentNode.getRelativePath() +
                  '`.`' + this.name + '`';
            }
          },

          // The text that can be dragged into the SQL editor.
          getDraggableText: function(fullyQualifyNames) {
            if (this.objectType === 'column') {
              return this.getSQLColumnName(fullyQualifyNames);
            } else {  /* if (this.objectType === 'table' ||
                this.objectType === 'view') { */
              return '`' + this.getRelativePath() + '`';
            }
          },

          // Returns a list of all the child column names,
          // in SQL-compatible text format.
          allColumnsSQL: function(fullyQualifyNames) {
            if (!this.list) {
              return '';
            }
            var result = '';
            for (var i = 0; i < this.list.length; ++i) {
              if (result !== '') {
                result += ',\n';
              }
              result += this.list[i].getSQLColumnName(fullyQualifyNames);
            }
            return result;
          },

          // Queries the server and populates the list
          // member of this node with its children.
          // searchQuery is optional and used for searching for
          // a subset of directories.
          fetchList: function(searchQuery) {
            var that = this;
            var promise;
            if (scope.box) {
              promise = util.nocache(
                '/ajax/getTableBrowserNodesForBox', {
                  operationRequest: {
                    'workspace': scope.box.workspace.ref(),
                    'box': scope.box.instance.id
                  },
                  path: this.absolutePath
                });
            } else {
              promise = util.nocache(
                '/ajax/getTableBrowserNodes', {
                  'path': this.absolutePath,
                  'query': searchQuery,
                  'isImplicitTable': this.objectType === 'table'
                });
            }
            promise.then(function(result) {
              var srcList = result.list || [];
              that.list = [];
              for (var i = 0; i < srcList.length; ++i) {
                that.list[i] = createNode(
                  that,
                  srcList[i].name,
                  srcList[i].absolutePath,
                  srcList[i].objectType,
                  srcList[i].columnType);
              }
            }, function(error) {
              that.error = util.responseToErrorMessage(error);
            });
          },

          // Opens this node in the tree view. When invoked for
          // the first time, the children are also fetched.
          toggle: function() {
            if (this.objectType === 'column') {
              return;  // no toggling for columns
            }
            if (this.isOpen) {
              this.isOpen = false;
            } else {
              this.isOpen = true;
              if (this.list === undefined) {
                this.fetchList();
              }
            }
          },
        };
      }
    }
  };
});
