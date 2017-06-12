// A tree-view based browser for directories, projects, views,
// tables and their columns.
'use strict';

angular.module('biggraph').directive('tableBrowser', function(util) {
  return {
    restrict: 'E',
    scope: {
      directory: '=',
      projectState: '=',
    },
    templateUrl: 'scripts/sql/table-browser.html',
    link: function(scope) {
      // Create a root node. Its path is the base path in which this
      // browser is operating. (Same as the path of the SQL box.)
      if (scope.projectState) {
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

          // Fetches the list of child nodes for nodes of type
          // table, column and view.
          fetchSubProjectList: function() {
            var that = this;
            util
              .nocache(
                '/ajax/getTableBrowserNodes', {
                  'path': this.absolutePath,
                  'isImplicitTable': this.objectType === 'table'
                })
              .then(function(result) {
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
                that.error = error.data;
              });
          },

          // Fetches the list of child nodes for nodes of type
          // directory. If the string searchQuery is non-empty,
          // then the subtree is traversed recursively, and only
          // nodes with their name including query are returned.
          fetchProjectList: function(query) {
            var that = this;
            var promise;
            if (query) {
              promise = util.nocache(
                '/ajax/projectSearch',
                {
                  'basePath': this.absolutePath,
                  'query': query,
                  'includeNotes': false,
                  'filterTypes': ['snapshot'],
                });
            } else {
              promise = util.nocache(
                '/ajax/projectList',
                {
                  'path': this.absolutePath,
                  'filterTypes': ['snapshot'],
                });
            }
            promise.then(function(result) {
              that.list = [];
              var i = 0;
              for (i = 0; i < result.directories.length; ++i) {
                var path = result.directories[i];
                that.list.push(createNode(
                    that,
                    that.childPathToName(path),
                    path,
                    'directory'));
              }
              for (i = 0; i < result.objects.length; ++i) {
                var obj = result.objects[i];
                that.list.push(createNode(
                    that,
                    that.childPathToName(obj.name),
                    obj.name,
                    obj.objectType));
              }
            });
          },

          // Queries the server and populates the list
          // member of this node with its children.
          // searchQuery is optional and used for searching for
          // a subset of directories.
          fetchList: function(searchQuery) {
            if (this.objectType !== 'directory') {
              this.fetchSubProjectList();
            } else {
              this.fetchProjectList(searchQuery);
            }
          },

          // Converts the absolute path of a child node
          // to the name of that childnode. i.e. strips
          // the path of this node from the prefix.
          childPathToName: function(path) {
            if (this.absolutePath === '') {
              return path;
            } else {
              return path.substring(this.absolutePath.length + 1);
            }
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
