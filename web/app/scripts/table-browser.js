// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('tableBrowser', function(util) {
  return {
    restrict: 'E',
    scope: {
      directory: '=',
      projectState: '=',
    },
    templateUrl: 'table-browser.html',
    link: function(scope) {
      var browseRootPath = scope.projectState ? scope.projectState.projectName : scope.directory;

      function createNode(item, parentNode) {
        return {
          name: item.relativePath,
          parentNode: parentNode,
          absolutePath: item.absolutePath,

          objectType: item.objectType,
          columnType: item.columnType,
          markedText: item.relativePath /* name */ +
              (item.objectType === 'column' ?
                  ' (' + item.columnType + ')' : ''),
          isOpen: false,
          list: undefined,

          getRelativePath: function() {
            var offset = 0;
            if (browseRootPath.length > 0) {
              offset = browseRootPath.length + 1;
            }
            return this.absolutePath.substring(offset);
          },

          getSQLColumnName: function(fullyQualifyNames) {
            if (!fullyQualifyNames) {
              return '`' + this.name + '`';
            } else {
              return '`' + this.parentNode.getRelativePath() +
                  '`.`' + this.name + '`';
            }
          },

          getDraggableText: function(fullyQualifyNames) {
            if (this.objectType === 'column') {
              return this.getSQLColumnName(fullyQualifyNames);
            } else {  /* if (this.objectType === 'table' ||
                this.objectType === 'view') { */
              return '`' + this.getRelativePath() + '`';
            }
          },

          allColumnsSQL: function(fullyQualifyNames) {
            if (!this.list) {
              return '';
            }
            var result = '';
            for (var i = 0; i < this.list.length; ++i) {
              result += this.list[i].getSQLColumnName(fullyQualifyNames) + ',\n';
            }
            return result;
          },

          fetchList: function(path) {
            console.log('fetchList');
            var that = this;
            util
              .nocache(
                  '/ajax/getAllTables',
                  { 'path': path })
              .then(function(result) {
                  var srcList = result.list || [];
                  that.list = [];
                  for (var i = 0; i < srcList.length; ++i) {
                    that.list[i] = createNode(srcList[i], that);
                  }
                  console.log('list fetched', that.list);
              });
          },

          toggle: function() {
            if (this.objectType === 'column') {
              return;  // no toggling for columns
            }

            if (this.isOpen) {
              this.isOpen = false;
            } else {
              this.isOpen = true;
              this.fetchList(this.absolutePath);
            }
          },

          isLoading: function() {
            console.log('isLoading ', this.isOpen, this.list === undefined);
            return this.isOpen && this.list === undefined;
          },

          isOpenAndReady: function() {
            console.log('isOpenAndReady ', this.isOpen, this.list !== undefined);
            return this.isOpen;  // && this.list !== undefined;
          },

        };
      }

      scope.node = createNode({
          absolutePath: browseRootPath,
          relativePath: ''},
          undefined);
      scope.node.toggle();
      
    }
  };
});
