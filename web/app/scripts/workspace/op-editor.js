// Editor of operation parameters.
import '../app';
import templateUrl from './op-editor.html?url';
import MarkdownIt from 'markdown-it';
const md = new MarkdownIt();

angular.module('biggraph')
  .directive('opEditor', function() {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        box: '=?',
        boxMeta: '=',
        parameters: '=',
        parametricParameters: '=',
        workspace: '=',
        wizard: '=',
        halfSize: '=?',
        onBlur: '&?',
      },
      link: function(scope) {
        scope.$watch('boxMeta.description', function() {
          if (scope.boxMeta?.description) {
            scope.descriptionMarkdown = md.render(scope.boxMeta.description);
          }
        });
      },
    };
  });
