// Inserts help content by ID.
'use strict';

angular.module('biggraph').directive('helpContent', function() {
  return {
    restrict: 'E',
    templateUrl: 'help-content.html',
  };
});

angular.module('biggraph').directive('helpId', function() {
  return {
    restrict: 'A',
    scope: { helpId: '=' },
    link: function(scope, element) {
      scope.$watch('helpId', function() {
        var id = scope.helpId.toLowerCase();
        var content = angular.element('help-content').find('#' + id).first();
        var tag = content[0].tagName;
        if (tag.length === 2 && tag[0] === 'H') {
          content = content.nextUntil(tag);
        }
        element.html(content.html());
      });
    }
  };
});
