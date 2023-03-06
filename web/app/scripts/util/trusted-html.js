// Renders dynamic content that may contain Angular directives.
'use strict';
import '../app';

angular.module('biggraph').directive('trustedHtml', ["$compile", function($compile) {
  return {
    restrict: 'A',
    scope: { trustedHtml: '=' },
    link: function(scope, element) {
      scope.$watch('trustedHtml', function(contents) {
        element.html(contents);
        $compile(element.contents())(scope);
      });
    },
  };
}]);
