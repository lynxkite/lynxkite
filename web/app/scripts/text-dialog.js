'use strict';

angular.module('biggraph').directive('textDialog', function($timeout) {
  return {
    restrict: 'E',
    scope: { buttonText: '@', onOk: '&', startValue: '@' },
    templateUrl: 'text-dialog.html',
    link: function(scope, element) {
      scope.okClick = function() {
        console.log(scope);
        if (scope.dialogVisible) {
          scope.onOk({ userText: scope.dialogText });
          scope.dialogVisible = false;
        } else {
          scope.dialogVisible = true;
          scope.dialogText = scope.startValue;
          $timeout(function() {
            var inputBox = element.find('#dialogInput');
            inputBox.focus();
            inputBox.select();
          });
        }
      };
    },
  };
});
