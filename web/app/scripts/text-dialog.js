// A button that offers a text input when clicked.
'use strict';

angular.module('biggraph').directive('textDialog', function($timeout) {
  return {
    restrict: 'E',
    scope: { buttonText: '@', onOk: '&', startValue: '@', help: '@' },
    templateUrl: 'text-dialog.html',
    link: function(scope, element) {
      var inputBox = element.find('#dialogInput');
      function openDialog() {
        scope.dialogVisible = true;
        scope.dialogEnabled = true;
        $timeout(function() {
          inputBox.focus();
          inputBox.select();
        });
      }
      function finishedCallback(success) {
        if (success) {
          scope.dialogVisible = false;
        } else {
          openDialog();
        }
      }
      scope.okClick = function() {
        if (scope.dialogVisible) {
          scope.dialogEnabled = false;
          scope.onOk({ userText: scope.dialogText, finishedCallback: finishedCallback });
        } else {
          scope.dialogText = scope.startValue;
          openDialog();
        }
      };
    },
  };
});
