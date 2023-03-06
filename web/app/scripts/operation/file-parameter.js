// Operation parameter for kind=file. Supports file upload.
'use strict';
import '../app';
import '../util/util';
import templateUrl from './file-parameter.html?url';

angular.module('biggraph').directive('fileParameter', ['util', '$timeout', function(util, $timeout) {
  return {
    restrict: 'E',
    scope: {
      // The filename is exported through "model".
      filename: '=model',
      // Function to call on "blur".
      onBlur: '&',
    },
    templateUrl,
    link: function(scope, element) {
      const input = angular.element(element).find('input[type="file"]');
      scope.dialog = function() {
        $timeout(function() { input.click(); });
      };
      scope.uploading = false;
      scope.progress = 0;
      input.bind('change', function() {
        scope.$apply(function() {
          const file = input[0].files[0];
          input.val(null); // Unset the selection, so the same file can be picked again.
          scope.uploading = true;
          scope.progress = 0;
          const xhr = new XMLHttpRequest();
          xhr.open('POST', 'ajax/upload');
          xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) { // DONE
              scope.$apply(function() {
                if (xhr.status === 200) { // SUCCESS
                  scope.filename = xhr.responseText;
                  scope.onBlur();
                } else {
                  util.error('File upload failed.', { file: file });
                }
                scope.uploading = false;
              });
            }
          };
          xhr.upload.addEventListener('progress', function(e) {
            if (e.lengthComputable) {
              scope.$apply(function() {
                const percentage = Math.round((e.loaded * 100) / e.total);
                scope.progress = percentage;
              });
            }
          });
          const fd = new FormData();
          fd.append('file', file);
          xhr.send(fd);
        });
      });
    },
  };
}]);
