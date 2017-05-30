// Operation parameter for kind=file. Supports file upload.
'use strict';

angular.module('biggraph').directive('fileParameter', function(util, $timeout) {
  return {
    restrict: 'E',
    scope: {
      // The filename is exported through "model".
      filename: '=model',
      // The number of ongoing uploads. It is incremented by 1 while an upload is in progress.
      fileUploadCount: '=',
      // Enable/disable the control.
      editable: '=',
      // Function to call on "blur".
      onBlur: '&',
    },
    templateUrl: 'scripts/operation/file-parameter.html',
    link: function(scope, element) {
      var input = angular.element(element).find('input[type="file"]');
      scope.dialog = function() {
        $timeout(function() { input.click(); });
      };
      scope.uploading = false;
      scope.progress = 0;
      input.bind('change', function() {
        scope.$apply(function() {
          var file = input[0].files[0];
          input.val(null);  // Unset the selection, so the same file can be picked again.
          scope.fileUploadCount = scope.fileUploadCount || 0;
          scope.fileUploadCount += 1;
          scope.uploading = true;
          scope.progress = 0;
          var xhr = new XMLHttpRequest();
          xhr.open('POST', '/ajax/upload');
          xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) {  // DONE
              scope.$apply(function() {
                if (xhr.status === 200) {  // SUCCESS
                  scope.filename = xhr.responseText;
                  scope.onBlur();
                } else {
                  util.error('File upload failed.', { file: file });
                }
                scope.fileUploadCount -= 1;
                scope.uploading = false;
              });
            }
          };
          xhr.upload.addEventListener('progress', function(e) {
            if (e.lengthComputable) {
              scope.$apply(function() {
                var percentage = Math.round((e.loaded * 100) / e.total);
                scope.progress = percentage;
              });
            }
          });
          var fd = new FormData();
          fd.append('file', file);
          xhr.send(fd);
        });
      });
    },
  };
});
