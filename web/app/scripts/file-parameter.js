'use strict';

angular.module('biggraph').directive('fileParameter', function() {
  return {
    restrict: 'E',
    scope: { param: '=', model: '=', fileUploads: '=', enabled: '=' },
    templateUrl: 'file-parameter.html',
    link: function(scope, element) {
      var input = angular.element(element).find('input[type="file"]');
      scope.dialog = function() {
        input.click();
      };
      scope.$watch('filename', function(fn) {
        scope.model = fn;
      });
      scope.uploading = false;
      scope.progress = 0;
      input.bind('change', function() {
        scope.$apply(function() {
          var file = input[0].files[0];
          scope.fileUploads = scope.fileUploads || 0;
          scope.fileUploads += 1;
          scope.uploading = true;
          scope.progress = 0;
          var xhr = new XMLHttpRequest();
          xhr.open('POST', '/ajax/upload');
          xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) {
              scope.$apply(function() {
                if (xhr.status === 200) {
                  scope.filename = xhr.responseText;
                } else {
                  console.error('Upload failed.');
                }
                scope.fileUploads -= 1;
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
