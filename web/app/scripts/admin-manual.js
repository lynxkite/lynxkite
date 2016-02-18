// Displays the admin manual.
'use strict';

angular.module('biggraph').directive('adminManualContent', function($http) {
  return {
    restrict: 'E',
    link: function(scope) {
      var html = $http.get('/admin-manual.html', { cache: true });
      html.then(function success(response) {
        /* global $ */
        var dom = $($.parseHTML(response.data));

        dom.find('a[href]').each(function(i, a) {
          a = angular.element(a);
          // Remove empty links.
          if (a.text() === '') {
            a.remove();
          } else {
            var href = a.attr('href');
            // Make cross-references relative to #/admin-manual.
            if (href[0] === '#') {
              a.attr('href', '#/admin-manual' + href);
            }
          }
        });

        scope.dom = dom;
      });

      scope.$watch('dom', function(dom) {
        if (dom !== undefined) {
          $('admin-manual-content').html(dom);
        }
      });      
    }
  };
});
