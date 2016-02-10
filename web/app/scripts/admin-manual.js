// Displays the admin manual.
'use strict';

angular.module('biggraph').directive('adminManualContent', function($http) {
  return {
    restrict: 'E',
    link: function(scope) {
      var html = $http.get('/adminManual.html', { cache: true });
      html.then(function success(response) {
        /* global $ */
        var dom = $($.parseHTML(response.data));

        // Make cross-references relative to #/adminManual.
        dom.find('a[href]').each(function(i, a) {
          a = angular.element(a);
          var href = a.attr('href');
          if (href[0] === '#') {
            a.attr('href', '#/adminManual' + href);
          }
        });

        // Remove empty links.
        dom.find('a[href]').each(function(i, a) {
          a = angular.element(a);
          if (a.text() === '') {
            a.remove();
          }
        });
        scope.dom = dom;
      });

      scope.$watch('dom', function(dom) {
        if (dom !== undefined) {
          $('#admin-manual-content').html(dom);
        }
      });      
    }
  };
});
