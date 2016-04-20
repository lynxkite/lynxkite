// Loads and preprocesses a documentation page.
'use strict';

angular.module('biggraph').factory('documentation', function($http) {
  var cache = {};
  function documentation(name) {
    if (cache[name] === undefined) {
      cache[name] = load(name);
    }
    return cache[name];
  }

  function load(name) {
    var html = $http.get('/' + name + '.html', { cache: true });
    var dom = html.then(function success(response) {
      /* global $ */
      var dom = $($.parseHTML('<div><div id="whole-help">' + response.data + '</div></div>'));

      // Move heading IDs to sectionbody divs.
      var sectSelector = 'div.sect1,div.sect2,div.sect3,div.sect4,div.sect5,div.sect6';
      dom.find(sectSelector).each(function(i, div) {
        div = angular.element(div);
        var heading = div.children('[id]').first();
        div.attr('id', heading.attr('id'));
        heading.attr('id', '');
      });
      // Move anchor IDs inside <dt> to the next <dd>.
      dom.find('dt > a[id]').each(function(i, a) {
        a = angular.element(a);
        var dd = a.parent().next('dd');
        var section = a.closest(sectSelector);
        var id = section.attr('id') + '-' + a.attr('id');
        dd.attr('id', id);
        a.attr('id', '');
      });
      // Make cross-references relative to #/name. (Except in PDF mode.)
      if (location.pathname.indexOf('/pdf-') !== 0) {
        dom.find('a[href]').each(function(i, a) {
          a = angular.element(a);
          var href = a.attr('href');
          if (href[0] === '#') {
            a.attr('href', '#/' + name + href);
          }
        });
      }
      return dom;
    });
    return dom;
  }
  return documentation;
});

angular.module('biggraph').directive('documentation', function(documentation) {
  return {
    scope: { documentation: '@' },
    link: function(scope, element) {
      documentation(scope.documentation).then(function(content) {
        element.html(content);
      });
    },
  };
});
