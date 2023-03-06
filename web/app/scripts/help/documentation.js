// Loads and preprocesses a documentation page.
'use strict';
import $ from 'jquery';
import '../app';

angular.module('biggraph').factory('documentation', ['$http', function($http) {
  const cache = {};
  function documentation(name) {
    if (cache[name] === undefined) {
      cache[name] = load(name);
    }
    return cache[name];
  }

  function load(name) {
    const html = $http.get(name + '/index.html', { cache: true });
    const dom = html.then(function success(response) {
      const dom = $($.parseHTML(
        '<div id="help-container"><div id="whole-help">' + response.data + '</div></div>'));

      // Move TOC outside and spruce it up.
      dom.find('#toc').each(function(i, div) {
        div = angular.element(div);
        div.prepend('<find-in-page-box></find-in-page-box>');
        div.prepend('<img src="images/logo.png" id="lynxkite-logo">');
        dom.prepend(div);
      });
      // Move heading IDs to sectionbody divs.
      const sectSelector = 'div.sect1,div.sect2,div.sect3,div.sect4,div.sect5,div.sect6';
      dom.find(sectSelector).each(function(i, div) {
        div = angular.element(div);
        const heading = div.children('[id]').first();
        div.attr('id', heading.attr('id'));
        heading.attr('id', '');
      });
      // Move parameter IDs from <dt> to the next <dd>.
      dom.find('dt > span[class]').each(function(i, span) {
        span = angular.element(span);
        const dd = span.parent().next('dd');
        const section = span.closest(sectSelector);
        const id = section.attr('id') + '-' + span.attr('class').replace('p-', '');
        dd.attr('id', id);
      });
      // Make cross-references relative to #/name. (Except in PDF mode.)
      if (location.pathname.indexOf('/pdf-') !== 0) {
        dom.find('a[href]').each(function(i, a) {
          a = angular.element(a);
          const href = a.attr('href');
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
}]);

angular.module('biggraph').directive('documentation', ['documentation', '$compile', '$anchorScroll', 'util', function(documentation, $compile, $anchorScroll, util) {
  return {
    scope: { documentation: '@', title: '@' },
    link: function(scope, element) {
      documentation(scope.documentation).then(function(content) {
        element.empty();
        element.append(content);
        // Activate Angular contents.
        $compile(content)(scope.$new());
        util.scopeTitle(scope, scope.title);
        // Scroll to linked anchor on help page now that the DOM is in place.
        $anchorScroll();
      });
    },
  };
}]);
