// Loads and preprocesses a documentation page.
'use strict';

angular.module('biggraph').factory('documentation', function($http) {
  const cache = {};
  function documentation(name) {
    if (cache[name] === undefined) {
      cache[name] = load(name);
    }
    return cache[name];
  }

  function load(name) {
    const html = $http.get('/' + name + '.html', { cache: true });
    const dom = html.then(function success(response) {
      /* global $ */
      const dom = $($.parseHTML(
            '<div id="help-container"><div id="whole-help">' + response.data + '</div></div>'));

      // Move TOC outside and spruce it up.
      dom.find('#toc').each(function(i, div) {
        div = angular.element(div);
        div.prepend('<find-in-page-box></find-in-page-box>');
        div.prepend('<img src="/images/logo.png" id="lynxkite-logo">');
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
      // Move anchor IDs inside <dt> to the next <dd>.
      dom.find('dt > a[id]').each(function(i, a) {
        a = angular.element(a);
        const dd = a.parent().next('dd');
        const section = a.closest(sectSelector);
        const id = section.attr('id') + '-' + a.attr('id');
        dd.attr('id', id);
        a.attr('id', '');
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
});

angular.module('biggraph').directive('documentation', function(documentation, $compile) {
  return {
    scope: { documentation: '@' },
    link: function(scope, element) {
      documentation(scope.documentation).then(function(content) {
        element.empty();
        element.append(content);
        // Activate Angular contents.
        $compile(content)(scope.$new());
      });
    },
  };
});
