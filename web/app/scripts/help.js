// Displays help content by ID.
'use strict';

// Loads and preprocesses the help pages.
angular.module('biggraph').directive('helpContent', function() {
  return {
    restrict: 'E',
    templateUrl: 'help-content.html',
    link: function(scope, element) {
      scope.onload = function() {
        var lastId;
        // Expand anchor IDs. <a name="X"> inside a section on Y (<h2 id="Y">) will get id="Y-X".
        // This is used for easy definition and reference of operation parameters.
        element.find('*').each(function(i, e) {
          e = angular.element(e);
          if (e.is(headersUntil(6))) {
            lastId = e.attr('id') || lastId;
          }
          if (e.is('span[name]') && !e.attr('id')) {
            e.attr('id', lastId + '-' + e.attr('name').toLowerCase());
          }
        });
      };
    },
  };
});

// Returns the list of header tags (h1, h2, ...) until and including hn.
function headersUntil(n) {
  var list = [];
  for (var i = 1; i <= n; ++i) {
    list.push('h' + i);
  }
  return list.join(',');
}

// Returns the header level of a jQuery element.
function headerLevel(e) {
  if (e.is(headersUntil(6))) {
    return parseInt(e[0].tagName[1]);
  } else {
    return undefined;
  }
}

// Finds a snippet from the help pages by its ID. Replaces the first <hr> with a "read more" link.
angular.module('biggraph').directive('helpId', function() {
  return {
    restrict: 'A',
    scope: { helpId: '=' },
    link: function(scope, element) {
      element.addClass('help');

      scope.$watch('helpId', function() {
        var id = scope.helpId.toLowerCase();
        var content = angular.element('help-content').find('#' + id).first();
        if (content.is(headersUntil(6))) {
          // A header. Take all content until the next header on the same level or higher.
          content = content.nextUntil(headersUntil(headerLevel(content)));
        }
        if (content.is('[name]')) {
          // An anchor. Take the <li> if this is inside a <li>, otherwise take the parent.
          var li = content.closest('li');
          if (li.length === 1) {
            content = li.contents();
          } else {
            content = content.parent().contents();
          }
        }
        content = content.clone();
        function expander(e, what) {
          return function() {
            e.hide();
            what.show();
          };
        }
        for (var i = 0; i < content.length; ++i) {
          if (content[i].tagName === 'HR') {
            var hr = content.slice(i, i + 1);
            hr.text('read more');
            hr.addClass('read-more');
            var rest = content.slice(i + 1);
            rest.hide();
            hr.click(expander(hr, rest));
          }
        }
        element.empty();
        element.append(content);
      });
    }
  };
});

// A button that displays a help snippet on hover/click.
angular.module('biggraph').directive('helpPopup', function($rootScope) {
  return {
    restrict: 'E',
    scope: { helpId: '=href' },
    templateUrl: 'help-popup.html',
    link: function(scope, element) {
      var body = angular.element('body');
      var button = element.find('#help-button');
      var popup = element.find('#help-popup');
      popup.hide();
      var showing = false;

      scope.$on('$destroy', function() {
        popup.remove();
      });
      scope.$on('help popup opened', function(e, source) {
        if (scope !== source) { // Another popup opened. Hide ourselves.
          popup.hide();
        }
      });

      function show() {
        $rootScope.$broadcast('help popup opened', scope);
        // Add the popup at the end of <body> to make sure it's above everything.
        body.append(popup.detach());
        popup.show();
        var offset = button.offset();
        offset.top += button.height() + 10;
        offset.left -= popup.width() / 2;
        popup.offset(offset);
      }

      scope.on = function() {
        show();
      };
      scope.off = function() {
        if (!showing) { popup.hide(); }
      };
      scope.toggle = function() {
        showing = !showing;
        if (showing) { show(); }
      };
    }
  };
});
