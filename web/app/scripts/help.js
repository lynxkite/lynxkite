// Displays help content by ID.
'use strict';

// Loads and preprocesses the help pages.
angular.module('biggraph').directive('helpContent', function() {
  return {
    restrict: 'E',
    templateUrl: 'help-content.html',
    link: function(scope, element) {
      scope.onload = function() {
        // Move heading IDs to sectionbody divs.
        element
          .find('div.sect1,div.sect2,div.sect3,div.sect4,div.sect5,div.sect6')
          .each(function(i, div) {
          div = angular.element(div);
          var heading = div.children('[id]').first();
          div.attr('id', heading.attr('id'));
          heading.attr('id', '');
        });
        // Move anchor IDs inside <dt> to the next <dd>.
        element.find('dt > a[id]').each(function(i, a) {
          a = angular.element(a);
          var dd = a.parent().next('dd');
          var section = a.closest('div.sect1,div.sect2,div.sect3,div.sect4,div.sect5,div.sect6');
          var id = section.attr('id') + '-' + a.attr('id');
          dd.attr('id', id);
        });
      };
    },
  };
});

// Finds a snippet from the help pages by its ID. Replaces the first <hr> with a "read more" link.
angular.module('biggraph').directive('helpId', function() {
  return {
    restrict: 'A',
    scope: { helpId: '=', removeHeader: '@' },
    link: function(scope, element) {
      element.addClass('help');

      scope.$watch('helpId', function() {
        var id = scope.helpId.toLowerCase();
        var content = angular.element('help-content').find('#' + id).first();
        content = content.clone();
        if (scope.removeHeader === 'yes') {
          content.find('h1,h2,h3,h4,h5,h6').first().remove();
        }
        function expander(e, what) {
          return function() {
            e.hide();
            what.show();
          };
        }
        var details = content.children('.exampleblock');
        var readMore = angular.element('<a class="read-more">read more</a>');
        readMore.click(expander(readMore, details));
        details.before(readMore);
        details.hide();
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
      var uiLayout = angular.element('ui-layout');
      var button = element.find('#help-button');
      var popup = element.find('#help-popup');
      popup.hide();
      var sticky = false;

      scope.$on('$destroy', function() {
        popup.remove();
      });
      scope.$on('help popup opened', function(e, source) {
        if (scope !== source) { // Another popup opened. Hide ourselves.
          popup.hide();
          sticky = false;
        }
      });

      function show() {
        $rootScope.$broadcast('help popup opened', scope);
        // Add the popup at the end of <body> to make sure it's above everything.
        body.append(popup.detach());
        popup.show();
        var offset = button.offset();
        var buttonLeft = offset.left;
        var buttonTop = offset.top;
        var buttonHeight = button.height();

        var popupLeft = buttonLeft - popup.width() / 2;
        var maxLeft = uiLayout.width() - popup.width() - 30;
        if (popupLeft > maxLeft) {
          popupLeft = maxLeft;
        }
        if (popupLeft < 0) {
          popupLeft = 0;
        }
        var fullHeight = uiLayout.height();

        popup.css('left', popupLeft + 'px');
        if (buttonTop > fullHeight - buttonTop - buttonHeight) {
          popup.css('top', 'auto');
          popup.css('bottom', (fullHeight - buttonTop + 10) + 'px');
          popup.css('max-height', (offset.top - 20) + 'px');
        } else {
          // More room below.
          var popupTop = buttonTop + buttonHeight + 10;
          popup.css('bottom', 'auto');
          popup.css('top', popupTop + 'px');
          popup.css('max-height', (fullHeight - popupTop - 10) + 'px');
        }
      }

      scope.on = function() {
        show();
      };
      scope.off = function() {
        if (!sticky) { popup.hide(); }
      };
      scope.toggle = function() {
        sticky = !sticky;
        if (sticky) { show(); }
      };
    }
  };
});
