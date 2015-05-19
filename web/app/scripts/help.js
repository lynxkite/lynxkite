// Displays help content by ID.
'use strict';

// Loads and preprocesses the help pages.
angular.module('biggraph').factory('helpContent', function($http) {
  var html = $http.get('/help.html', { cache: true });
  var dom = html.then(function success(response) {
    /* global $ */
    var dom = $($.parseHTML('<div><div id="whole-help">' + response.data + '</div></div>'));

    // Move heading IDs to sectionbody divs.
    dom.find('div.sect1,div.sect2,div.sect3,div.sect4,div.sect5,div.sect6').each(function(i, div) {
      div = angular.element(div);
      var heading = div.children('[id]').first();
      div.attr('id', heading.attr('id'));
      heading.attr('id', '');
    });
    // Move anchor IDs inside <dt> to the next <dd>.
    dom.find('dt > a[id]').each(function(i, a) {
      a = angular.element(a);
      var dd = a.parent().next('dd');
      var section = a.closest('div.sect1,div.sect2,div.sect3,div.sect4,div.sect5,div.sect6');
      var id = section.attr('id') + '-' + a.attr('id');
      dd.attr('id', id);
    });
    // Make cross-references relative to #/help.
    dom.find('a[href]').each(function(i, a) {
      a = angular.element(a);
      var href = a.attr('href');
      if (href[0] === '#') {
        a.attr('href', '#/help' + href);
      }
    });
    return dom;
  });
  return dom;
});

// Finds a snippet from the help pages by its ID. Replaces the first <hr> with a "read more" link.
angular.module('biggraph').directive('helpId',
    function(helpContent, $compile, $anchorScroll, util) {
  return {
    restrict: 'A',
    scope: { helpId: '@', removeHeader: '@' },
    link: function(scope, element) {
      element.addClass('help');

      helpContent.then(function(helpContent) {
        var id = scope.helpId.toLowerCase();
        var content = helpContent.find('#' + id).first();
        if (content.length === 0) {
          console.warn('Could not find help ID', id);
        }
        content = content.clone();
        if (scope.removeHeader === 'yes') {
          content.find('h1,h2,h3,h4,h5,h6').first().remove();
        }
        if (id === 'whole-help') {
          // Set title in single-page mode.
          util.scopeTitle(scope, '"LynxKite User Manual"');
        } else {
          // Help links inside the main UI should open in new tabs.
          content.find('a[href]').each(function(i, a) {
            a.setAttribute('target', '_blank');
          });
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
        // Activate Angular contents.
        $compile(content)(scope.$new());
        if (id === 'whole-help') {
          // Scroll to linked anchor on help page now that the DOM is in place.
          $anchorScroll();
        }
      });
    }
  };
});

// A button that displays a help snippet on hover/click.
angular.module('biggraph').directive('helpPopup', function($rootScope) {
  return {
    restrict: 'E',
    scope: { helpId: '@href' },
    templateUrl: 'help-popup.html',
    link: function(scope, element) {
      var body = angular.element('body');
      var uiLayout = angular.element('.kite-top');
      var button = element.find('#help-button');
      var popup = element.find('#help-popup');
      popup.hide();
      scope.isEmpty = function() {
        return popup.children().length === 0;
      };
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
        if (popup.is(':visible')) { return; }
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
        // We don't allow large popups upwards as the user won't find the start of the text.
        var topPopupHeight = Math.min(250, buttonTop - 10);
        if (fullHeight - buttonTop - buttonHeight - 20 < topPopupHeight) {
          // Very little room below, we put it above.
          popup.css('top', (buttonTop - topPopupHeight - 10) + 'px');
          popup.css('height', topPopupHeight + 'px');
        } else {
          // We have enough room below.
          var popupTop = buttonTop + buttonHeight + 10;
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
