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
      a.attr('id', '');
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
    scope: {
      helpId: '@',
      removeHeader: '@',
      scalars: '=',
    },
    link: function(scope, element) {
      function isUserWorkflowId(id) {
        return id.match('^workflows/');
      }
      element.addClass('help');
      helpContent.then(function(helpContent) {
        var id = scope.helpId.toLowerCase();
        if (isUserWorkflowId(id)) {
          element.empty();
          return;
        }
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
          // Collapse sections marked with ====.
          var details = content.find('.exampleblock');
          var readMore = angular.element('<a class="read-more">read more</a>');
          readMore.click(expander(readMore, details));
          details.before(readMore);
          details.hide();
        }
        function expander(e, what) {
          return function() {
            e.hide();
            what.show();
          };
        }

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
angular.module('biggraph')
  .directive('helpPopup', function(dropTooltipConfig) {

  return {
    restrict: 'E',
    scope: { helpId: '@href' },
    templateUrl: 'help-popup.html',
    link: function(scope, element) {
      var button = element.find('#help-button')[0];
      var popup = element.find('#help-popup')[0];
      /* global Drop */
      var drop = new Drop({
        target: button,
        content: popup,
        position: 'bottom center',
        classes: 'drop-theme-help-popup',
        openOn: dropTooltipConfig.enabled ? 'hover' : undefined,
        remove: true,
        tetherOptions: {
          // Keep within the page.
          constraints: [{
            to: 'window',
            pin: true,
            attachment: 'together',
          }],
        },
      });

      scope.$on('$destroy', function() {
        drop.destroy();
      });
    }
  };
});
