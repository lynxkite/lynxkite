// Displays help content by ID.
'use strict';

// Finds a snippet from the help pages by its ID. Replaces the first <hr> with a "read more" link.
angular.module('biggraph').directive('helpId',
  function(documentation, $compile, $anchorScroll, util) {
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
        documentation('help').then(function(helpContent) {
          var id = scope.helpId.toLowerCase();
          if (isUserWorkflowId(id)) {
            element.empty();
            return;
          }
          var content = helpContent.find('#' + id).first();
          if (content.length === 0 && !id.includes('-apply_to')) {
            /* eslint-disable no-console */
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
            content.find('.exampleblock').each(function (index, exampleBlock) {
              var exampleBlockElement = angular.element(exampleBlock);
              var readMoreElement = angular.element('<a class="read-more">read more</a>');
              readMoreElement.click(expander(readMoreElement, exampleBlockElement));
              exampleBlockElement.before(readMoreElement);
              exampleBlockElement.hide();
            });
          }
          function expander(expanderLink, expanderContent) {
            return function() {
              expanderLink.hide();
              expanderContent.show();
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
      scope: {
        helpId: '@href',
        fallbackText: '@'
      },
      templateUrl: 'scripts/help/help-popup.html',
      link: function(scope, element) {
        var button = element.find('#help-button')[0];
        var popup = element.find('#help-popup');
        if (!scope.helpId && scope.fallbackText) {
          popup.append(scope.fallbackText);
        }
        scope.isEmpty = function() {
          return !popup.text();
        };
        /* global Drop */
        var drop = new Drop({
          target: button,
          content: popup[0],
          position: 'bottom center',
          classes: 'drop-theme-help-popup',
          openOn: dropTooltipConfig.enabled ? 'click hover' : undefined,
          hoverOpenDelay: 700,
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
