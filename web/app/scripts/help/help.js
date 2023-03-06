// Displays help content by ID.
'use strict';
import '../app';
import Drop from "tether-drop";
import templateUrl from './help-popup.html?url';

// Finds a snippet from the help pages by its ID. Replaces the first <hr> with a "read more" link.
angular.module('biggraph').directive('helpId',
  ["documentation", "$compile", function(documentation, $compile) {
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
          const id = scope.helpId.toLowerCase();
          if (!id || isUserWorkflowId(id)) {
            element.empty();
            return;
          }
          let content = helpContent.find('#' + id).first();
          if (content.length === 0 && !id.includes('-apply_to')) {
            /* eslint-disable no-console */
            console.warn('Could not find help ID', id);
          }
          content = content.clone();
          if (scope.removeHeader === 'yes') {
            content.find('h1,h2,h3,h4,h5,h6').first().remove();
          }
          // Help links inside the main UI should open in new tabs.
          content.find('a[href]').each(function(i, a) {
            a.setAttribute('target', '_blank');
          });
          // Collapse sections marked with ====.
          content.find('.exampleblock').each(function (index, exampleBlock) {
            const exampleBlockElement = angular.element(exampleBlock);
            const readMoreElement = angular.element('<a class="read-more">read more</a>');
            readMoreElement.click(expander(readMoreElement, exampleBlockElement));
            exampleBlockElement.before(readMoreElement);
            exampleBlockElement.hide();
          });
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
        });
      }
    };
  }]);

// A button that displays a help snippet on hover/click.
angular.module('biggraph')
  .directive('helpPopup', ["dropTooltipConfig", function(dropTooltipConfig) {

    return {
      restrict: 'E',
      scope: {
        helpId: '@href',
        fallbackText: '@',
        container: '@',
      },
      templateUrl,
      link: function(scope, element) {
        const button = element.find('#help-button')[0];
        const popup = element.find('#help-popup');
        if (!scope.helpId && scope.fallbackText) {
          popup.append(scope.fallbackText);
        }
        scope.isEmpty = function() {
          return !popup.text();
        };
        /* global Drop */
        const drop = new Drop({
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
              to: scope.container ? document.querySelector(scope.container) : 'window',
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
  }]);
