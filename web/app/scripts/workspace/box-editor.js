'use strict';

// Viewer and editor of a box instance.

angular.module('biggraph')
 .directive('boxEditor', function(util, $rootScope) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/box-editor.html',
      scope: {
        box: '=',
        workspaceName: '=',
      },
      link: function(scope) {
        scope.$watchGroup(
            ['workspaceName', 'box.instance.id'],
            function() {
              if (!scope.workspaceName || !scope.box) {
                return;
              }
              // The below magic makes sure that the response
              // to the result of the latest getOperationMetaRequest
              // will be passed to scope.newOpSelected().
              var currentRequest;
              scope.lastRequest = currentRequest = util
                .nocache(
                  '/ajax/getOperationMeta',
                  {
                      workspace: scope.workspaceName,
                      box: scope.box.instance.id
                  })
                .then(
                  function(boxMeta) {
                    // success
                    if (scope.lastRequest === currentRequest) {
                      scope.newOpSelected(boxMeta);
                    }
                  },
                  function() {
                    // error
                    if (scope.lastRequest === currentRequest) {
                      scope.newOpSelected(undefined);
                    }
                  });
            });

        scope.paramValues = {};

        // Invoked when the user selects a new operation and its
        // metadata is successfully downloaded.
        scope.newOpSelected = function(boxMeta) {
            scope.boxMeta = boxMeta;
            // Make a copy of the parameter values.
            scope.paramValues = Object.assign(
                {}, scope.box.instance.parameters);
            if (!scope.boxMeta) {
              return;
            }

            // Populate parameter values.
            for (var i = 0; i < boxMeta.parameters.length; ++i) {
              var p = boxMeta.parameters[i];
              if (scope.paramValues[p.id] !== undefined) {
                // Parameter is set externally.
              } else if (p.options.length === 0) {
                scope.paramValues[p.id] = p.defaultValue;
              } else if (p.multipleChoice) {
                scope.paramValues[p.id] = '';
              } else {
                scope.paramValues[p.id] = p.options[0].id;
              }
            }
        };

        scope.apply = function() {
          $rootScope.$broadcast(
              'box parameters updated',
              {
                  boxId: scope.box.instance.id,
                  paramValues: scope.paramValues
              });
        };
      },
    };
});
