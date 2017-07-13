'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
  .directive('stateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '=',
        plug: '=',
        popupModel: '=',
      },
      link: function(scope) {
        scope.instruments = [];
        scope.$watch('plug.stateId', update);
        util.deepWatch(scope, 'instruments', update);

        function update() {
          if (scope.instruments.length > 0) {
            scope.result = util.get('/ajax/getInstrumentedState', {
              workspace: scope.workspace.ref(),
              inputStateId: scope.plug.stateId,
              instruments: scope.instruments });
          } else {
            scope.result = { states: [scope.plug], metas: [] };
          }
        }

        scope.getDefaultSnapshotName = function() {
          return scope.workspace.name + '-' + scope.plugId;
        };

        scope.setInstrument = function(index, operationId) {
          scope.instruments.splice(index);
          scope.instruments.push({
            operationId: operationId,
            parameters: {},
            parametricParameters: {},
          });
          update();
        };

        scope.clearInstrument = function(index) {
          scope.instruments.splice(index);
          update();
        };

        scope.createSnapshot = function(stateId, saveAsName, success, error) {
          var postOpts = { reportErrors: false };
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: stateId,
          }, postOpts).then(success, error);
        };
      },
    };
  });
