'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
  .directive('stateView', function(util, $q) {
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
        scope.$watch('plug.stateId', function() {
          console.log('statid change');
          scope.instruments[0] = {
            stateId: scope.plug.stateId,
            kind: scope.plug.kind,
            error: scope.plug.error,
            promise: $q.resolve(scope.plug),
          };
        });

        scope.getDefaultSnapshotName = function() {
          return scope.workspace.name + '-' + scope.plugId;
        };

        scope.setInstrument = function(index, operationId) {
          scope.instruments.splice(index + 1);
          var instrument = {
            workspace: scope.workspace.ref(),
            operationId: operationId,
            parameters: {},
            parametricParameters: {}
          };
          var instrState = { instrument: instrument };
          scope.instruments.push(instrState);
          instrState.promise = scope.instruments[index].promise.then(function(state) {
            instrument.inputStateId = state.stateId;
            return util.get('/ajax/getInstrumentedState', instrument);
          });
          instrState.promise.then(function(state) {
            instrState.stateId = state.stateId;
            instrState.kind = state.kind;
            instrState.error = state.error;
          });
        };

        scope.clearInstrument = function(index) {
          scope.instruments.splice(index + 1);
        };

        scope.createSnapshot = function(saveAsName, success, error) {
          var postOpts = { reportErrors: false };
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: scope.plug.stateId,
          }, postOpts).then(success, error);
        };
      },
    };
  });
