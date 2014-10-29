'use strict';

// Creates a binding between "value" and an axisOptions field.
// This is tricky because we cannot pre-initialize axisOptions in the state.
// See #918 for more details.
angular.module('biggraph').directive('axisOption', function() {
  return {
    restrict: 'E',
    scope: { side: '=', type: '@', attr: '@', field: '@', value: '=' },
    link: function(scope) {
      function getAxisOptions() {
        return scope.side.axisOptions(scope.type, scope.attr);
      }
      function setAxisOptions(ao) {
        scope.side.state.axisOptions[scope.type][scope.attr] = ao;
      }
      scope.value = getAxisOptions()[scope.field];
      scope.$watch('value', function(value, before) {
        if (value === before) { return; }  // It's just watcher registration.
        var axisOptions = getAxisOptions();
        axisOptions[scope.field] = value;
        setAxisOptions(axisOptions);
      });
    },
  };
});
