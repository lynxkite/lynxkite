// Axis options are per-attribute settings, such as logarithmic vs linear scale.
'use strict';

angular.module('biggraph').factory('axisOptions', function axisOptionsFactory(util) {
  return {
    // Binds axisOptions into the current scope as "variable".
    // This is tricky because we cannot pre-initialize axisOptions in the state.
    // See #918 for more details.
    bind: function(scope, side, type, attr, variable) {
      function getAxisOptions() {
        return side.axisOptions(type, attr);
      }
      function setAxisOptions(ao) {
        side.state.axisOptions[type][attr] = ao;
      }
      util.deepWatch(scope, getAxisOptions, function(ao) {
        scope[variable] = ao;
      });
      util.deepWatch(scope, variable, function(ao, before) {
        if (angular.equals(ao, before)) { return; }  // It's just watcher registration.
        setAxisOptions(ao);
      });
    },
  };
});
