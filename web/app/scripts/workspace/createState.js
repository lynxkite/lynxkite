'use strict';

angular.module('biggraph').factory('createState', function() {
  return function(state) {
    return {
      kind: state.kind,
      state: state.state,
    };
  };
});
