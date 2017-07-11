// UI for the "hash-check" parameter kind.
'use strict';

angular.module('biggraph').directive('hashCheck', function(util) {
  return {
    scope: {
      box: '=',
      hash: '=',
    },
    templateUrl: 'scripts/operation/hash-check.html',
    link: function(scope) {
      util.deepWatch(scope, 'box.instance', function(instance) {
        var staleSettings = util.get('ajax/staleImportParameters',
          {
            box: instance,
            parameterHash: instance.parameters['last_hash'],
          });
        staleSettings.then(function (response) {
          scope.stale = response.stale;
          scope.message = (response.stale) ? 'Stale settings!' : 'Settings are up to date';
        });
      });
    }
  };
});
