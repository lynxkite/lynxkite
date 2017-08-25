// The tag list is a list that can be populated from a set of options.
// (Effectively this is the same as a select[multiple], just with a different, horizontal design.)
'use strict';

angular.module('biggraph').directive('tagList', function(util) {
  return {
    restrict: 'E',
    scope: {
      model: '=',
      options: '=',
      editable: '=',
      onBlur: '&',
    },
    templateUrl: 'scripts/operation/tag-list.html',
    link: function(scope) {
      scope.addTag = function(id) {
        scope.removeTag(id);
        scope.model.push(id);
        scope.onBlur();
      };
      scope.removeTag = function(id) {
        if (scope.editable) {
          scope.model = scope.model.filter(function(x) { return x !== id; });
          scope.onBlur();
        }
      };
      function getTags() {
        var tagsById = {};
        for (var i = 0; i < scope.options.length; ++i) {
          tagsById[scope.options[i].id] = scope.options[i];
        }
        var tags = [];
        for (i = 0; i < scope.model.length; ++i) {
          var id = scope.model[i];
          if (tagsById[id] !== undefined) {
            tags.push(tagsById[id]);
          } else {
            tags.push({ id: id, title: id, unknown: true });
          }
        }
        return tags;
      };
      util.deepWatch(scope, 'model', function() { scope.tags = getTags(); });
      util.deepWatch(scope, 'options', function() { scope.tags = getTags(); });
    },
  };
});
