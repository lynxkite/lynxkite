// The tag list is a list that can be populated from a set of options.
// (Effectively this is the same as a select[multiple], just with a different, horizontal design.)
'use strict';

angular.module('biggraph').directive('tagList', function() {
  return {
    restrict: 'E',
    scope: {
      model: '=',
      options: '=',
    },
    templateUrl: 'tag-list.html',
    link: function(scope) {
      scope.addTag = function(id) {
        scope.removeTag(id);
        scope.model.push(id);
      };
      scope.removeTag = function(id) {
        scope.model = scope.model.filter(function(x) { return x !== id; });
      };
      scope.getTags = function() {
        var tagsByID = {};
        for (var i = 0; i < scope.options.length; ++i) {
          tagsByID[scope.options[i].id] = scope.options[i];
        }
        var tags = [];
        for (i = 0; i < scope.model.length; ++i) {
          tags.push(tagsByID[scope.model[i]]);
        }
        return tags;
      };
    },
  };
});
