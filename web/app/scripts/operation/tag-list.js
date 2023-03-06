// The tag list is a list that can be populated from a set of options.
// (Effectively this is the same as a select[multiple], just with a different, horizontal design.)
'use strict';
import '../app';
import '../util/util';
import templateUrl from './tag-list.html?url';

angular.module('biggraph').directive('tagList', ['util', function(util) {
  return {
    restrict: 'E',
    scope: {
      model: '=',
      options: '=',
      onBlur: '&',
    },
    templateUrl,
    link: function(scope) {
      scope.addTag = function(id) {
        scope.removeTag(id);
        scope.model.push(id);
        scope.onBlur();
      };
      scope.removeTag = function(id) {
        scope.model = scope.model.filter(function(x) { return x !== id; });
        scope.onBlur();
      };
      function getTags() {
        const tagsById = {};
        for (let i = 0; i < scope.options.length; ++i) {
          tagsById[scope.options[i].id] = scope.options[i];
        }
        const tags = [];
        for (let i = 0; i < scope.model.length; ++i) {
          const id = scope.model[i];
          if (tagsById[id] !== undefined) {
            tags.push(tagsById[id]);
          } else {
            tags.push({ id: id, title: id, unknown: true });
          }
        }
        return tags;
      }
      util.deepWatch(scope, 'model', function() { scope.tags = getTags(); });
      util.deepWatch(scope, 'options', function() { scope.tags = getTags(); });
    },
  };
}]);
