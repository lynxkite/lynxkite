'use strict';

// Collapses parenthesised sections of its "text" attribute, replaced by a colored, 2-letter mnemonic.
angular.module('biggraph').directive('shortn', ['$compile', function($compile) {
  // Java's hash function in JS.
  function hashCode(text) {
    /* jshint bitwise: false */
    var hash = 0;
    for (var i = 0; i < text.length; ++i) {
      hash = hash * 31 + text.charCodeAt(i);
      hash |= 0; // Trim to 32 bits.
    }
    return Math.abs(hash);
  }

  // A 2-character hash.
  var A = 'A'.charCodeAt(0);
  function hash(text) {
    var code = hashCode(text);
    var h = String.fromCharCode(A + code % 26);
    code = Math.floor(code / 26);
    h += String.fromCharCode(A + code % 26);
    return h;
  }

  // A color hash.
  function color(text) {
    var code = hashCode(text);
    var h = '#';
    h += (10 + 4 * (code % 2)).toString(16); code = Math.floor(code / 2);
    h += (10 + 4 * (code % 2)).toString(16); code = Math.floor(code / 2);
    h += (10 + 4 * (code % 2)).toString(16); code = Math.floor(code / 2);
    return h;
  }

  return {
    restrict: 'E',
    scope: { text: '@' },
    replace: false,
    link: function(scope, element) {
      scope.$watch('text', function(text) {
        function getFrom(i) {
          var result = '';
          for (; i < text.length; ++i) {
            if (text[i] === '(') {
              var r = getFrom(i + 1);
              if (r.text.length > 10) {
                var t = r.text;
                var h = hash(t);
                var c = color(t);
                result += '<shortned hash="' + h + '" color="' + c + '">' + t + '</shortned>';
              } else {
                // Do not collapse short strings.
                result += '(' + r.text + ')';
              }
              i = r.end;
            } else if (text[i] === ')') {
              return {end: i, text: result};
            } else {
              result += text[i];
            }
          }
          return {end: i - 1, text: result};
        }
        var r = getFrom(0);
        element.empty();
        var el = angular.element('<span>' + r.text + '</span>');
        var comp = $compile(el);
        element.append(comp(scope));
      });
    },
  };
}]);

// The collapsed text. Can be expanded with a click.
angular.module('biggraph').directive('shortned', function() {
  return {
    restrict: 'E',
    replace: false,
    transclude: true,
    scope: { hash: '@', color: '@' },
    template: '<span class="shortned" style="background-color: {{color}};" ng-hide="expanded" ng-click="expanded = true; $event.stopPropagation()">{{hash}}</span><span class="unshortned" ng-show="expanded"><b>(</b><span ng-transclude></span><b>)</b></span>',
    link: function() {},
  };
});
