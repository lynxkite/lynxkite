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
    return 30 * (code % 12);
  }

  return {
    restrict: 'E',
    scope: { text: '@' },
    replace: false,
    link: function(scope, element) {
      scope.$watch('text', function(text) {
        function getFrom(start) {
          var markup = '';
          for (var i = start; i < text.length; ++i) {
            if (text[i] === '(') {
              var r = getFrom(i + 1);
              if (r.text.length > 5) {
                markup += r.markup;
              } else {
                // Do not collapse short strings.
                markup += '(' + r.text + ')';
              }
              i = r.end;
            } else if (text[i] === ')') {
              break;
            } else {
              markup += text[i];
            }
          }
          var t = text.substring(start, i);
          var h = hash(t);
          var c = color(t);
          markup = '<shortned hash="' + h + '" color="' + c + '">' + markup + '</shortned>';
          return {end: i, markup: markup, text: t};
        }
        var r = getFrom(0);
        element.empty();
        var el = angular.element(r.markup);
        el.attr('expanded', 'true');
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
    scope: { hash: '@', color: '@', expanded: '@' },
    templateUrl: 'shortned.html',
    link: function(scope) {
      scope.clicked = function(event) {
        scope.expanded = !scope.expanded;
        event.preventDefault();
        event.stopPropagation();
      };
    },
  };
});
