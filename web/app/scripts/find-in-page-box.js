'use strict';

angular.module('biggraph').directive('findInPageBox', function() {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'find-in-page-box.html',
    link: function(scope, element) {
      // DOM root node where to do the searching.
      // Currently this is hard-wired to the parent.
      var doc = element.parent();
      // The position of the found result which is focused.
      // (Orange color, scrolled into view.)
      var focusedResultPos = 0;
      // Number of results found.
      var numMatches = 0;

      // Gets the list of searchable text nodes from the document.
      function getTextNodes(node, list) {
        if (node.nodeType === 3) {
          // Text node.
          list.push(node);
        } else if (node.nodeType === 1 && node.childNodes &&
          node.tagName !== 'script' &&
          node.tagName !== 'style' &&
          node.tagName !== 'find-in-page-box') {
          // Take only element nodes that have children.
          // Ignore script node, style nodes and search box.
          for (var i = 0; i < node.childNodes.length; i++) {
            getTextNodes(node.childNodes[i], list);
          }
        }
      }

      function getText(nodes) {
        return nodes.map(function(node) { return node.data; }).join('');
      }

      function normalizeString(str) {
        return str.toLowerCase()
          .replace(/(?:\r\n|\r|\n|\t)/g, ' ');
      }

      // Lists the starting positions of the string pattern
      // in the string txt.
      function getMatchPositions(txt, pattern) {
        pattern = normalizeString(pattern);
        txt = normalizeString(txt);
        var startPos = 0;
        var list = [];
        /* eslint-disable no-constant-condition */
        while (true) {
          var pos = txt.indexOf(pattern, startPos);
          if (pos < 0) {
            break;
          }
          list.push(pos);
          startPos = pos + 1;
        }
        return list;
      }

      function wrapTextNodeInHighlightElem(node, selectionId) {
        var highlightElem = document.createElement('span');
        highlightElem.className =
          'find-highlight find-highlight-' + selectionId;
        var nodeClone = node.cloneNode(true);
        highlightElem.appendChild(nodeClone);
        node.parentNode.replaceChild(highlightElem, node);
      }

      // Highlights a portion of the text in node by replacing
      // it with a <span...></span> node. Returns a new node
      // containing the remaining text from node starting after
      // </span>.
      function highlightSection(node, start, length, selectionId) {
        // We break the text of node into 3 parts:
        // before selection, selection, after selection
        var part23 = node.splitText(start);
        // At this point it's true that:
        //   node === beforeSelection
        //   part23 === selection + afterSelection
        var part3 = part23.splitText(length);
        var part2 = part23;
        // At this point it's true that:
        //   part2 === selection
        //   part3 === afterSelection
        wrapTextNodeInHighlightElem(part2, selectionId);
        return part3;
      }

      // Given the list of text nodes and match positions,
      // replaces text of matches with a
      // <span class="find-higlight ...">text</span>
      // node.
      function higlightMatches(txtNodes, matchList, patternLength) {
        // Position at the text node at the beginning of the processed node.
        var pos = 0;
        // Index to matchList.
        var matchPos = 0;
        // If a matching pattern has started at the end of the previous
        // node, then this stores the number of remaining characters.
        // Zero otherwise.
        var partialMatchRemaining = 0;

        for (var i = 0; i < txtNodes.length; ++i) {
          var node = txtNodes[i];
          // If the current match started in the previous node:
          if (partialMatchRemaining > 0) {
            var takeLen1 = Math.min(partialMatchRemaining, node.length);
            node = highlightSection(node, 0, takeLen1, matchPos - 1);
            partialMatchRemaining = partialMatchRemaining - takeLen1;
          }
          // Process matches in the current node:
          while (matchPos < matchList.length && matchList[matchPos] < pos + node.length) {
            var startPos = matchList[matchPos] - pos;
            var len = Math.min(patternLength, node.length - startPos);
            node = highlightSection(node, matchList[matchPos] - pos, len, matchPos);
            partialMatchRemaining = patternLength - len;

            pos = matchList[matchPos] + patternLength;
            matchPos++;
          }

          pos += node.length;
        }
      }

      // Highlight all occurrances of pattern in node.
      function highlight(node, pattern) {
        var textNodes = [];
        getTextNodes(node, textNodes);
        var txt = getText(textNodes);
        var matchList = getMatchPositions(txt, pattern);
        higlightMatches(textNodes, matchList, pattern.length);
        return matchList.length;
      }

      function unhighlight() {
        return $('span.find-highlight').each(function () {
          var parent = this.parentNode;
          parent.replaceChild(this.firstChild, this);
          parent.normalize();
        }).end();
      }

      /* globals document, $ */
      function scrollTo(element) {
        var body = $('html, body');
        var windowTop = document.documentElement.scrollTop ||
          document.body.scrollTop;
        var height = $(window).height();
        var windowBottom = windowTop + height;
        var elementTop = element.offset().top;
        var elementBottom = elementTop + element.height();

        if (elementTop < windowTop || elementBottom > windowBottom) {
          body.scrollTop(element.offset().top - height / 2.0);
        }

      }

      function focusResult(pos) {
        var next = doc.find('span.find-highlight-' + pos);
        angular.element(next).addClass('find-highlight-current');
        scrollTo(angular.element(next));
      }

      function unFocusResult(pos) {
        var current = doc.find('span.find-highlight-' + pos);
        angular.element(current).removeClass('find-highlight-current');
      }

      function findNext() {
        unFocusResult(focusedResultPos);
        focusedResultPos = (focusedResultPos + 1) % numMatches;
        focusResult(focusedResultPos);
      }
      scope.findNext = function() {
        if (numMatches > 0) {
          findNext();
        }
      };
      scope.findPrev = function() {
        if (numMatches > 0) {
          unFocusResult(focusedResultPos);
          focusedResultPos = (focusedResultPos + numMatches - 1) % numMatches;
          focusResult(focusedResultPos);
        }
      };

      $('#find-in-page-text').bind(
        'keydown keypress',
        function (event) {
          if (event.keyCode === 13) {
            findNext();
            event.preventDefault();
          }
        });
      scope.$watch('text', function() {
        unhighlight(doc);
        if (scope.text && scope.text.length >= 3) {
          numMatches = highlight(doc[0], scope.text);
          focusedResultPos = 0;
          if (numMatches > 0) {
            focusResult(0);
          }
        }
      });
    }
  };
});
