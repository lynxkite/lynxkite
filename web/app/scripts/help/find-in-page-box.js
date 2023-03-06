'use strict';
import '../app';

angular.module('biggraph').directive('findInPageBox', function() {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'scripts/help/find-in-page-box.html',
    link: function(scope) {
      /* globals $ */
      // The position of the found result which is focused.
      // (Orange color, scrolled into view.)
      let focusedResultPos = 0;
      // Number of results found.
      let numMatches = 0;

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
          for (let i = 0; i < node.childNodes.length; i++) {
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
        let startPos = 0;
        const list = [];
        /* eslint-disable no-constant-condition */
        while (true) {
          const pos = txt.indexOf(pattern, startPos);
          if (pos < 0) {
            break;
          }
          list.push(pos);
          startPos = pos + 1;
        }
        return list;
      }

      function wrapTextNodeInHighlightElem(node, selectionId) {
        const highlightElem = document.createElement('span');
        highlightElem.className =
          'find-highlight find-highlight-' + selectionId;
        const nodeClone = node.cloneNode(true);
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
        const part23 = node.splitText(start);
        // At this point it's true that:
        //   node === beforeSelection
        //   part23 === selection + afterSelection
        const part3 = part23.splitText(length);
        const part2 = part23;
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
        let pos = 0;
        // Index to matchList.
        let matchPos = 0;
        // If a matching pattern has started at the end of the previous
        // node, then this stores the number of remaining characters.
        // Zero otherwise.
        let partialMatchRemaining = 0;

        for (let i = 0; i < txtNodes.length; ++i) {
          let node = txtNodes[i];
          // If the current match started in the previous node:
          if (partialMatchRemaining > 0) {
            const takeLen1 = Math.min(partialMatchRemaining, node.length);
            node = highlightSection(node, 0, takeLen1, matchPos - 1);
            partialMatchRemaining = partialMatchRemaining - takeLen1;
          }
          // Process matches in the current node:
          while (matchPos < matchList.length && matchList[matchPos] < pos + node.length) {
            const startPos = matchList[matchPos] - pos;
            const len = Math.min(patternLength, node.length - startPos);
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
        const textNodes = [];
        getTextNodes(node, textNodes);
        const txt = getText(textNodes);
        const matchList = getMatchPositions(txt, pattern);
        higlightMatches(textNodes, matchList, pattern.length);
        return matchList.length;
      }

      function unhighlight() {
        return $('span.find-highlight').each(function () {
          const parent = this.parentNode;
          parent.replaceChild(this.firstChild, this);
          parent.normalize();
        }).end();
      }

      function focusResult(pos) {
        const next = $('span.find-highlight-' + pos);
        next.addClass('find-highlight-current');
        next[0].scrollIntoView({});
      }

      function unFocusResult(pos) {
        const current = $('span.find-highlight-' + pos);
        $(current).removeClass('find-highlight-current');
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
          if (event.keyCode === 13) { // ENTER
            findNext();
            event.preventDefault();
          } else if (event.keyCode === 27) { // ESC
            scope.text = '';
            scope.$digest();
          }
        });
      scope.$watch('text', function() {
        unhighlight();
        if (scope.text && scope.text.length >= 3) {
          numMatches = highlight($('#whole-help')[0], scope.text);
          focusedResultPos = 0;
          if (numMatches > 0) {
            focusResult(0);
          }
        }
      });
    }
  };
});
