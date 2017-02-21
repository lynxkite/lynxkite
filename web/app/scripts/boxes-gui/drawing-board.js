'use strict';

angular.module('biggraph')
 .directive('drawingBoard', function(createArrow, createBox, createState) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/boxes-gui/drawing-board.html',
      templateNamespace: 'svg',
      scope: {
        diagram: '=',
        selectedBox: '=',
        selectedState: '=',
      },
      link: function(scope, element) {
        scope.boxes = [];
        scope.boxMap = {};
        scope.arrows = [];
        scope.stateMap = {};

        scope.$watch('diagram', function() {
          // triggers on full diagram change triggered externally
          scope.boxes = [];
          scope.boxMap = {};
          for (var i = 0; i < scope.diagram.boxes.length; ++i) {
            var box = createBox(scope.diagram.boxes[i]);
            scope.boxes[i] = box;
            scope.boxMap[box.data.id] = box;
          }
          scope.arrows = [];
          for (i = 0; i < scope.diagram.arrows.length; ++i) {
            scope.arrows[i] = createArrow(
                scope.diagram.arrows[i],
                scope.boxMap);
          }
          scope.stateMap = {};
          for (i = 0; i < scope.diagram.states.length; ++i) {
            var state = scope.diagram.states[i];
            scope.stateMap[state.box + '.' + state.output] = createState(state);
          }
        });

        scope.selectBox = function(box) {
          scope.selectedBox = box;
        };
        scope.selectPlug = function(plug) {
          scope.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            var key = plug.boxId + '.' + plug.data.id;
            scope.selectedState = scope.stateMap[key];
          }
        };
        scope.onMouseMove = function(event) {
          scope.mouseX = event.offsetX;
          scope.mouseY = event.offsetY;
          if (event.buttons === 1 && scope.movedBox) {
            scope.movedBox.onMouseMove(event);
          }
        };
        scope.onMouseUp = function() {
          scope.movedBox = undefined;
          scope.pulledPlug = undefined;
        };
        scope.onMouseDownOnBox = function(box, event) {
          scope.movedBox = box;
          scope.movedBox.onMouseDown(event);
        };
        scope.onMouseDownOnPlug = function(plug, event) {
          event.stopPropagation();
          scope.selectPlug(plug);
          scope.pulledPlug = plug;
        };
        scope.addArrow = function(plug1, plug2) {
          if (plug1.direction === plug2.direction) {
            return;
          }
          var plugs = {};
          plugs[plug1.direction] = plug1;
          plugs[plug2.direction] = plug2;

          scope.diagram.arrows.push({
            'src': plugs.outputs.toArrowEnd(),
            'dst': plugs.inputs.toArrowEnd(),
          });
          // refresh hack:
          scope.diagram = Object.assign({}, scope.diagram);
        };
        scope.onMouseUpOnPlug = function(plug, event) {
          event.stopPropagation();
          if (scope.pulledPlug) {
            scope.addArrow(scope.pulledPlug, plug);
            scope.pulledPlug = undefined;
          }
        };

        var cnt = 0;
        function hackBox(box) {
          box.inputs = [ { 'id': 'project', 'kind': 'project' } ];
          box.outputs = [ { 'id': 'project', 'kind': 'project' } ];
          box.operation = box.title;
          cnt++;
          box.id = box.id + cnt;
        }
        scope.addBox = function(box, x, y) {
          hackBox(box);
          box.x = x;
          box.y = y;
          scope.diagram.boxes.push(box);
          // hack: trigger recreate
          scope.diagram = Object.assign({}, scope.diagram);
        };
        element.bind('dragover', function(event) {
          event.preventDefault();
        });
        element.bind('drop', function(event) {
          event.preventDefault();
          var origEvent = event.originalEvent;
          var opText = event.originalEvent.dataTransfer.getData('text');
          var op = JSON.parse(opText);
          scope.$apply(function() {
            scope.addBox(op, origEvent.offsetX, origEvent.offsetY);
          });
        });
      }

    };

});
