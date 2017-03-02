'use strict';

angular.module('biggraph')
 .directive('drawingBoard', function(createBox, createArrow /*, createBox, createState */ ) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/boxes-gui/drawing-board.html',
      templateNamespace: 'svg',
      scope: {
        diagram: '=',
        selectedBox: '=',
        selectedState: '=',
        allBoxes: '=',
      },
      link: function(scope, element) {
        scope.boxes = [];
        scope.boxMap = {};
        scope.arrows = [];
        scope.stateMap = {};

        scope.$watchGroup(['diagram', 'allBoxes'], function() {


          // triggers on full diagram change triggered externally
          scope.boxes = [];
          scope.boxMap = {};
          if (!scope.diagram || !scope.allBoxes) {
            return;
          }
          scope.allBoxesMap = {};
          var i;
          for (i = 0; i < scope.allBoxes.length; ++i) {
            var boxMeta = scope.allBoxes[i];
            scope.allBoxesMap[boxMeta.operation] = boxMeta;
          }

          scope.arrows = [];

          if (scope.diagram.boxes !== undefined) {
            var box;
            for (i = 0; i < scope.diagram.boxes.length; ++i) {
              var bx0 = scope.diagram.boxes[i];
              var operationId = bx0.operation;
              var boxId = bx0.id;
              box = createBox(
                  scope.allBoxesMap[operationId],
                  bx0);
              scope.boxes[i] = box;
              scope.boxMap[boxId] = box;
            }

            for (i = 0; i < scope.boxes.length; ++i) {
              var dst = scope.boxes[i];
              var inputs = dst.instance.inputs;
              for (var inputName in inputs) {
                if (inputs.hasOwnProperty(inputName)) {
                  var input = inputs[inputName];
                  console.log('INPUT: ', inputs, inputName, input);
                  var src = scope.boxMap[input.box];
                  console.log(src, dst, input.id, inputName);
                  scope.arrows.push(createArrow(
                    src.outputs, input.id,
                    dst.inputs, inputName
                  ));
                }
              }
            }

          }

/*
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
          }*/
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
          var src = plugs.outputs;
          var dst = plugs.inputs;


          dst.instance.inputs[dst.data.id] = {
            box: src.boxId,
            id: src.data.id
          };

/*
          scope.diagram.arrows.push({
            'src': plugs.outputs.toArrowEnd(),
            'dst': plugs.inputs.toArrowEnd(),
          });
*/
          

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
        scope.addBox = function(operationId, x, y) {
          cnt++;
          var boxId = operationId + cnt;

          scope.diagram.boxes.push(
              {
                id: boxId,
                operation: operationId,
                x: x,
                y: y,
                inputs: {},
                parameters: {}
              });
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
            scope.addBox(op.operation, origEvent.offsetX, origEvent.offsetY);
          });
        });
      }

    };

});
