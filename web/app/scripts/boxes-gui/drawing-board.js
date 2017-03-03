'use strict';

angular.module('biggraph')
 .directive('drawingBoard', function(createBox, createArrow, util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/boxes-gui/drawing-board.html',
      templateNamespace: 'svg',
      scope: {
        workspaceName: '=',
        selectedBox: '=',
        selectedState: '=',
        allBoxes: '=',
      },
      link: function(scope, element) {
        scope.boxes = [];
        scope.boxMap = {};
        scope.arrows = [];
        scope.stateMap = {};

        scope.$watch('workspaceName', function() {
          scope.diagram = util.nocache(
            '/ajax/getWorkspace',
            {
              name: scope.workspaceName
            });
        });

        scope.$watchGroup(
          ['diagram.$resolved', 'allBoxes.$resolved'],
          function() {
            if (scope.diagram && scope.diagram.$resolved &&
                scope.allBoxes && scope.allBoxes.$resolved) {
              scope.refresh();
            }
        });

        scope.refresh = function() {
          scope.boxes = [];
          scope.boxMap = {};
          scope.allBoxesMap = {};
          var i;
          for (i = 0; i < scope.allBoxes.boxes.length; ++i) {
            var boxMeta = scope.allBoxes.boxes[i];
            scope.allBoxesMap[boxMeta.operationID] = boxMeta;
          }

          scope.arrows = [];

          if (scope.diagram.boxes !== undefined) {
            var box;
            for (i = 0; i < scope.diagram.boxes.length; ++i) {
              var bx0 = scope.diagram.boxes[i];
              var operationId = bx0.operationID;
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
                  var src = scope.boxMap[input.boxID];
                  scope.arrows.push(createArrow(
                    src.outputs, input.id,
                    dst.inputs, inputName
                  ));
                }
              }
            }

          }

        };

        scope.saveChange = function() {
          util.post(
            '/ajax/setWorkspace',
            {
              name: scope.workspaceName,
              workspace: scope.diagram
            });
        };

        scope.selectBox = function(box) {
          scope.selectedBox = box;
        };
        scope.selectState = function(boxID, outputID) {
          console.log('selectstate: ', boxID, outputID);
        };
        scope.selectPlug = function(plug) {
          scope.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            //var key = plug.boxId + '.' + plug.data.id;
            scope.selectState(plug.boxId, plug.data.id);
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
          if (scope.movedBox) {
            scope.saveChange();
          }
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
            boxID: src.boxId,
            id: src.data.id
          };

          scope.refresh();
          scope.saveChange();
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
                operationID: operationId,
                x: x,
                y: y,
                inputs: {},
                parameters: {}
              });
          scope.refresh();
          scope.saveChange();
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
            scope.addBox(op.operationID, origEvent.offsetX, origEvent.offsetY);
          });
        });
      }

    };

});
