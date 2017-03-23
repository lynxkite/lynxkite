'use strict';

// An "uber manager" for the workspace state.
// This class is responsible for hooking a workspace to the
// several outside-world components, like the drawing-board
// box-editor, the state-view and also load/save to the backend.
//
// The responsibility of the workspaceState is to mind its own
// business, i.e. it's just an API for the data structures
// describing the workspace.
//
// Life cycle:
// 1. boxCatalog needs to be loaded at all times for things to work
// 2. loadWorkspace()
//    - downloads a workspace
//    - triggers workspace.build()
//    - sets scope.workspace to the downloaded and built workspace
//    - visible GUI gets updated
// 3. user edit happens, e.g. box move, add box, or add arrow
// 4. in cases of "complex edits" - edits except for move:
//    - scope.workspace.build() is called by the edit code
//    - this updates the visible GUI immediately
// 5. saveWorkspace()
// 6. GOTO 2

angular.module('biggraph')
  .factory('workspaceManager', function(workspaceState, util, $interval) {
    return function(boxCatalog, workspaceName) {
      var progressUpdater;

      var boxCatalogMap = {};
      for (var i = 0; i < boxCatalog.boxes.length; ++i) {
        var boxMeta = boxCatalog.boxes[i];
        boxCatalogMap[boxMeta.operationID] = boxMeta;
      }

      var boxSelectionCallback;

      var manager = {
        loadWorkspace: function() {
          var that = this;
          util.nocache(
              '/ajax/getWorkspace',
              {
                name: workspaceName
              })
              .then(function(rawWorkspace) {
                that.workspace = workspaceState(
                  rawWorkspace, boxCatalogMap);
                that.broadcastBoxSelection();
              });
        },

        saveWorkspace: function() {
          var that = this;
          util.post(
            '/ajax/setWorkspace',
            {
              name: workspaceName,
              workspace: that.workspace.rawWorkspace(),
            }).then(
              // Reload workspace both in error and success cases.
              function() { that.loadWorkspace(); },
              function() { that.loadWorkspace(); });
        },

        selectBox: function(boxId) {
          var old = this.selectedBoxId;
          this.selectedBoxId = boxId;
          if (old !== boxId) {
            this.broadcastBoxSelection();
          }
        },

        setBoxSelectionCallback: function(callback) {
          boxSelectionCallback = callback;
        },

        broadcastBoxSelection: function() {
          if (boxSelectionCallback) {
            boxSelectionCallback();
          }
        },

        selectedBox: function() {
          if (this.selectedBoxId) {
            return this.workspace.boxMap[this.selectedBoxId];
          } else {
            return undefined;
          }
        },

        updateSelectedBox: function(paramValues) {
          this.workspace.setBoxParams(this.selectedBoxId, paramValues);
          this.saveWorkspace();
        },

        selectState: function(boxID, outputID) {
          this.selectedStateId = util.nocache(
            '/ajax/getOutputID',
            {
              workspace: workspaceName,
              output: {
                boxID: boxID,
                id: outputID
              }
            });
        },

        selectPlug: function(plug) {
          this.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            this.selectState(plug.boxId, plug.data.id);
            this.startProgressUpdate();
          } else {
            this.selectedState = undefined;
          }
        },

        onMouseMove: function(mouseLogical) {
          this.mouseLogical = mouseLogical;
          if (event.buttons === 1 && this.movedBox) {
            this.movedBox.onMouseMove(this.mouseLogical);
          }
        },

        onMouseUp: function() {
          if (this.movedBox && this.movedBox.isMoved) {
            this.saveWorkspace();
          }
          this.movedBox = undefined;
          this.pulledPlug = undefined;
        },

        onMouseDownOnBox: function(box, mouseLogical) {
          this.selectBox(box.instance.id);
          this.movedBox = box;
          this.movedBox.onMouseDown(mouseLogical);
        },

        onMouseDownOnPlug: function(plug, event) {
          event.stopPropagation();
          this.pulledPlug = plug;
        },

        onMouseUpOnPlug: function(plug, event) {
          event.stopPropagation();
          if (this.pulledPlug) {
            var otherPlug = this.pulledPlug;
            this.pulledPlug = undefined;
            if (this.workspace.addArrow(otherPlug, plug)) {
              this.saveWorkspace();
            }
          }
          if (!this.pulledPlug || this.pulledPlug !== plug) {
            this.selectPlug(plug);
          }
        },

        addBox: function(operationId, pos) {
          this.workspace.addBox(operationId, pos.x, pos.y);
          this.saveWorkspace();
        },

        getAndUpdateProgress: function(errorHandler) {
          var that = this;
          var workspaceBefore = this.workspace;
          var plugBefore = this.selectedPlug;
          if (workspaceBefore && plugBefore && plugBefore.direction === 'outputs') {
            util.nocache('/ajax/getProgress', {
              workspace: workspaceName,
              output: {
                boxID: plugBefore.boxId,
                id: plugBefore.data.id
              }
            }).then(
              function success(response) {
                if (that.workspace && that.workspace === workspaceBefore &&
                    that.selectedPlug && that.selectedPlug === plugBefore) {
                  that.workspace.updateProgress(response.progressList);
                }
              },
              errorHandler);
          }
        },

        startProgressUpdate: function() {
          var that = this;
          that.stopProgressUpdate();
          progressUpdater = $interval(function() {
            function errorHandler(error) {
              util.error('Couldn\'t get progress information for selected state.', error);
              that.stopProgressUpdate();
              that.workspace.clearProgress();
            }
            that.getAndUpdateProgress(errorHandler);
          }, 2000);
        },

        stopProgressUpdate: function() {
          if (progressUpdater) {
            $interval.cancel(progressUpdater);
            progressUpdater = undefined;
          }
        },
      };
      manager.loadWorkspace();
      return manager;
    };
  });
