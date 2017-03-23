'use strict';

// This class manages a workspace state and its connection to Angular
// components (workspace-drawing-board, box-editor, state-view) and the
// backend.
// Handling the workspace state data structure and wrapping it with API
// objects is outsourced from here to workspaceWrapper.
//
// Life cycle:
// 1. boxCatalog needs to be loaded at all times for things to work
// 2. loadWorkspace()
//    - downloads a workspace state
//    - creates a workspaceWrapper using the downloaded state and
//      sets this.workspace to points to it
//    - visible GUI gets updated via workspace-drawing-board
// 3. user edit happens, e.g. box move, add box, or add arrow
//    - all objects are updated inside workspaceWrapper
// 5. saveWorkspace()
// 5. GOTO 2

angular.module('biggraph')
  .factory('workspace', function(workspaceWrapper, util, $interval) {
    return function(boxCatalog, workspaceName) {
      var progressUpdater;

      var boxCatalogMap = {};
      for (var i = 0; i < boxCatalog.boxes.length; ++i) {
        var boxMeta = boxCatalog.boxes[i];
        boxCatalogMap[boxMeta.operationID] = boxMeta;
      }

      var boxSelectionCallback;

      var manager = {
        name: workspaceName,

        boxes: function() {
          return this.workspace ? this.workspace.boxes : [];
        },

        arrows: function() {
          return this.workspace ? this.workspace.arrows : [];
        },

        loadWorkspace: function() {
          var that = this;
          util.nocache(
              '/ajax/getWorkspace',
              {
                name: this.name
              })
              .then(function(state) {
                that.workspace = workspaceWrapper(
                  state, boxCatalogMap);
                that.broadcastBoxSelection();
              });
        },

        saveWorkspace: function() {
          var that = this;
          util.post(
            '/ajax/setWorkspace',
            {
              name: this.name,
              workspace: that.workspace.state,
            }).finally(
              // Reload workspace both in error and success cases.
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
          var that = this;
          util.nocache(
            '/ajax/getOutputID',
            {
              workspace: this.name,
              output: {
                boxID: boxID,
                id: outputID
              }
            })
            .then(
              function success(response) {
                that.selectedStateId = response.id;
              },
              function error() {
                that.selectedStateId = undefined;
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
              workspace: this.name,
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
