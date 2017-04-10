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
//    - downloads a workspace state and saves it in backendState
//    - creates a workspaceWrapper using the downloaded state and
//      sets this.wrapper to point to it
//    - visible GUI gets updated based on this.wrapper via
//      workspace-drawing-board
// 3. user edit happens, e.g. box move, add box, or add arrow
//    - this updates the wrapper.state
//    - all frontend-facing objects are updated inside
//      workspaceWrapper
//    - backendState remains unchanged at this point
// 5. saveWorkspace()
// 6. GOTO 2

angular.module('biggraph')
  .factory('workspace', function(workspaceWrapper, util, $interval) {
    return function(boxCatalog, workspaceName) {
      var progressUpdater;

      var boxCatalogMap = {};
      for (var i = 0; i < boxCatalog.boxes.length; ++i) {
        var boxMeta = boxCatalog.boxes[i];
        boxCatalogMap[boxMeta.operationID] = boxMeta;
      }

      var workspace = {
        name: workspaceName,

        boxes: function() {
          return this.wrapper ? this.wrapper.boxes : [];
        },

        arrows: function() {
          return this.wrapper ? this.wrapper.arrows : [];
        },

        loadWorkspace: function() {
          var that = this;
          util.nocache(
              '/ajax/getWorkspace',
              {
                name: this.name
              })
              .then(function(state) {
                that.backendState = state;
                // User edits will be applied to a deep copy of
                // the original backend state. This way watchers
                // of backendState will only be notified once the
                // backend is fully aware of the new state.
                var stateCopy = angular.copy(state);
                that.wrapper = workspaceWrapper(
                  stateCopy, boxCatalogMap);
              });
        },

        saveWorkspace: function() {
          var that = this;
          util.post(
            '/ajax/setWorkspace',
            {
              name: this.name,
              workspace: that.wrapper.state,
            }).finally(
              // Reload workspace both in error and success cases.
              function() { that.loadWorkspace(); });
        },

        selectBox: function(boxId) {
          this.selectedBoxId = boxId;
        },

        selectedBox: function() {
          if (this.selectedBoxId) {
            return this.wrapper.boxMap[this.selectedBoxId];
          } else {
            return undefined;
          }
        },

        updateSelectedBox: function(paramValues) {
          var that = this;
          var operationId = this.wrapper.boxMap[that.selectedBoxId].metadata.operationID;
          util.nocache(
            '/ajax/getSummary',
            {
                operationId: operationId,
                parameters: paramValues,
            })
          .then(
            function success(response) {
                that.wrapper.setBoxSummary(operationId, response.summary);
            });
          this.wrapper.setBoxParams(this.selectedBoxId, paramValues);
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
                that.selectedStateKind = response.kind;
              },
              function error() {
                that.selectedStateId = undefined;
                that.selectedStateKind = undefined;
              });
        },

        selectPlug: function(plug) {
          this.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            this.selectState(plug.boxId, plug.id);
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
            if (this.wrapper.addArrow(otherPlug, plug)) {
              this.saveWorkspace();
            }
          }
          if (!this.pulledPlug || this.pulledPlug !== plug) {
            this.selectPlug(plug);
          }
        },

        // boxID should be used for test-purposes only
        addBox: function(operationId, pos, boxID) {
          this.wrapper.addBox(operationId, pos.x, pos.y, boxID);
          this.saveWorkspace();
        },

        getAndUpdateProgress: function(errorHandler) {
          var that = this;
          var workspaceBefore = this.wrapper;
          var plugBefore = this.selectedPlug;
          if (workspaceBefore && plugBefore && plugBefore.direction === 'outputs') {
            util.nocache('/ajax/getProgress', {
              workspace: this.name,
              output: {
                boxID: plugBefore.boxId,
                id: plugBefore.id
              }
            }).then(
              function success(response) {
                if (that.wrapper && that.wrapper === workspaceBefore &&
                    that.selectedPlug && that.selectedPlug === plugBefore) {
                  that.wrapper.updateProgress(response.progressList);
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
              that.wrapper.clearProgress();
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
      workspace.loadWorkspace();
      return workspace;
    };
  });
