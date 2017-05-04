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

angular.module('biggraph').factory(
  'workspaceGuiMaster',
  function(workspaceWrapper) {
    return function(boxCatalog, workspaceName) {

      var workspace = {
        getBox: function(id) {
          return this.wrapper.boxMap[id];
        },

        getOutputPlug: function(boxId, plugId) {
          return this.getBox(boxId).outputMap[plugId];
        },

      };

      workspace.wrapper = workspaceWrapper(workspaceName, boxCatalog);
      workspace.wrapper.loadWorkspace();
      return workspace;
    };
  });
