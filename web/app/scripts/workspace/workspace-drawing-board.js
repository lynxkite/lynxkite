'use strict';

// The drawing board where the user can create and modify a boxes and
// arrows diagram.

angular.module('biggraph').directive(
  'workspaceDrawingBoard',
  function(
    environment, hotkeys, PopupModel, SelectionModel, WorkspaceWrapper, $rootScope, $q,
    $location, util, longPoll, pythonCodeGenerator, $timeout) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/workspace-drawing-board.html',
      scope: {
        boxCatalog: '=',
        workspaceName: '=',
      },
      link: function(scope, element) {
        scope.workspace = undefined;
        scope.selection = new SelectionModel();
        scope.dragMode = window.localStorage.getItem('drag_mode') || 'pan';
        scope.selectedBoxIds = [];
        scope.movedBoxes = undefined;
        // If the user is connecting plugs by drawing a line with the
        // mouse, then this points to the plug where the line was
        // started.
        scope.pulledPlug = undefined;
        // The last known position of the mouse, expressed in logical
        // workspace coordinates.
        scope.mouseLogical = { x: 300, y: 300 };
        scope.popups = [];
        scope.movedPopup = undefined;

        scope.$watch(
          'workspaceName',

          function() {
            if (scope.workspaceName) {
              scope.workspace = new WorkspaceWrapper(
                scope.workspaceName,
                scope.boxCatalog);
              scope.workspace.loadWorkspace();
              util.scopeTitle(scope, scope.getLastPart(scope.workspaceName));
            }
          });
        scope.$watch(
          'dragMode',
          function(dragMode) {
            window.localStorage.setItem('drag_mode', dragMode);
          });

        let workspaceDrag = false;
        let selectBoxes = false;
        let workspaceX = 0;
        let workspaceY = 0;
        let workspaceZoom = 0;
        let mouseX = 300;
        let mouseY = 300;
        const svgElement = element.find('svg');
        let svgOffset = svgElement.offset();
        function zoomToScale(z) { return Math.exp(z * 0.001); }
        function addLogicalMousePosition(event) {
        // event.offsetX/Y are distorted when the mouse is
        // over a popup window (even if over an invisible
        // overflow part of it), hence we compute our own:
          event.workspaceX = event.pageX - svgOffset.left;
          event.workspaceY = event.pageY - svgOffset.top;
          // Add location according to pan and zoom:
          const logical = scope.pageToLogical({ x: event.pageX, y: event.pageY });
          event.logicalX = logical.x;
          event.logicalY = logical.y;
          return event;
        }

        scope.pageToLogical = function(pos) {
          const z = zoomToScale(workspaceZoom);
          return {
            x: (pos.x - svgOffset.left - workspaceX) / z,
            y: (pos.y - svgOffset.top - workspaceY) / z,
          };
        };

        scope.logicalToPage = function(pos) {
          const z = zoomToScale(workspaceZoom);
          return {
            x: pos.x * z + workspaceX + svgOffset.left,
            y: pos.y * z + workspaceY + svgOffset.top,
          };
        };

        function actualDragMode(event) {
          const dragMode = (window.localStorage.getItem('drag_mode') || 'pan');
          // Shift chooses the opposite mode.
          if (dragMode === 'select') {
            return event.shiftKey ? 'pan' : 'select';
          } else {
            return event.shiftKey ? 'select' : 'pan';
          }
        }

        scope.onMouseMove = function(event) {
          event.preventDefault();
          svgOffset = svgElement.offset(); // Just in case the layout changed.
          addLogicalMousePosition(event);
          if (workspaceDrag) {
            workspaceX += event.workspaceX - mouseX;
            workspaceY += event.workspaceY - mouseY;
          } else if (selectBoxes) {
            scope.selection.onMouseMove(event);
            scope.selectBoxesInSelection();
          }
          mouseX = event.workspaceX;
          mouseY = event.workspaceY;

          const leftButton = event.buttons & 1;
          // Protractor omits button data from simulated mouse events.
          if (leftButton || environment.protractor) {
            scope.mouseLogical = {
              x: event.logicalX,
              y: event.logicalY,
            };
            if (scope.movedBoxes) {
              for (let i = 0; i < scope.movedBoxes.length; i++) {
                scope.movedBoxes[i].onMouseMove(event);
              }
              if (scope.movedBoxes.length === 1) {
                autoConnect(scope.movedBoxes[0]);
              }
            } else if (scope.movedPopup) {
              scope.movedPopup.onMouseMove(event);
            }
          }
        };

        // Tries hooking up plugs when a box is moving.
        function autoConnect(moving) {
          function flatten(array) {
            return Array.prototype.concat.apply([], array);
          }
          function filterOpen(plugs) {
            return plugs.filter(function(plug) { return plug.getAttachedPlugs().length === 0; });
          }
          const allOutputs = flatten(scope.workspace.boxes.map(function(box) { return box.outputs; }));
          const allInputs = flatten(scope.workspace.boxes.map(function(box) { return box.inputs; }));
          autoConnectPlugs(moving.inputs, allOutputs);
          autoConnectPlugs(filterOpen(moving.outputs), filterOpen(allInputs));
        }

        function autoConnectPlugs(srcPlugs, dstPlugs) {
          const hookDistance = 20;
          for (let i = 0; i < srcPlugs.length; ++i) {
            const src = srcPlugs[i];
            for (let j = 0; j < dstPlugs.length; ++j) {
              const dst = dstPlugs[j];
              const dx = src.cx() - dst.cx();
              const dy = src.cy() - dst.cy();
              const dist = Math.sqrt(dx * dx + dy * dy);
              if (dist < hookDistance) {
                scope.workspace.addArrow(src, dst, { willSaveLater: true });
              }
            }
          }
        }

        scope.onMouseDownOnBox = function(box, event) {
          event.stopPropagation();
          const leftClick = event.button === 0;
          if (!leftClick) {
            return;
          }
          addDragListeners();

          addLogicalMousePosition(event);
          scope.selection.remove();
          if (scope.selectedBoxIds.indexOf(box.instance.id) === -1) {
            if (!event.ctrlKey) {
              scope.selectedBoxIds = [];
            }
            scope.selectBox(box.instance.id);
            scope.movedBoxes = [box];
            scope.movedBoxes[0].onMouseDown(event);
          } else if (event.ctrlKey) {
            const selectedIndex = scope.selectedBoxIds.indexOf(box.instance.id);
            scope.selectedBoxIds.splice(selectedIndex, 1);
            scope.movedBoxes[0].onMouseDown(event);
          } else {
            scope.movedBoxes = this.selectedBoxes();
            scope.movedBoxes.map(function(b) {
              b.onMouseDown(event);
            });
          }
        };

        function placePopup(event) {
        // Avoid the event position, stay on the screen, and try to be close to the event.
          const w = 500;
          const h = 500;
          const eventX = event.pageX - w / 2;
          const eventY = event.pageY - h / 2;
          const minX = 0;
          const minY = svgOffset.top; // Do not overlap toolbar.
          const maxX = svgElement.width() - w - 35; // Do not overlap toolbox.
          const maxY = svgElement.height() - h;

          function len(x, y) { return Math.sqrt(x * x + y * y); }
          function rectangleOverlapArea(left1, top1, width1, height1, left2, top2, width2, height2) {
            const right1 = left1 + width1;
            const bottom1 = top1 + height1;
            const right2 = left2 + width2;
            const bottom2 = top2 + height2;
            // Compute intersection:
            const left = Math.max(left1, left2);
            const top = Math.max(top1, top2);
            const right = Math.min(right1, right2);
            const bottom = Math.min(bottom1, bottom2);
            if (left > right || top > bottom) {
              return 0;
            } else {
              return (right - left) * (bottom - top);
            }
          }
          function overlap(x, y) {
            let total = 0;
            for (let i = 0; i < scope.popups.length; ++i) {
              const pi = scope.popups[i];
              total += rectangleOverlapArea(
                pi.x, pi.y, pi.width, pi.height || pi.maxHeight,
                x, y, w, h);
            }
            return total;
          }
          function score(x, y) {
            return {
              distance: len(x - eventX, y - eventY),
              overlap: overlap(x, y),
            };
          }
          function isScoreBetterThan(current, best) {
            const minDist = Math.sqrt(w * w + h * h) / 2;
            if (best.distance < minDist && current.distance > best.distance) {
              return true;
            }
            if (current.overlap < best.overlap) {
              return true;
            } else if (current.overlap > best.overlap) {
              return false;
            }
            if (current.distance > minDist && current.distance < best.distance) {
              return true;
            } else {
              return false;
            }
          }

          let bestX = (minX + maxX) / 2;
          let bestY = (minY + maxY) / 2;
          let bestScore = score(bestX, bestY);
          for (let x = minX; x <= maxX; x += (maxX - minX) / 10) {
            for (let y = minY; y <= maxY; y += (maxY - minY) / 20) {
              const currentScore = score(x, y);
              if (isScoreBetterThan(currentScore, bestScore)) {
                bestX = x;
                bestY = y;
                bestScore = currentScore;
              }
            }
          }
          return {
            x: bestX,
            y: bestY,
            width: w,
            height: h,
          };
        }

        function getPopup(event, id, title, content) {
          for (let p of scope.popups) {
            if (p.id === id) {
              return p;
            }
          }
          const pos = placePopup(event);
          return new PopupModel(id, title, content, pos.x, pos.y, pos.width, pos.height, scope);
        }

        function afterUpdates(fn) {
          // Timeout to let the delayed blur events go first. Then wait for the request.
          $timeout(() => scope.workspace.loading.then(fn));
        }

        scope.onMouseUpOnBox = function(box, event) {
          if (box.isDirty || scope.pulledPlug || scope.selection.isActive()) {
            return;
          }
          const leftButton = event.button === 0;
          if (!leftButton || event.ctrlKey || event.shiftKey) {
            return;
          }
          afterUpdates(() => {
            const model = getPopup(
              event,
              box.instance.id,
              box.instance.operationId,
              {
                type: 'box',
                boxId: box.instance.id,
              });
            model.toggle();
          });
        };

        scope.closePopup = function(id) {
          for (let i = 0; i < scope.popups.length; ++i) {
            if (scope.popups[i].id === id) {
              scope.popups.splice(i, 1);
              return true;
            }
          }
          return false;
        };

        scope.closeLastPopup = function() {
          scope.popups.pop();
        };

        scope.onClickOnPlug = function(plug, event) {
          const leftButton = event.button === 0;
          if (!leftButton || event.ctrlKey || event.shiftKey) {
            return;
          }
          event.stopPropagation();
          if (plug.direction === 'outputs') {
            afterUpdates(() => {
              const model = getPopup(
                event,
                plug.boxId + '_' + plug.id,
                plug.boxInstance.operationId + ' ➡ ' + plug.id,
                {
                  type: 'plug',
                  boxId: plug.boxId,
                  plugId: plug.id,
                });
              model.toggle();
            });
          }
        };

        scope.onMouseDownOnPlug = function(plug, event) {
          event.stopPropagation();
          scope.pulledPlug = plug;
          scope.mouseLogical = undefined;
        };

        scope.onMouseUpOnPlug = function(plug, event) {
          event.stopPropagation();
          if (scope.pulledPlug) {
            const otherPlug = scope.pulledPlug;
            scope.pulledPlug = undefined;
            scope.workspace.addArrow(otherPlug, plug);
          }
        };

        scope.onMouseUp = function() {
          element[0].style.cursor = '';
          workspaceDrag = false;
          selectBoxes = false;
          removeDragListeners();
          scope.selection.remove();
          if (scope.movedBoxes) {
            const req = scope.workspace.saveIfBoxesDirty();
            const placedBox = scope.movedBoxes.filter(b => b.isDirty)[0];
            // We need to wait for the request and wait for the DOM to update.
            // Otherwise we would be replacing the box to which the tutorial is bound.
            // That would cause the "End Tour" button to not work properly.
            req && req.then(() => $timeout(() => tutorialBoxPlaced(placedBox)));
          }
          scope.movedBoxes = undefined;
          scope.pulledPlug = undefined;
          scope.movedPopup = undefined;
        };

        function wrapCallback(callback) {
          return function(event) { scope.$apply(function () { callback(event); }); };
        }
        const wrappedOnMouseMove = wrapCallback(scope.onMouseMove);
        const wrappedOnMouseUp = wrapCallback(scope.onMouseUp);

        scope.startMovingPopup = function(popup) {
          scope.movedPopup = popup;
          addDragListeners();
        };
        function addDragListeners() {
          window.addEventListener('mousemove', wrappedOnMouseMove);
          window.addEventListener('mouseup', wrappedOnMouseUp);
        }
        function removeDragListeners() {
          window.removeEventListener('mousemove', wrappedOnMouseMove);
          window.removeEventListener('mouseup', wrappedOnMouseUp);
        }

        scope.onMouseDown = function(event) {
          addDragListeners();
          const dragMode = actualDragMode(event);
          event.preventDefault();
          addLogicalMousePosition(event);
          if (dragMode === 'pan') {
            workspaceDrag = true;
            setGrabCursor(element[0]);
            mouseX = event.workspaceX;
            mouseY = event.workspaceY;
          } else if (dragMode === 'select') {
            selectBoxes = true;
            scope.selectedBoxIds = [];
            scope.selection.onMouseDown(event);
          }
        };

        scope.workspaceTransform = function() {
          const z = zoomToScale(workspaceZoom);
          return 'translate(' + workspaceX + ', ' + workspaceY + ') scale(' + z + ')';
        };

        scope.boxes = function() {
          if (!scope.workspace) {
            return undefined;
          }
          const boxes = scope.workspace.boxes.slice();
          boxes.sort(function(a, b) { return a.instance.y < b.instance.y ? -1 : 1; });
          return boxes;
        };

        scope.arrows = function() {
          return scope.workspace ? scope.workspace.arrows : [];
        };

        scope.selectBoxesInSelection = function() {
          const boxes = scope.workspace.boxes;
          this.selectedBoxIds = [];
          for (let i = 0; i < boxes.length; i++) {
            const box = boxes[i];
            if (this.selection.inSelection(box)) {
              this.selectedBoxIds.push(box.instance.id);
            }
          }
        };

        scope.selectBox = function(boxId) {
          scope.selectedBoxIds.push(boxId);
        };

        scope.selectedBoxes = function() {
          if (scope.selectedBoxIds) {
            return scope.selectedBoxIds.map(function(id) {
              return scope.workspace.boxMap[id];
            });
          } else {
            return undefined;
          }
        };

        function inputBoxFocused() {
        // We don't want to hijack copy and paste events from input fields.
          return ['INPUT', 'TEXTAREA'].indexOf(document.activeElement.tagName) !== -1;
        }

        /* global jsyaml */
        scope.copyBoxes = function(e) {
          if (inputBoxFocused() || window.getSelection().toString()) {
            return;
          }
          const data = jsyaml.safeDump(
            scope.selectedBoxes().map(function(box) { return box.instance; }),
            { noCompatMode: true, sortKeys: true });
          e.clipboardData.setData('text/plain', data);
          e.preventDefault();
        };

        scope.insertBoxesFromYaml = function(data) {
          data = data.replace(/\xa0/g, ' '); // Convert back non-breaking spaces, e.g. from Gmail.
          let boxes, message;
          try {
            boxes = jsyaml.safeLoad(data);
          } catch (err) {
            const jData = { clipboard: data };
            message = 'Cannot create boxes from clipboard. (Not in JSON format)';
            util.error(message, jData);
            /* eslint-disable no-console */
            console.error(message, err);
            return;
          }
          let added;
          try {
            added = scope.workspace.pasteFromData(boxes, getMouseLogical(-50, -50));
          } catch (err) {
            const someJson = { clipboard: data };
            message = 'Cannot parse boxes from clipboard';
            util.error(message, someJson);
            /* eslint-disable no-console */
            console.error(message, err);
            return;
          }
          scope.selectedBoxIds = added.map(function(box) { return box.id; });
        };

        scope.pasteBoxes = function(e) {
          if (inputBoxFocused()) {
            return;
          }
          const data = e.clipboardData.getData('Text');
          scope.insertBoxesFromYaml(data);
        };

        const wrappedCopyBoxes = wrapCallback(scope.copyBoxes);
        const wrappedPasteBoxes = wrapCallback(scope.pasteBoxes);

        window.addEventListener('copy', wrappedCopyBoxes);
        window.addEventListener('paste', wrappedPasteBoxes);

        scope.deleteBoxes = function(boxIds) {
          const popups = scope.popups.slice();
          popups.forEach(function(popup) {
            const boxId = popup.content.boxId;
            if (boxIds.includes(boxId) && boxId !== 'anchor') {
              scope.closePopup(popup.id);
            }
          });
          scope.workspace.deleteBoxes(boxIds);
        };

        scope.deleteSelectedBoxes = function() {
          this.deleteBoxes(this.selectedBoxIds);
          this.selectedBoxIds = [];
        };

        scope.saveBoxesAsPython = function() {
          if (this.selectedBoxIds.length === 0) { // Nothing is selected, use all boxes
            const boxIds = this.workspace.boxes.map(b => b.instance.id);
            // TODO: add parameter to signal whether decorator code is needed
            pythonCodeGenerator.saveAsPython(this.workspace, boxIds);
          } else {
            pythonCodeGenerator.saveAsPython(this.workspace, this.selectedBoxIds);
          }

        };

        scope.diveUp = function() {
          const boxSettings = scope.workspace.customBoxStack.pop();
          delete scope.workspace._boxCatalogMap; // Force a catalog refresh.
          scope.workspace.loadWorkspace().then(() => {
            scope.popups = [];
            scope.selectedBoxIds = [boxSettings.id];
            workspaceX = boxSettings.viewSettings.x;
            workspaceY = boxSettings.viewSettings.y;
            workspaceZoom = boxSettings.viewSettings.zoom;
          });
        };

        scope.diveDown = function() {
          const boxSettings = {
            id: scope.selectedBoxIds[0],
            viewSettings: {
              x: workspaceX,
              y: workspaceY,
              zoom: workspaceZoom}};
          scope.workspace.customBoxStack.push(boxSettings);
          delete scope.workspace._boxCatalogMap; // Force a catalog refresh.
          scope.workspace.loadWorkspace().then(() => {
            scope.popups = [];
            scope.selectedBoxIds = [];
            workspaceX = 0;
            workspaceY = 0;
            workspaceZoom = 0;
          });
        };

        scope.saveSelectionAsCustomBox = function(name, done) {
          const b = scope.workspace.saveAsCustomBox(
            scope.selectedBoxIds, name, 'Created from ' + scope.workspaceName);
          b.promise.finally(done).then(() => {
            scope.selectedBoxIds = [b.customBox.id];
            scope.saveSelectionAsCustomBoxInputOpen = false;
          });
        };
        const hk = hotkeys.bindTo(scope);
        hk.add({
        // Only here for the tooltip.
          combo: 'mod+c', description: 'Copy boxes', callback: function() {} });
        hk.add({
        // Only here for the tooltip.
          combo: 'mod+v', description: 'Paste boxes', callback: function() {} });
        hk.add({
          combo: 'mod+z', description: 'Undo',
          callback: function() { scope.workspace.undo(); } });
        hk.add({
          combo: 'mod+y', description: 'Redo',
          callback: function() { scope.workspace.redo(); } });
        hk.add({
          combo: 'del', description: 'Delete boxes',
          callback: function() { scope.deleteSelectedBoxes(); } });
        hk.add({
          combo: 'backspace', description: 'Delete boxes',
          callback: function() { scope.deleteSelectedBoxes(); } });
        hk.add({
          combo: '/', description: 'Find operation',
          callback: function(e) {
            e.preventDefault(); // Do not type "/".
            $rootScope.$broadcast('open operation search');
          }});
        hk.add({
          combo: 'escape', description: 'Close last popup',
          callback: function() {
            scope.closeLastPopup();
          }});

        function setGrabCursor(e) {
        // Trying to assign an invalid cursor will silently fail. Try to find a supported value.
          e.style.cursor = '';
          e.style.cursor = 'grabbing';
          if (!e.style.cursor) {
            e.style.cursor = '-webkit-grabbing';
          }
          if (!e.style.cursor) {
            e.style.cursor = '-moz-grabbing';
          }
        }

        element.find('svg').on('wheel', function(event) {
          event.preventDefault();
          let delta = event.originalEvent.deltaY;
          if (/Firefox/.test(window.navigator.userAgent)) {
          // Every browser sets different deltas for the same amount of scrolling.
          // It is tiny on Firefox. We need to boost it.
            delta *= 20;
          }
          scope.$apply(function() {
            const z1 = zoomToScale(workspaceZoom);
            workspaceZoom -= delta;
            // Enforce a maximal zoom where the icons are not yet pixelated.
            workspaceZoom = Math.min(workspaceZoom, 1000 * Math.log(4 / 3));
            // Enforce a minimal zoom where boxes are still visible.
            workspaceZoom = Math.max(workspaceZoom, 1000 * Math.log(1 / 50));
            const z2 = zoomToScale(workspaceZoom);
            // Maintain screen-coordinates of logical point under the mouse.
            workspaceX = mouseX - (mouseX - workspaceX) * z2 / z1;
            workspaceY = mouseY - (mouseY - workspaceY) * z2 / z1;
          });
        });

        scope.addOperation = function(op, event) {
          addLogicalMousePosition(event);
          // Offset event to place icon centered on the cursor.
          // TODO: May be better to center all icons on the logical positions.
          // Then we never need to worry about sizes.
          event.logicalX -= 50;
          event.logicalY -= 50;
          const box = scope.workspace.addBox(op.operationId, event, { willSaveLater: true });
          scope.onMouseDownOnBox(scope.workspace.getBox(box.id), event);
          tutorialDragStart();
        };

        // Insert an import box when a file is dropped on the board.
        element.bind('dragover', function(e) { e.preventDefault(); });
        element.bind('drop', function(event) {
          event = event.originalEvent;
          event.preventDefault();
          addLogicalMousePosition(event);
          event.logicalX -= 50;
          event.logicalY -= 50;
          const file = event.dataTransfer.files[0];
          if (file.name.match(/\.ya?ml$/i)) {
            let reader = new FileReader();
            reader.onload = function(event) {
              scope.insertBoxesFromYaml(event.target.result);
            };
            reader.readAsText(file);
            return;
          }
          let op = 'Import CSV';
          if (file.name.match(/\.json$/i) || file.name.match(/\.json\.gz$/i)) {
            op = 'Import JSON';
          } else if (file.name.match(/\.parquet$/i)) {
            op = 'Import Parquet';
          } else if (file.name.match(/\.orc$/i)) {
            op = 'Import ORC';
          }
          const box = scope.workspace.addBox(op, event, { willSaveLater: true });
          if (op === 'Import CSV') {
            box.parameters.infer = 'yes';
          }
          uploadFile(file).then(function(filename) {
            box.parameters.filename = filename;
            return util.post('/ajax/importBox', {
              box: box,
              ref: scope.workspace.ref(),
            });
          }).then(function(response) {
            box.parameters.imported_table = response.key;
            box.parameters.last_settings = response.parameterSettings;
            scope.workspace.saveWorkspace();
          }).catch(function() {
            scope.workspace.deleteBoxes([box.id]);
          });
        });

        function uploadFile(file) {
          const defer = $q.defer();
          const xhr = new XMLHttpRequest();
          xhr.open('POST', 'ajax/upload');
          xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) { // DONE
              scope.$apply(function() {
                if (xhr.status === 200) { // SUCCESS
                  defer.resolve(xhr.responseText);
                } else {
                  util.error('File upload failed.', { file: file });
                  defer.reject(xhr);
                }
              });
            }
          };
          const fd = new FormData();
          fd.append('file', file);
          xhr.send(fd);
          return defer.promise;
        }

        longPoll.onUpdate(scope, function(status) {
          scope.workspace.updateProgress(status.progress);
        });

        scope.$on('$destroy', function() {
          window.removeEventListener('copy', wrappedCopyBoxes);
          window.removeEventListener('paste', wrappedPasteBoxes);
        });

        // Box colors come as strings from the backend. We map them to feColorMatrix matrices that
        // get applied to the grayscale icon images. This function builds the mapping.
        function makeFilters() {
        // Brand colors.
          const colors = [
            ['blue', '#39bcf3'],
            ['green', '#6fdb11'],
            ['orange', '#ff8800'],
            ['purple', '#bf45e8'],
          ];
          const filters = {};
          for (let i = 0; i < colors.length; ++i) {
            const name = colors[i][0];
            /* global chroma */
            const rgb = chroma(colors[i][1]).rgb();
            filters[name] = (
              (rgb[0] / 255 / 2.3 + ' ').repeat(3) + '0 0 ' +
              (rgb[1] / 255 / 2.3 + ' ').repeat(3) + '0 0 ' +
              (rgb[2] / 255 / 2.3 + ' ').repeat(3) + '0 0   0 0 0 1 0');
          }
          // The "natural" filter leaves the colors alone. This is used for user-specified images.
          filters.natural = '1 0 0 0 0   0 1 0 0 0   0 0 1 0 0   0 0 0 1 0';
          return filters;
        }
        scope.filters = makeFilters();

        scope.bezier = function(x1, y1, x2, y2) {
          return ['M', x1, y1, 'C', x1 + 100, y1, ',', x2 - 100, y2, ',', x2, y2].join(' ');
        };

        scope.getDirectoryPart = function(path) {
          if (path === undefined) { return undefined; }
          const dir = path.split('/').slice(0, -1);
          return dir.length === 0 ? '' : dir.join('/') + '/';
        };

        scope.getLastPart = function(path) {
          if (path === undefined) { return undefined; }
          return path.split('/').slice(-1)[0];
        };

        scope.closeWorkspace = function() {
          const dir = scope.workspace.name.split('/').slice(0, -1).join('/');
          $location.url('/dir/' + dir);
        };

        scope.$on('create box under mouse', createBoxUnderMouse);
        function createBoxUnderMouse(event, operationId) {
          addAndSelectBox(operationId, getMouseLogical(-50, -50));
        }

        // This is separate from scope.addOperation because we don't have a mouse event here,
        // which makes using the onMouseDown function pretty difficult.
        function addAndSelectBox(id, pos, options) {
          const box = scope.workspace.addBox(id, { logicalX: pos.x, logicalY: pos.y }, options);
          scope.selectedBoxIds = [box.id];
        }

        function getMouseLogical(offx, offy) {
          const z = zoomToScale(workspaceZoom);
          return { x: (mouseX - workspaceX) / z + offx, y: (mouseY - workspaceY) / z + offy };
        }

        scope.deleteArrow = function(arrow) {
          scope.workspace.deleteArrow(arrow);
        };

        function showTutorial() {
          if (!util.user.$resolved) { // Wait for user data before deciding to show it.
            util.user.then(showTutorial);
            return;
          }
          if (util.user.wizardOnly || localStorage.getItem('workspace-drawing-board tutorial done')) {
            return;
          }
          /* global Tour */
          scope.tutorial = new Tour({
            autoscroll: false,
            framework: 'bootstrap3',
            storage: null,
            backdrop: true,
            showProgressBar: false,
            steps: [
              {
                orphan: true,
                content: `
                <p>This is a LynxKite workspace.
                `,
                animation: false,
              }, {
                placement: 'bottom',
                element: '#workspace-name',
                content: `
                <p>The well-chosen name of your workspace.
                `,
                animation: false,
              }, {
                placement: 'bottom',
                element: '#workspace-toolbar-buttons',
                content: `
                <p>These buttons are pretty useful!

                <p>Most importantly, <i class="glyphicon glyphicon-remove"></i>
                closes the workspace and gets you back to the file browser.
                The workspace is automatically saved.

                <p>Don't click it just yet!
                `,
                animation: false,
              }, {
                placement: 'left',
                element: '.operation-selector',
                content: `
                <p>This is the most important part. Each icon here is a category
                of boxes that you can use in your workspace.

                <p>Click a category to open it. Or use <i class="glyphicon glyphicon-search"></i>
                to search for a box by name. (Hotkey: <code>/</code>)

                <p><i>The tutorial will continue when you open a category.</i>
                `,
                animation: false,
              },
            ],
            onEnd: function() {
              if (!scope.tutorial.toBeContinued) {
                // Manual abort before we had reached the last step.
                localStorage.setItem('workspace-drawing-board tutorial done', 'true');
                delete scope.tutorial;
              }
            },
          });

          scope.tutorial.start();
        }

        scope.tutorialCategoryOpen = function() {
          if (!scope.tutorial) {
            return;
          }
          scope.tutorial.addStep({
            placement: 'left',
            element: '.operation-selector .box:not(.ng-hide)',
            content: `
            <p>Grab one of the box names with your mouse and drag it into the workspace!
            `,
          });
          // Let the category actually open before going to the new step.
          setTimeout(function() { scope.tutorial.next(); });
        };

        function tutorialDragStart() {
          if (scope.tutorial) {
            scope.tutorial.toBeContinued = true;
            scope.tutorial.end();
          }
        }

        function tutorialBoxPlaced(box) {
          if (!scope.tutorial) {
            return;
          }
          const element = '#' + box.instance.id.toLowerCase();
          scope.tutorial = new Tour({
            autoscroll: false,
            framework: 'bootstrap3',
            storage: null,
            backdrop: true,
            showProgressBar: false,
            steps: [
              {
                placement: 'auto',
                element,
                content: `
                <p>Now the box is in the workspace.

                <p>A box can have inputs (circles on its left side) and outputs (circles on its
                right side).

                <p>You can connect one box to another by holding and dragging the mouse from an
                output to an input. Or if you move the boxes so that the circles touch, a connection
                will be added between them.

                <p>This makes it easy to build up your pipeline!
                `,
                animation: false,
              }, {
                placement: 'auto',
                element,
                content: `
                <p>You can click on a box to change its parameters.

                <p>Or you can click on an output circle to see the output state.

                <p>The inputs and outputs of boxes can be various states. A "graph" state
                is a rich collection of graphs and their attributes. A "table" state
                is a traditional SQL table. There are a few other states, such as visualizations.
                `,
                animation: false,
              }, {
                placement: 'auto',
                element,
                content: `
                <p>That's it! Drag boxes from the catalog, wire them together, set their parameters,
                and look at their outputs.

                <p>A good box to start with is the <i>Create example graph</i> box in the
                <i>Build graph</i> <i class="fas fa-gavel"></i> category.

                <p>If you want to learn more, be sure to follow the
                <i class="glyphicon glyphicon-question-sign"></i> signs, skim the
                <a href="/#/help" target="_blank">LynxKite User Guide</a>, or look up
                some of our <a href="https://lynxkite.com/docs/latest">tutorials</a>.
                `,
                animation: false,
              },
            ],
            onEnd: function() {
              localStorage.setItem('workspace-drawing-board tutorial done', 'true');
              delete scope.tutorial;
            },
          });

          scope.tutorial.start();
        }

        showTutorial();
        scope.$on('start tutorial', function() {
          localStorage.removeItem('workspace-drawing-board tutorial done');
          showTutorial();
        });
        scope.$on('$destroy', function() { scope.tutorial && scope.tutorial.end(); });
      }
    };
  });
