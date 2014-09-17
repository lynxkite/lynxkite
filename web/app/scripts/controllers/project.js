'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $location, util, hotkeys) {
    var hk = hotkeys.bindTo($scope);
    hk.add({
      combo: 'ctrl+z', description: 'Undo',
      callback: function() { $scope.left.undo(); } });
    hk.add({
      combo: 'ctrl+y', description: 'Redo',
      callback: function() { $scope.left.redo(); } });
    hk.add({
      combo: 'l b', description: 'Bucketed view (left)',
      callback: function() { $scope.left.state.graphMode = 'bucketed'; } });
    hk.add({
      combo: 'l s', description: 'Sampled view (left)',
      callback: function() { $scope.left.state.graphMode = 'sampled'; } });
    hk.add({
      combo: 'l x', description: 'Close graph (left)',
      callback: function() { $scope.left.state.graphMode = undefined; } });
    hk.add({
      combo: 'r b', description: 'Bucketed view (right)',
      callback: function() { $scope.right.state.graphMode = 'bucketed'; } });
    hk.add({
      combo: 'r s', description: 'Sampled view (right)',
      callback: function() { $scope.right.state.graphMode = 'sampled'; } });
    hk.add({
      combo: 'r x', description: 'Close graph (right)',
      callback: function() { $scope.right.state.graphMode = undefined; } });
    $scope.util = util;

    function defaultSideState() {
      return {
        projectName: undefined,
        filters: {},
        graphMode: undefined,
        bucketCount: 4,
        sampleRadius: 1,
      };
    }

    function Side(options) {
      var that = this;
      angular.forEach(options, function(value, key) {
        that[key] = value;
      });
      // The state of controls. E.g. bucket count.
      this.state = defaultSideState();
      // Everything needed for a view (state included), use this for rendering graph view instead of using state directly.
      this.viewData = {};
      // The /ajax/project Ajax response.
      this.project = undefined;
    }

    Side.prototype.updateViewData = function() {
      var vd = this.viewData || {};
      if (!this.loaded() || !this.state.graphMode ||
          (this.state.graphMode === 'sampled' && !this.state.centers)) {
        this.viewData = undefined;
        return;
      }

      vd.vertexSet = { id: this.project.vertexSet };
      if (this.project.edgeBundle) { vd.edgeBundle = { id: this.project.edgeBundle }; }
      vd.graphMode = this.state.graphMode;

      vd.bucketCount = this.state.bucketCount;

      // "state" uses attribute names, while "viewData" uses attribute UUIDs.
      vd.xAttribute = this.resolveVertexAttribute(this.state.xAttributeTitle);
      vd.yAttribute = this.resolveVertexAttribute(this.state.yAttributeTitle);
      vd.sizeAttribute = this.resolveVertexAttribute(this.state.sizeAttributeTitle);
      vd.labelAttribute = this.resolveVertexAttribute(this.state.labelAttributeTitle);

      vd.filters = {};
      for(var name in this.state.filters) {
        vd.filters[this.resolveVertexAttribute(name)] = this.state.filters[name];
      }

      vd.centers = this.state.centers || [];
      var that = this;
      vd.setCenter = function(id) { that.state.centers = [id]; };
      vd.sampleRadius = this.state.sampleRadius;
      vd.animate = this.state.animate;

      this.viewData = vd;
    };

    Side.prototype.maybeRequestNewCenter = function() {
      if (this.state.graphMode === 'sampled' && !this.state.centers) {
        this.requestNewCenter(1);
      }
    };
    Side.prototype.requestRandomCenter = function() {
      var that = this;
      this.requestNewCenter(100).then(function() {
        var centers = that.state.centers;
        var i = Math.floor(Math.random() * centers.length);
        that.state.centers = [centers[i]];
      });
    };
    Side.prototype.requestNewCenter = function(count) {
      var params = {
        vertexSetId: this.project.vertexSet,
        filters: this.nonEmptyFilters() || '',
        count: count,
      };
      var that = this;
      this.centerRequest = util.get('/ajax/center', params);
      return this.centerRequest.$promise.then(
        function(result) { that.state.centers = result.centers; },
        function(response) { util.ajaxError(response); }
      );
    };

    Side.prototype.shortName = function() {
      var name = this.state.projectName;
      if (!name) { return undefined; }
      var parts = name.split('/');
      if (parts[parts.length - 1] === 'project') {
        parts.pop();
      }
      return util.spaced(parts[parts.length - 1]);
    };
    Side.prototype.parentProjects = function() {
      var name = this.state.projectName;
      if (!name) { return []; }
      var parts = name.split('/');
      var parents = [];
      while (parts.length > 0) {
        if (parts[0] === 'segmentations') {
          parts.shift();  // "segmentations"
          parents.push(util.spaced(parts.shift()));  // segmentation name
          parts.shift();  // "project"
        } else {
          parents.push(util.spaced(parts.shift()));
        }
      }
      parents.pop();  // The last one is the project name.
      return parents;
    };

    // Side.reload makes an unconditional, uncached Ajax request.
    // This is called when the project name changes, or when the project
    // itself is expected to change. (Such as after an operation.)
    Side.prototype.reload = function() {
      if (this.state.projectName) {
        var newProject = this.load();  // The old project is used to look for segmentations.
        for (var i = 0; i < $scope.sides.length; ++i) {
          var side = $scope.sides[i];
          if (side === this) { continue; }
          // If this project is open on the other side, update that instance too.
          if (side.state.projectName === this.state.projectName) {
            side.project = newProject;
          }
          // If a segmentation or parent is open, reload it as well.
          if (side.isSegmentationOf(this) || this.isSegmentationOf(side)) {
            side.project = side.load();
          }
        }
        this.project = newProject;
      } else {
        this.project = undefined;
      }
    };

    Side.prototype.load = function() {
      return util.nocache('/ajax/project', { name: this.state.projectName });
    };

    Side.prototype.loaded = function() {
      return this.project && this.project.$resolved && !this.project.error;
    };

    Side.prototype.set = function(setting, value) {
      if (this.state[setting] === value) {
        // Clicking the same attribute setting again turns it off.
        delete this.state[setting];
      } else {
        this.state[setting] = value;
      }
    };

    Side.prototype.close = function() {
      this.state.projectName = undefined;
      for (var i = 0; i < $scope.sides.length; ++i) {
        if ($scope.sides[i].state.projectName !== undefined) {
          return;
        }
      }
      $location.url('/');
    };

    Side.prototype.saveAs = function(newName) {
      var that = this;
      util.post('/ajax/forkProject',
        {
          from: this.state.projectName,
          to: newName,
        },
        function() {
          that.state.projectName = newName;
        });
    };

    Side.prototype.undo = function() {
      var that = this;
      util.post('/ajax/undoProject',
        {
          project: this.state.projectName,
        },
        function() {
          that.reload();
        });
    };
    Side.prototype.redo = function() {
      var that = this;
      util.post('/ajax/redoProject',
        {
          project: this.state.projectName,
        },
        function() {
          that.reload();
        });
    };

    // Returns a promise.
    Side.prototype.applyOp = function(op, params) {
      var that = this;
      return util.post('/ajax/projectOp',
        {
          project: this.state.projectName,
          op: { id: op, parameters: params },
        },
        function() {
          that.reload();
        });
    };

    Side.prototype.saveNotes = function() {
      var that = this;
      this.savingNotes = true;
      this.applyOp('Change-project-notes', { notes: this.project.notes })
        .then(function(success) {
        if (success) {
          that.unsavedNotes = false;
          that.savingNotes = false;
        }
      });
    };

    Side.prototype.rename = function(kind, oldName, newName) {
      if (oldName === newName) { return; }
      this.applyOp('Rename-' + kind, { from: oldName, to: newName });
    };

    Side.prototype.discard = function(kind, name) {
      // if the other side is the segmentation to be discarded, close it
      if (kind === 'segmentation') {
        for (var i = 0; i < $scope.sides.length; ++i) {
          var side = $scope.sides[i];
          if (side === this) { continue; }
          for (var j = 0; j < this.project.segmentations.length; ++j) {
            var seg = this.project.segmentations[j];
            if (seg.name === name) {
              if (side.project && seg.fullName === side.project.name) { side.close(); } else { break; }
            }
          }
        }
      }
      this.applyOp('Discard-' + kind, { name: name });
    };

    // Returns resolved filters (i.e. keyed by UUID).
    Side.prototype.nonEmptyFilters = function() {
      var res = [];
      for (var attr in this.state.filters) {
        if (this.state.filters[attr] !== '') {
          res.push({
            attributeId: this.resolveVertexAttribute(attr),
            valueSpec: this.state.filters[attr] });
        }
      }
      return res;
    };
    Side.prototype.hasFilters = function() {
      return this.nonEmptyFilters().length !== 0;
    };
    Side.prototype.applyFilters = function() {
      var that = this;
      util.post('/ajax/filterProject',
        {
          project: this.state.projectName,
          filters: this.nonEmptyFilters()
        },
        function() {
          that.state.filters = {};
          that.reload();
        });
    };
    Side.prototype.resolveVertexAttribute = function(title) {
      for (var attrIdx = 0; attrIdx < this.project.vertexAttributes.length; attrIdx++) {
        var attr = this.project.vertexAttributes[attrIdx];
        if (attr.title === title) {
          return attr.id;
        }
      }
      return undefined;
    };

    Side.prototype.openSegmentation = function(seg) {
      // For now segmentations always open on the right.
      $scope.right.state.projectName = seg.fullName;
    };

    Side.prototype.loadScalars = function() {
      if (!this.loaded()) { return; }
      var scalars = this.project.scalars;
      this.scalars = {};
      for (var i = 0; i < scalars.length; ++i) {
        var s = scalars[i];
        this.scalars[s.title] = util.get('/ajax/scalarValue', { scalarId: s.id });
      }
    };

    Side.prototype.isSegmentationOf = function(parent) {
      return parent.getBelongsTo(this) !== undefined;
    };
    Side.prototype.getBelongsTo = function(segmentation) {
      if (!this.loaded()) { return undefined; }
      if (!segmentation.project || !segmentation.project.$resolved) { return undefined; }
      for (var i = 0; i < this.project.segmentations.length; ++i) {
        var seg = this.project.segmentations[i];
        if (segmentation.project.name === seg.fullName) {
          return seg.belongsTo;
        }
      }
      return undefined;
    };

    // "vertex_count" and "edge_count" are displayed separately at the top.
    $scope.commonScalar = function(s) {
      return s.title !== 'vertex_count' && s.title !== 'edge_count';
    };

    function getLeftToRightPath() {
      var left = $scope.left;
      var right = $scope.right;
      if (!left.loaded() || !right.loaded()) { return undefined; }
      // If it is a segmentation, use "belongsTo" as the connecting path.
      if (right.isSegmentationOf(left)) {
        return [{ bundle: left.getBelongsTo(right), pointsLeft: false }];
      }
      // If it is the same project on both sides, use its internal edges.
      if (left.project.name === right.project.name) {
        return [{ bundle: { id: left.project.edgeBundle }, pointsLeft: false }];
      }
      return undefined;
    }

    $scope.showGraph = function() {
      return $scope.left.viewData || $scope.right.viewData;
    };

    $scope.$watch('left.project.$resolved', function() { $scope.leftToRightPath = getLeftToRightPath(); });
    $scope.$watch('right.project.$resolved', function() { $scope.leftToRightPath = getLeftToRightPath(); });
    $scope.$watch('left.project.$resolved', function() { $scope.left.loadScalars(); });
    $scope.$watch('right.project.$resolved', function() { $scope.right.loadScalars(); });

    $scope.left = new Side({ primary: true });
    $scope.right = new Side();
    $scope.sides = [$scope.left, $scope.right];

    $scope.$watch('left.state.projectName', function() { $scope.left.reload(); });
    $scope.$watch('right.state.projectName', function() { $scope.right.reload(); });
    $scope.$watch('left.project.$resolved', function() { $scope.left.updateViewData(); });
    $scope.$watch('right.project.$resolved', function() { $scope.right.updateViewData(); });
    util.deepWatch($scope, 'left.state', function() { $scope.left.updateViewData(); });
    util.deepWatch($scope, 'right.state', function() { $scope.right.updateViewData(); });
    $scope.$watch('left.state.graphMode', function() { $scope.left.maybeRequestNewCenter(); });
    $scope.$watch('right.state.graphMode', function() { $scope.right.maybeRequestNewCenter(); });

    // This watcher copies the state from the URL into $scope.
    // It is an important part of initialization. Less importantly it makes
    // it possible to edit the state manually in the URL, or use the "back"
    // button to undo state changes.
    util.deepWatch(
      $scope,
      function() { return $location.search(); },
      function(after, before) {
        var beforeState = parseState(before);
        // We are only interested in this change, if the old URL reflected
        // the current state. Otherwise the change in the state triggered the
        // change in the URL (from the watcher below). In this case we are
        // already at the state reflected in the URL, or even further ahead
        // of it. Plus we also load the state if this is the initial loading
        // of the page.
        var initialLoad = before.q === after.q;
        if (initialLoad || angular.equals(beforeState, getState())) {
          var afterState = parseState(after);
          $scope.leftToRightPath = afterState.leftToRightPath;
          $scope.left.state = afterState.left;
          $scope.right.state = afterState.right;
          console.log('Loaded state from URL:', afterState);
        }
      });

    function parseState(search) {
      var state = {};
      if (search.q === undefined) {
        state.leftToRightPath = undefined;
        state.left = defaultSideState();
        state.right = defaultSideState();
        // In the absence of query parameters, take the left-side project
        // name from the URL. This makes for friendlier project links.
        state.left.projectName = $routeParams.project;
      } else {
        state = JSON.parse(search.q);
      }
      return state;
    }

    util.deepWatch(
      $scope,
      getState,
      function(after, before) {
        if (after === before) {
          return;  // Do not modify URL on initialization.
        }
        if ($location.path().indexOf('/project/') === -1) {
          return;  // Navigating away. Leave the URL alone.
        }
        $location.search({ q: JSON.stringify(after) });
      });

    function getState() {
      return {
        leftToRightPath: $scope.leftToRightPath,
        left: $scope.left.state,
        right: $scope.right.state,
      };
    }
  });
