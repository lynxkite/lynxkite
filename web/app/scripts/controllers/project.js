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
    util.scopeTitle($scope, 'left.shortName()');

    function defaultSideState() {
      return {
        projectName: undefined,
        filters: {
          edge: {},
          vertex: {},
        },
        axisOptions: {
          edge: {},
          vertex: {},
        },
        graphMode: undefined,
        bucketCount: 4,
        sampleRadius: 1,
        display: 'svg',
        animate: {
          enabled: false,
          labelAttraction: 0,
        },
        attributeTitles: {},
      };
    }

    function Side(options) {
      var that = this;
      angular.forEach(options, function(value, key) {
        that[key] = value;
      });
      // The state of controls. E.g. bucket count.
      this.state = defaultSideState();
      // Everything needed for a view (state included).
      // Use this for rendering graph view instead of using state directly.
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
      if (this.project.edgeBundle) {
        vd.edgeBundle = { id: this.project.edgeBundle };
      } else {
        vd.edgeBundle = undefined;
      }
      vd.graphMode = this.state.graphMode;
      vd.display = this.state.display;

      vd.bucketCount = this.state.bucketCount;

      // "state" uses attribute names, while "viewData" uses attribute UUIDs.
      vd.xAttribute = this.resolveVertexAttribute(this.state.attributeTitles.x);
      vd.yAttribute = this.resolveVertexAttribute(this.state.attributeTitles.y);
      vd.xAxisOptions = this.axisOptions('vertex', this.state.attributeTitles.x);
      vd.yAxisOptions = this.axisOptions('vertex', this.state.attributeTitles.y);
      vd.attrs = {};
      vd.attrs.size = this.resolveVertexAttribute(this.state.attributeTitles.size);
      vd.attrs.label = this.resolveVertexAttribute(this.state.attributeTitles.label);
      vd.attrs.color = this.resolveVertexAttribute(this.state.attributeTitles.color);
      vd.attrs.slider = this.resolveVertexAttribute(this.state.attributeTitles.slider);
      vd.attrs.icon = this.resolveVertexAttribute(this.state.attributeTitles.icon);
      vd.attrs.image = this.resolveVertexAttribute(this.state.attributeTitles.image);

      vd.edgeWidth = this.resolveEdgeAttribute(this.state.attributeTitles.width);

      vd.filters = {
        edge: this.nonEmptyEdgeFilters(),
        vertex: this.nonEmptyVertexFilters(),
      };

      vd.centers = this.state.centers || [];
      var that = this;
      vd.hasCenter = function(id) { return that.state.centers.indexOf(id) !== -1; };
      vd.setCenter = function(id) { that.state.centers = [id]; };
      vd.addCenter = function(id) { that.state.centers = that.state.centers.concat([id]); };
      vd.removeCenter = function(id) {
        that.state.centers =
          that.state.centers.filter(function(element) { return element !== id; });
      };
      vd.sampleRadius = this.state.sampleRadius;
      vd.animate = this.state.animate;
      vd.sliderPos = this.state.sliderPos;

      vd.hasParent = function() {
        return that.getParentSide() !== undefined;
      };
      vd.parentFilters = function() {
        return that.getParentSide().state.filters.vertex;
      };
      vd.parentFilterName = function() {
        return that.getParentSide().getSegmentationEntry(that).equivalentAttribute.title;
      };
      vd.filterValue = function(id) {
        return 'exists(' + id + ')';
      };
      vd.filterParentToSegment = function(segmentId) {
        vd.parentFilters()[vd.parentFilterName()] = vd.filterValue(segmentId);
      };
      vd.isParentFilteredToSegment = function(segmentId) {
        return vd.parentFilters()[vd.parentFilterName()] === vd.filterValue(segmentId);
      };
      vd.deleteParentsSegmentFilter = function() {
        delete vd.parentFilters()[vd.parentFilterName()];
      };

      vd.hasSegmentation = function() {
        return that.getSegmentationSide() !== undefined;
      };
      vd.segmentationFilters = function() {
        return that.getSegmentationSide().state.filters.vertex;
      };
      vd.filterSegmentationToParent = function(parentId) {
        vd.segmentationFilters().$members = vd.filterValue(parentId);
      };
      vd.isSegmentationFilteredToParent = function(parentId) {
        return vd.segmentationFilters().$members === vd.filterValue(parentId);
      };
      vd.deleteSegmentationsParentFilter = function() {
        delete vd.segmentationFilters().$members;
      };

      vd.setVertexFilter = function(title, value) {
        that.state.filters.vertex[title] = value;
      };

      this.viewData = vd;
    };

    Side.prototype.axisOptions = function(type, attr) {
      var defaultAxisOptions = {
        logarithmic: false,
      };
      return this.state.axisOptions[type][attr] || defaultAxisOptions;
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
        filters: this.nonEmptyVertexFilters() || '',
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
      sendReloadNotification();
    };

    Side.prototype.load = function() {
      return util.nocache('/ajax/project', { name: this.state.projectName });
    };

    Side.prototype.loaded = function() {
      return this.project && this.project.$resolved && !this.project.$error;
    };

    Side.prototype.reportLoadingError = function() {
      return util.reportRequestError(
          this.project, 'Could not load project: ' + this.state.projectName);
    };

    Side.prototype.toggleAttributeTitle = function(setting, value) {
      if (this.state.attributeTitles[setting] === value) {
        // Clicking the same attribute setting again turns it off.
        delete this.state.attributeTitles[setting];
      } else {
        this.state.attributeTitles[setting] = value;
        // Apply mutual exclusions.
        if (setting === 'slider') {
          this.state.attributeTitles.color = undefined;
        } else if (setting === 'color') {
          this.state.attributeTitles.image = undefined;
          this.state.attributeTitles.slider = undefined;
        } else if (setting === 'icon') {
          this.state.attributeTitles.image = undefined;
        } else if (setting === 'image') {
          this.state.attributeTitles.color = undefined;
          this.state.attributeTitles.icon = undefined;
        }
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

    Side.prototype.duplicate = function(kind, name) {
      this.applyOp('Copy-' + kind, { from: name, to: 'Copy of ' + name });
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
              if (side.project && seg.fullName === side.project.name) {
                side.close();
              } else {
                break;
              }
            }
          }
        }
      }
      this.applyOp('Discard-' + kind, { name: name });
    };

    // Returns unresolved filters (i.e. keyed by the attribute name).
    Side.prototype.nonEmptyVertexFilterNames = function() {
      return this.nonEmptyFilterNames(this.state.filters.vertex);
    };
    Side.prototype.nonEmptyEdgeFilterNames = function() {
      return this.nonEmptyFilterNames(this.state.filters.edge);
    };
    Side.prototype.nonEmptyFilterNames = function(filters) {
      var res = [];
      for (var attr in filters) {
        if (filters[attr] !== '') {
          res.push({
            attributeName: attr,
            valueSpec: filters[attr] });
        }
      }
      return res;
    };

    // Returns resolved filters (i.e. keyed by UUID).
    Side.prototype.nonEmptyVertexFilters = function() {
      var that = this;
      return this.nonEmptyVertexFilterNames().map(function(f) {
        return {
          attributeId: that.resolveVertexAttribute(f.attributeName).id,
          valueSpec: f.valueSpec,
        };
      });
    };
    Side.prototype.nonEmptyEdgeFilters = function() {
      var that = this;
      return this.nonEmptyEdgeFilterNames().map(function(f) {
        return {
          attributeId: that.resolveEdgeAttribute(f.attributeName).id,
          valueSpec: f.valueSpec,
        };
      });
    };

    Side.prototype.hasFilters = function() {
      return (this.nonEmptyEdgeFilterNames().length !== 0 ||
              this.nonEmptyVertexFilterNames().length !== 0);
    };
    Side.prototype.applyFilters = function() {
      var that = this;
      util.post('/ajax/filterProject',
        {
          project: this.state.projectName,
          edgeFilters: this.nonEmptyEdgeFilterNames(),
          vertexFilters: this.nonEmptyVertexFilterNames(),
        },
        function() {
          that.clearFilters();
          that.reload();
        });
    };
    Side.prototype.clearFilters = function() {
      this.state.filters = { edge: {}, vertex: {} };
    };
    Side.prototype.filterSummary = function() {
      var res = [];
      function addNonEmpty(value, key) {
        if (value) {
          var nbsp = '\u00a0';
          res.push(' ' + key + nbsp + value);
        }
      }
      angular.forEach(this.state.filters.vertex, addNonEmpty);
      angular.forEach(this.state.filters.edge, addNonEmpty);
      return res.join(', ');
    };

    Side.prototype.resolveVertexAttribute = function(title) {
      for (var attrIdx = 0; attrIdx < this.project.vertexAttributes.length; attrIdx++) {
        var attr = this.project.vertexAttributes[attrIdx];
        if (attr.title === title) {
          return attr;
        }
      }
      for (var segIdx = 0; segIdx < this.project.segmentations.length; segIdx++) {
        var sattr = this.project.segmentations[segIdx].equivalentAttribute;
        if (sattr.title === title) {
          return { id: sattr.id, title: title };
        }
      }
      return undefined;
    };

    Side.prototype.resolveEdgeAttribute = function(title) {
      for (var attrIdx = 0; attrIdx < this.project.edgeAttributes.length; attrIdx++) {
        var attr = this.project.edgeAttributes[attrIdx];
        if (attr.title === title) {
          return attr;
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
        var res = util.get('/ajax/scalarValue', { scalarId: s.id, calculate: true });
        res.details = { project: this.state.projectName, scalar: s };
        this.scalars[s.title] = res;
      }
    };

    Side.prototype.isSegmentationOf = function(parent) {
      return parent.getBelongsTo(this) !== undefined;
    };
    Side.prototype.getBelongsTo = function(segmentation) {
      var entry = this.getSegmentationEntry(segmentation);
      if (entry) {
        return entry.belongsTo;
      }
      return undefined;
    };
    Side.prototype.getSegmentationEntry = function(segmentation) {
      if (!this.loaded()) { return undefined; }
      if (!segmentation.project || !segmentation.project.$resolved) { return undefined; }
      for (var i = 0; i < this.project.segmentations.length; ++i) {
        var seg = this.project.segmentations[i];
        if (segmentation.project.name === seg.fullName) {
          return seg;
        }
      }
      return undefined;
    };
    Side.prototype.getParentSide = function() {
      for (var i = 0; i < $scope.sides.length; ++i) {
        var side = $scope.sides[i];
        if (side === this) { continue; }
        if (side.getSegmentationEntry(this)) {
          return side;
        }
      }
      return undefined;
    };
    Side.prototype.getSegmentationSide = function() {
      for (var i = 0; i < $scope.sides.length; ++i) {
        var side = $scope.sides[i];
        if (side === this) { continue; }
        if (side.isSegmentationOf(this)) {
          return side;
        }
      }
      return undefined;
    };

    // Called when Side.project is loaded.
    Side.prototype.onProjectLoaded = function() {
      this.cleanState();
      $scope.leftToRightBundle = getLeftToRightBundle();
      $scope.rightToLeftBundle = getRightToLeftBundle();
      this.loadScalars();
      this.updateViewData();
      if (!this.project.vertexSet) {
        this.state.graphMode = undefined;
      }
    };

    // Removes entries from state which depend on nonexistent attributes
    Side.prototype.cleanState = function() {
      if (!this.loaded()) { return; }
      var vTitles = this.project.vertexAttributes.map(function(a) { return a.title; });
      var eTitles = this.project.edgeAttributes.map(function(a) { return a.title; });
      for (var attr in this.state.filters.edge) {
        if (eTitles.indexOf(attr) === -1) {
          delete this.state.filters.edge[attr];
        }
      }
      for (attr in this.state.filters.vertex) {
        if (vTitles.indexOf(attr) === -1) {
          delete this.state.filters.vertex[attr];
        }
      }
      for (attr in this.state.axisOptions.vertex) {
        if (vTitles.indexOf(attr) === -1) {
          delete this.state.axisOptions.vertex[attr];
        }
      }
      for (attr in this.state.axisOptions.edge) {
        if (eTitles.indexOf(attr) === -1) {
          delete this.state.axisOptions.edge[attr];
        }
      }
      for (attr in this.state.attributeTitles) {
        if (attr === 'width') {
          if (eTitles.indexOf(this.state.attributeTitles[attr]) === -1) {
            delete this.state.attributeTitles[attr];
          }
        } else {
          if (vTitles.indexOf(this.state.attributeTitles[attr]) === -1) {
            delete this.state.attributeTitles[attr];
          }
        }
      }
    };

    // "vertex_count" and "edge_count" are displayed separately at the top.
    $scope.commonScalar = function(s) {
      return s.title !== 'vertex_count' && s.title !== 'edge_count';
    };

    $scope.unconnectedSides = function() {
      return (
        $scope.left.loaded() && $scope.right.loaded() &&
        !$scope.right.isSegmentationOf($scope.left)
      );
    };
    function getLeftToRightBundle() {
      var left = $scope.left;
      var right = $scope.right;
      if (!left.loaded() || !right.loaded()) { return undefined; }
      // If it is a segmentation, use "belongsTo" as the connecting path.
      if (right.isSegmentationOf(left)) {
        return left.getBelongsTo(right).id;
      }
      // If it is the same project on both sides, use its internal edges.
      if (left.project.name === right.project.name) {
        return left.project.edgeBundle;
      }
      return undefined;
    }

    function getRightToLeftBundle() {
      var left = $scope.left;
      var right = $scope.right;
      if (!left.loaded() || !right.loaded()) { return undefined; }
      // If it is the same project on both sides, use its internal edges.
      if (left.project.name === right.project.name) {
        return left.project.edgeBundle;
      }
      return undefined;
    }

    $scope.showGraph = function() {
      return $scope.left.viewData || $scope.right.viewData;
    };

    $scope.$watch('left.project.$resolved', function(loaded) {
      if (loaded) { $scope.left.onProjectLoaded(); } });
    $scope.$watch('right.project.$resolved', function(loaded) {
      if (loaded) { $scope.right.onProjectLoaded(); } });

    $scope.left = new Side({ primary: true });
    $scope.right = new Side();
    $scope.sides = [$scope.left, $scope.right];

    $scope.$watch('left.state.projectName', function() { $scope.left.reload(); });
    $scope.$watch('right.state.projectName', function() { $scope.right.reload(); });
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
          $scope.left.state = afterState.left;
          $scope.right.state = afterState.right;
          console.log('Loaded state from URL:', afterState);
        }
        if (after.link) {
          $scope.linkChannel = after.link;
          console.log('Tuned in to parent\'s link channel:', after.link);
          $location.search('link', null);
        }
      });

    function parseState(search) {
      var state = {};
      if (search.q === undefined) {
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
        localStorage.setItem($scope.linkChannel, JSON.stringify(after));
      });

    // Persist channel name across refreshes.
    var randomChannel = 'channel-' + Math.random().toString(36);
    $scope.linkChannel = sessionStorage.getItem('link') || randomChannel;
    sessionStorage.setItem('link', $scope.linkChannel);
    console.log('link channel is:', $scope.linkChannel);

    // Handle state change and reload notifications from other windows.
    function updateFromAnotherWindow(e) {
      if (e.key === $scope.linkChannel) {
        var oldState = JSON.parse(e.oldValue);
        var newState = JSON.parse(e.newValue);
        if (angular.equals(oldState, getState())) {
          $scope.$apply(function() {
            $scope.left.state = newState.left;
            $scope.right.state = newState.right;
          });
        }
      } else if (e.key === 'reload:' + $scope.linkChannel) {
        // Unconditionally reload everything.
        for (var i = 0; i < $scope.sides.length; ++i) {
          var side = $scope.sides[i];
          if (side.state.projectName) {
            side.project = side.load();
          } else {
            side.project = undefined;
          }
        }
      } else if (e.key === 'ping') {
        wiggleChannel('pong:' + $scope.linkChannel);
      } else if (e.key.indexOf('pong:') === 0) {
        pongs[e.key.substring('pong:'.length)] = true;
      }
    }
    // This listener is only triggered on localStorage changes from another window.
    // If the change originates from this window, or the new value matches the old value,
    // it will not be triggered. sessionStorage changes also do not trigger it.
    window.addEventListener('storage', updateFromAnotherWindow);
    $scope.$on('$destroy', function() {
      window.removeEventListener('storage', updateFromAnotherWindow);
    });

    // URL for a linked window.
    $scope.linkedURL = function() {
      if (Object.keys($location.search()).length > 0) {
        return $location.absUrl() + '&link=' + $scope.linkChannel;
      } else {
        return $location.absUrl() + '?link=' + $scope.linkChannel;
      }
    };

    function wiggleChannel(channel) {
      // Write a random string to almost certainly trigger a storage event.
      localStorage.setItem(channel, Date() + Math.random().toString(36));
    }

    function sendReloadNotification() {
      wiggleChannel('reload:' + $scope.linkChannel);
    }

    var pongs = {};
    function cleanChannels() {
      wiggleChannel('ping');
      var deadlineSeconds = 10;
      setTimeout(function() {
        // Delete unresponsive channels.
        for (var i = 0; i < localStorage.length; ++i) {
          var key = localStorage.key(i);
          var parts = key.split(':');
          var channel = null;
          if (key.indexOf('channel-') === 0) { channel = key; }
          else if (parts[0] === 'reload') { channel = parts[1]; }
          else if (parts[0] === 'pong') { channel = parts[1]; }
          if (channel !== null && channel !== $scope.linkChannel && !pongs[channel]) {
            localStorage.removeItem(key);
          }
        }
      }, deadlineSeconds * 1000);
    }
    cleanChannels();

    function getState() {
      return {
        left: $scope.left.state,
        right: $scope.right.state,
      };
    }
  });
