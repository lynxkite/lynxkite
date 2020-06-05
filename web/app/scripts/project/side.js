// The Side object represents one project in the project view.
'use strict';

angular.module('biggraph')
  .factory('side', function (util, $rootScope, getCenter) {
    function defaultSideState() {
      // Get a deep copy of the default UI state.
      // util.globals will always be resolved by the time execution
      // gets here. That's because util.globals is fetched at page
      // load time, but this code only runs when the user opens a popup.
      return angular.copy(util.globals.defaultUIStatus);
    }

    function Side(sides, direction) {
      // The list of all sides.
      this.sides = sides;
      // Left or right?
      this.direction = direction;
      // The state of controls. E.g. bucket count.
      this.state = defaultSideState();
      // Everything needed for a view (state included).
      // Use this for rendering graph view instead of using state directly.
      this.viewData = {};
      // The /ajax/getProjectOutput Ajax response.
      this.project = undefined;
    }

    Side.prototype.sections = ['scalar', 'vertex-attribute', 'edge-attribute', 'segmentation'];
    Side.prototype.sectionHumanName = {
      'scalar': 'Graph attributes',
      'vertex-attribute': 'Vertex attributes',
      'edge-attribute': 'Edge attributes',
      'segmentation': 'Segmentations',
    };
    Side.prototype.sectionHelp = {
      'scalar': 'graph-attributes',
      'vertex-attribute': 'attributes',
      'edge-attribute': 'attributes',
      'segmentation': 'segmentations',
    };
    Side.prototype.sectionElements = function(section) {
      if (section === 'scalar') {
        return this.project.scalars.filter(function(s) {
          return s.title[0] !== '!';
        });
      } else if (section === 'vertex-attribute') {
        return this.project.vertexAttributes;
      } else if (section === 'edge-attribute') {
        return this.project.edgeAttributes;
      } else if (section === 'segmentation') {
        return this.project.segmentations;
      }
      /* eslint-disable no-console */
      console.error('Unexpected section:', section);
    };
    Side.prototype.showSection = function(section, show) {
      const key = 'section-visibility: ' + section;
      if (show === undefined) {
        const saved = localStorage.getItem(key);
        return saved === null ? true : saved === 'true';
      } else {
        localStorage.setItem(key, show);
      }
    };

    Side.prototype.activeSides = function() {
      const active = [];
      for (let i = 0 ; i < this.sides.length; ++i) {
        if (this.sides[i].state.projectPath !== undefined) {
          active.push(this.sides[i]);
        }
      }
      return active;
    };

    // Creates a JSON formatted version of the current UI state of this side. The output is
    // abstracted a bit away from the exact status so that it can be reapplied in slighly different
    // circumctances as well. E.g. project name is omitted, as the same visualization makes
    // sense in forks of the original project and center list is replaced with the request used
    // to get this center list (as long as the current center list came from a getCenters request).
    Side.prototype.getBackendJson = function() {
      const backendState = angular.copy(this.state);
      delete backendState.projectName;
      if (this.state.centers === this.state.lastCentersResponse) {
        delete backendState.centers;
      } else {
        delete backendState.lastCentersRequest;
      }
      delete backendState.lastCentersResponse;
      return JSON.stringify(backendState, null, 2);
    };

    Side.prototype.updateFromBackendJson = function(backendState) {
      if (!backendState) {
        backendState = defaultSideState();
      }
      backendState = angular.copy(backendState);
      backendState.projectName = this.state.projectName;
      this.state = backendState;
      if (this.state.graphMode && this.state.centers === undefined) {
        this.state.centers = [];
        this.sendCenterRequest(this.state.lastCentersRequest);
      }
    };

    Side.prototype.updateViewData = function() {
      const vd = this.viewData || {};
      const noCenters = (this.state.centers === undefined) || (this.state.centers.length === 0);
      if (!this.loaded() || !this.state.graphMode ||
          (this.state.graphMode === 'sampled' && noCenters)) {
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
      vd.preciseBucketSizes = this.state.preciseBucketSizes;
      vd.relativeEdgeDensity = this.state.relativeEdgeDensity;
      vd.vertexColorMap = this.state.vertexColorMap;
      vd.labelColorMap = this.state.labelColorMap;
      vd.edgeColorMap = this.state.edgeColorMap;
      vd.sliderColorMap = this.state.sliderColorMap;

      const at = this.state.attributeTitles;

      // "state" uses attribute names, while "viewData" uses attribute UUIDs.
      vd.xAttribute = this.resolveVertexAttribute(at.x);
      vd.yAttribute = this.resolveVertexAttribute(at.y);
      vd.xAxisOptions = this.axisOptions('vertex', at.x);
      vd.yAxisOptions = this.axisOptions('vertex', at.y);

      vd.vertexAttrs = {};
      vd.vertexAttrs.size = this.resolveVertexAttribute(at.size);
      vd.vertexAttrs.color = this.resolveVertexAttribute(at.color);
      vd.vertexAttrs.opacity = this.resolveVertexAttribute(at.opacity);
      vd.vertexAttrs.label = this.resolveVertexAttribute(at.label);
      vd.vertexAttrs.labelSize = this.resolveVertexAttribute(at['label size']);
      vd.vertexAttrs.labelColor = this.resolveVertexAttribute(at['label color']);
      vd.vertexAttrs.slider = this.resolveVertexAttribute(at.slider);
      vd.vertexAttrs.icon = this.resolveVertexAttribute(at.icon);
      vd.vertexAttrs.image = this.resolveVertexAttribute(at.image);
      vd.vertexAttrs.position = this.resolveVertexAttribute(at.position);
      vd.vertexAttrs.geo = this.resolveVertexAttribute(at['geo coordinates']);

      vd.edgeAttrs = {};
      const aggregated = function(attr, aggregator) {
        if (attr) {
          const aggrAttr = angular.copy(attr);
          aggrAttr.aggregator = aggregator;
          return aggrAttr;
        }
        return undefined;
      };
      vd.edgeAttrs.width = aggregated(
        this.resolveEdgeAttribute(this.state.attributeTitles.width),
        'sum');

      if (vd.graphMode !== 'bucketed') {
        vd.edgeAttrs.edgeLabel = aggregated(
          this.resolveEdgeAttribute(this.state.attributeTitles['edge label']),
          'set');
        const edgeColorAttr = this.resolveEdgeAttribute(this.state.attributeTitles['edge color']);
        if (edgeColorAttr !== undefined) {
          vd.edgeAttrs.edgeColor =
            (edgeColorAttr.typeName === 'number') ?
              aggregated(edgeColorAttr, 'sum') : aggregated(edgeColorAttr, 'set');
        }
      }

      vd.edgeWidth = this.resolveEdgeAttribute(this.state.attributeTitles.width);

      vd.filters = {
        edge: this.nonEmptyEdgeFilters(),
        vertex: this.nonEmptyVertexFilters(),
      };

      vd.centers = this.state.centers || [];
      const that = this;
      vd.setCenter = function(id) { that.state.centers = [id]; };
      vd.setCenters = function(ids) { that.state.centers = ids; };
      vd.sampleRadius = this.state.sampleRadius;
      vd.animate = this.state.animate;
      vd.edgeStyle = this.state.edgeStyle;

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
        vd.segmentationFilters()['#members'] = vd.filterValue(parentId);
      };
      vd.isSegmentationFilteredToParent = function(parentId) {
        return vd.segmentationFilters()['#members'] === vd.filterValue(parentId);
      };
      vd.deleteSegmentationsParentFilter = function() {
        delete vd.segmentationFilters()['#members'];
      };

      vd.setVertexFilter = function(title, value) {
        that.state.filters.vertex[title] = value;
      };

      this.viewData = vd;
    };

    Side.prototype.axisOptions = function(type, attr) {
      const defaultAxisOptions = {
        logarithmic: false,
      };
      return this.state.axisOptions[type][attr] || defaultAxisOptions;
    };

    Side.prototype.onGraphModeChange = function() {
      if (this.state.graphMode === 'bucketed') {
        if (this.state.display !== 'svg') {
          this.state.display = 'svg';
        }
      }
    };

    Side.prototype.resolveCenterRequestParams = function(params) {
      const resolvedParams = angular.copy(params);
      resolvedParams.filters = this.resolveVertexFilters(params.filters);
      resolvedParams.vertexSetId = this.project.vertexSet;
      return resolvedParams;
    };
    Side.prototype.sendCenterRequest = function(params) {
      const that = this;
      const resolvedParams = this.resolveCenterRequestParams(params);
      this.centerRequest = getCenter(resolvedParams);
      this.centerRequest.then(
        function(centers) {
          that.state.centers = centers;
          that.state.lastCentersRequest = params;
          that.state.lastCentersResponse = centers;
        });
    };

    Side.prototype.shortName = function() {
      if (this.state.projectPath === undefined) {
        return undefined;
      }
      const path = util.projectPath(this.state.projectPath);
      return path[path.length - 1];
    };
    Side.prototype.parentProjects = function() {
      if (this.state.projectPath === undefined) {
        return undefined;
      }
      const path = util.projectPath(this.state.projectPath);
      return path.slice(0, -1); // Discard own name.
    };
    Side.prototype.isSegmentation = function() {
      return this.parentProjects().length !== 0;
    };
    Side.prototype.parentProject = function() {
      return this.parentProjects().join('.');
    };

    Side.prototype.reload = function() {
      // We don't directly download data into this.project, so that
      // the UI can still show the previous state of the project
      // while loading, and state like (center picker stuff) does
      // not get lost. On the other hand, we want an indicator for
      // loading. The solution is to first load into
      // this.pendingProject and only copy into this.project on completion.
      this.pendingProject = undefined;
      if (this.stateId !== undefined && this.state.projectPath !== undefined) {
        const that = this;
        that.pendingProject = this.load();
        return that.pendingProject.finally(function () {
          if (!angular.equals(that.project, that.pendingProject)) {
            // This check is to avoid DOM-rebuild of entity drop popups.
            that.project = that.pendingProject;
            that.onProjectLoaded();
          }
        });
      } else {
        this.state = defaultSideState();
        this.project = undefined;
        return undefined;
      }
    };

    Side.prototype.load = function() {
      return util.get(
        '/ajax/getProjectOutput', {
          path: this.state.projectPath,
          id: this.stateId,
        });
    };

    Side.prototype.loaded = function() {
      return this.project && this.project.$resolved && !this.project.$error;
    };

    Side.prototype.reportLoadingError = function() {
      return util.reportRequestError(
        this.project, 'Could not load project: ' + this.state.projectPath);
    };

    Side.prototype.toggleAttributeTitle = function(setting, value) {
      if (this.state.attributeTitles[setting] === value) {
        // Clicking the same attribute setting again turns it off.
        this.state.attributeTitles[setting] = undefined;
        // Apply dependencies.
        if (setting === 'label') {
          this.state.attributeTitles['label size'] = undefined;
          this.state.attributeTitles['label color'] = undefined;
        }
      } else {
        this.state.attributeTitles[setting] = value;
        // Apply mutual exclusions and do initialization.
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
        } else if (setting === 'position') {
          this.state.attributeTitles['geo coordinates'] = undefined;
        } else if (setting === 'geo coordinates') {
          this.state.attributeTitles.position = undefined;
        }
      }
    };

    Side.prototype.filterApplied = function(settings, value) {
      const that = this;
      const applied = [];
      for (let i = 0; i < settings.length; ++i) {
        if (that.state.attributeTitles[settings[i]] === value) {
          applied.push(settings[i]);
        }
      }
      return applied;
    };

    Side.prototype.closeInternal = function() {
      this.state.projectPath = undefined;
      this.reload();
    };

    Side.prototype.close = function() {
      if (this.direction === 'right' &&
          this.parentProjects().length >= 2 &&
          this.parentProject() === this.sides[0].state.projectPath) {
        // If this project was:
        // 1. open on the right hand side
        // 2. was a segmentation of a segmentation
        // 3. it's parent was open on the left hand side
        // Then move its parent to the right hand side, and open its
        // grandparent on the left hand side.
        this.state.projectPath = this.sides[0].parentProject();
        this.reload();
        this.swapWithSide(this.sides[0]);
      } else if (this.direction === 'left' && this.sides[1].state.projectName) {
        // If this project was on the left and there is something on the right,
        // then let the project on the right shift to the left.
        this.closeInternal();
        this.swapWithSide(this.sides[1]);
      } else {
        this.closeInternal();
      }
    };

    Side.prototype.startSavingAs = function() {
      this.showSaveAs = true;
      this.saveAsName = this.state.projectName;
    };

    Side.prototype.maybeSaveAs = function() {
      // We only need to do an actual action if the user has changed the name.
      if (this.saveAsName !== this.state.projectName) {
        this.saveAs(this.saveAsName);
      }
      this.showSaveAs = false;
    };

    Side.prototype.saveAs = function(newName) {
      const that = this;
      util.post('/ajax/forkEntry',
        {
          from: this.state.projectName,
          to: newName,
        }).then(function() {
        that.state.projectName = newName;
      });
    };

    Side.prototype.saveNotes = function() {
      const that = this;
      this.savingNotes = true;
      this.applyOp('Change-project-notes', { notes: this.project.notes })
        .then(function() {
          that.unsavedNotes = false;
          that.savingNotes = false;
        });
    };

    Side.prototype.rename = function(kind, oldName, newName) {
      if (oldName === newName) { return; }
      this.applyOp('Rename-' + kind, { from: oldName, to: newName });
    };

    Side.prototype.duplicate = function(kind, name) {
      this.applyOp('Copy-' + kind, { from: name, to: 'copy_of_' + name });
    };

    Side.prototype.discard = function(kind, name) {
      // if the other side is the segmentation to be discarded, close it
      if (kind === 'segmentation') {
        for (let i = 0; i < this.sides.length; ++i) {
          const side = this.sides[i];
          if (side === this) { continue; }
          for (let j = 0; j < this.project.segmentations.length; ++j) {
            const seg = this.project.segmentations[j];
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

    Side.prototype.isInternalVertexFilter = function(name) {
      return this.resolveVertexAttribute(name).isInternal;
    };
    // Returns unresolved filters (i.e. keyed by the attribute name).
    Side.prototype.nonEmptyVertexFilterNames = function() {
      return this.nonEmptyFilterNames(this.state.filters.vertex);
    };
    Side.prototype.nonEmptyEdgeFilterNames = function() {
      return this.nonEmptyFilterNames(this.state.filters.edge);
    };
    Side.prototype.nonEmptyFilterNames = function(filters) {
      const res = [];
      for (const attr in filters) {
        if (filters[attr] !== '') {
          res.push({
            attributeName: attr,
            valueSpec: filters[attr] });
        }
      }
      return res;
    };

    Side.prototype.resolveVertexFilters = function(filters) {
      return filters
        .filter(f => this.resolveVertexAttribute(f.attributeName))
        .map(f => ({
          attributeId: this.resolveVertexAttribute(f.attributeName).id,
          valueSpec: f.valueSpec,
        }));
    };
    Side.prototype.nonEmptyVertexFilters = function() {
      return this.resolveVertexFilters(this.nonEmptyVertexFilterNames());
    };
    Side.prototype.resolveEdgeFilters = function(filters) {
      return filters
        .filter(f => this.resolveEdgeAttribute(f.attributeName))
        .map(f => ({
          attributeId: this.resolveEdgeAttribute(f.attributeName).id,
          valueSpec: f.valueSpec,
        }));
    };
    Side.prototype.nonEmptyEdgeFilters = function() {
      return this.resolveEdgeFilters(this.nonEmptyEdgeFilterNames());
    };

    Side.prototype.hasFilters = function() {
      return (this.nonEmptyEdgeFilterNames().length !== 0 ||
              this.nonEmptyVertexFilterNames().length !== 0);
    };
    Side.prototype.applyFiltersEnabled = function() {
      const that = this;
      return this.nonEmptyVertexFilterNames().every(
        function(filter) { return !that.isInternalVertexFilter(filter.attributeName); });
    };

    Side.prototype.applyFilters = function() {
      const that = this;
      util.post('/ajax/filterProject',
        {
          project: this.state.projectName,
          edgeFilters: this.nonEmptyEdgeFilterNames(),
          vertexFilters: this.nonEmptyVertexFilterNames(),
        }).then(function() {
        that.clearFilters();
        that.reload();
      });
    };
    Side.prototype.clearFilters = function() {
      this.state.filters = { edge: {}, vertex: {} };
    };
    Side.prototype.filterSummary = function() {
      const that = this;
      const NBSP = '\u00a0';
      const res = [];
      function addNonEmpty(value, key) {
        if (value) {
          res.push(' ' + key + NBSP + value);
        }
      }
      function addProblematic(value, key) {
        if (that.isInternalVertexFilter(key)) {
          const note = ' Cannot' + NBSP + 'apply' + NBSP + key + NBSP + 'here';
          res.push(note);
        }
      }
      angular.forEach(this.state.filters.vertex, addProblematic);
      angular.forEach(this.state.filters.vertex, addNonEmpty);
      angular.forEach(this.state.filters.edge, addNonEmpty);
      return res.join(', ');
    };
    Side.prototype.filterableVertexAttributes = function() {
      return this.project.vertexAttributes.concat(
        this.project.segmentations.map(function(segmentation) {
          return segmentation.equivalentAttribute;
        }));
    };
    Side.prototype.resolveVertexAttribute = function(title) {
      const filterableAttributes = this.filterableVertexAttributes();
      for (let attrIdx = 0; attrIdx < filterableAttributes.length; attrIdx++) {
        const attr = filterableAttributes[attrIdx];
        if (attr.title === title) {
          return attr;
        }
      }
      return undefined;
    };

    Side.prototype.resolveEdgeAttribute = function(title) {
      for (let attrIdx = 0; attrIdx < this.project.edgeAttributes.length; attrIdx++) {
        const attr = this.project.edgeAttributes[attrIdx];
        if (attr.title === title) {
          return attr;
        }
      }
      return undefined;
    };

    Side.prototype.swapWithSide = function(otherSide) {
      let tmp;

      tmp = this.project; this.project = otherSide.project; otherSide.project = tmp;
      tmp = this.state; this.state = otherSide.state; otherSide.state = tmp;
      tmp = this.viewData; this.viewData = otherSide.viewData; otherSide.viewData = tmp;
      tmp = this.scalars; this.scalars = otherSide.scalars; otherSide.scalars = tmp;
    };

    Side.prototype.openSegmentation = function(seg) {
      // Move this side to the left if it's not on the left.
      if (this.direction !== 'left') {
        // Swap sides 0 and this.
        this.sides[0].closeInternal();
        this.sides[0].swapWithSide(this);
      }

      // Segmentations always open on the right.
      this.sides[1].state.projectPath = seg.fullName;
      this.sides[1].reload();
      this.sides[1].state.graphMode = undefined;
    };

    // Abandon outstanding scalar requests.
    Side.prototype.abandonScalars = function() {
      if (this.scalars !== undefined) {
        const scalars = Object.keys(this.scalars);
        for (let i = 0; i < scalars.length; ++i) {
          this.scalars[scalars[i]].$abandon();
        }
      }
    };

    Side.prototype.loadScalars = function() {
      if (!this.loaded()) { return; }
      this.abandonScalars();
      this.scalars = {};
      const scalars = this.project.scalars;
      for (let i = 0; i < scalars.length; ++i) {
        const scalar = scalars[i];
        this.scalars[scalar.title] = util.lazyFetchScalarValue(
          scalar,
          true);
      }
    };

    Side.prototype.isSegmentationOf = function(parent) {
      return parent.getBelongsTo(this) !== undefined;
    };
    Side.prototype.getBelongsTo = function(segmentation) {
      const entry = this.getSegmentationEntry(segmentation);
      if (entry) {
        return entry.belongsTo;
      }
      return undefined;
    };
    Side.prototype.getSegmentationEntry = function(segmentation) {
      if (!this.loaded()) { return undefined; }
      if (!segmentation.project || !segmentation.project.$resolved) { return undefined; }
      for (let i = 0; i < this.project.segmentations.length; ++i) {
        const seg = this.project.segmentations[i];
        if (segmentation.project.name === seg.fullName) {
          return seg;
        }
      }
      return undefined;
    };
    Side.prototype.getParentSide = function() {
      for (let i = 0; i < this.sides.length; ++i) {
        const side = this.sides[i];
        if (side === this) { continue; }
        if (side.getSegmentationEntry(this)) {
          return side;
        }
      }
      return undefined;
    };
    Side.prototype.getSegmentationSide = function() {
      for (let i = 0; i < this.sides.length; ++i) {
        const side = this.sides[i];
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
      this.loadScalars();
      this.updateViewData();
    };

    // Removes entries from state which depend on nonexistent attributes
    Side.prototype.cleanState = function() {
      if (!this.loaded()) { return; }
      const vTitles = this.filterableVertexAttributes().map(function(a) { return a.title; });
      const eTitles = this.project.edgeAttributes.map(function(a) { return a.title; });
      let attr;
      for (attr in this.state.filters.edge) {
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
      const allTitles = eTitles.concat(vTitles);
      for (attr in this.state.attributeTitles) {
        if (allTitles.indexOf(this.state.attributeTitles[attr]) === -1) {
          delete this.state.attributeTitles[attr];
        }
      }
    };

    Side.prototype.setIcon = function(kind, name, icon) {
      return this.applyOp('Set-' + kind + '-icon', { name: name, icon: icon });
    };

    return { Side: Side };
  });
