// The "/project" page displays one or two projects.
'use strict';

angular.module('biggraph')
  .controller('ProjectViewCtrl', function ($scope, $routeParams, $location, util, hotkeys, side) {
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
        return left.getBelongsTo(right);
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
      var history = $scope.left.showHistory || $scope.right.showHistory;
      var graph = $scope.left.viewData || $scope.right.viewData;
      return !history && graph;
    };

    $scope.$watch('left.project.$resolved', function(loaded) {
      if (loaded) {
        $scope.left.onProjectLoaded();
        $scope.leftToRightBundle = getLeftToRightBundle();
        $scope.rightToLeftBundle = getRightToLeftBundle();
      }
    });
    $scope.$watch('right.project.$resolved', function(loaded) {
      if (loaded) {
        $scope.right.onProjectLoaded();
        $scope.leftToRightBundle = getLeftToRightBundle();
        $scope.rightToLeftBundle = getRightToLeftBundle();
      }
    });

    $scope.sides = [];
    $scope.sides.push(new side.Side($scope.sides, 'left'));
    $scope.sides.push(new side.Side($scope.sides, 'right'));
    $scope.left = $scope.sides[0];
    $scope.right = $scope.sides[1];

    util.deepWatch($scope, 'left.state', function() { $scope.left.updateViewData(); });
    util.deepWatch($scope, 'right.state', function() { $scope.right.updateViewData(); });
    $scope.$watch('left.state.graphMode', function() { $scope.left.onGraphModeChange(); });
    $scope.$watch('right.state.graphMode', function() { $scope.right.onGraphModeChange(); });
    $scope.$on('$destroy', function() {
      for (var i = 0; i < $scope.sides.length; ++i) {
        $scope.sides[i].abandonScalars();
      }
    });

    // This watcher copies the state from the URL into $scope.
    // It is an important part of initialization. Less importantly it makes
    // it possible to edit the state manually in the URL, or use the "back"
    // button to undo state changes.
    util.deepWatch(
      $scope,
      function() { return $location.search(); },
      function(after, before) {
        /* eslint-disable no-console */
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

    $scope.$watch('left.state.projectName',
        function() { $scope.left.reload(); checkAllClosed(); });
    $scope.$watch('right.state.projectName',
        function() { $scope.right.reload(); checkAllClosed(); });
    function checkAllClosed() {
      for (var i = 0; i < $scope.sides.length; ++i) {
        if ($scope.sides[i].state.projectName !== undefined) {
          return;
        }
      }
      $location.url('/');
    }

    function parseState(search) {
      var state = {};
      if (search.q === undefined) {
        state.left = side.defaultSideState();
        state.right = side.defaultSideState();
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
    /* eslint-disable no-console */
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
      localStorage.setItem(channel, Date.now() + Math.random().toString(36));
    }

    function sendReloadNotification() {
      wiggleChannel('reload:' + $scope.linkChannel);
    }
    $scope.$on('project reloaded', sendReloadNotification);

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
          if (key.indexOf('channel-') === 0) {
            channel = key;
          } else if (parts[0] === 'reload') {
            channel = parts[1];
          } else if (parts[0] === 'pong') {
            channel = parts[1];
          }
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
