// Provides utility functions, most importantly the Ajax IO functions.
'use strict';

angular.module('biggraph').factory('util', function utilFactory(
      $location, $window, $http, $rootScope, $modal, $q) {
  var siSymbols = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];

  // A request queue with a limit on the number of parallel requests.
  function RequestQueue(maxParallel) {
    this.maxParallel = maxParallel;
    this.queue = [];
  }
  RequestQueue.prototype = {
    // Adds a request to the queue, sending it immediately if possible.
    request: function(config) {
      var req;
      if (this.queue.length < this.maxParallel) {
        req = sendRequest(config);
      } else {
        var queuing = $q.defer();
        req = queuing.promise.then(function() {
          var next = sendRequest(config);
          req.$abandon = next.$abandon;  // Abandoning should now try to cancel the request.
          return next;
        });
        req.$config = config;  // For debugging.
        req.$execute = function() { queuing.resolve(); };
        req.$abandon = function() {
          queuing.reject({ config: config, statusText: 'Abandoned.' });
        };
      }
      var that = this;
      req.finally(function() { that.finished(req); });
      this.queue.push(req);
      return req;
    },

    // A request has finished. Send more requests if available.
    finished: function(request) {
      var q = this.queue;
      for (var i = 0; i < q.length; ++i) {
        if (q[i] === request) {
          q.splice(i, 1);
          if (i < this.maxParallel && q.length >= this.maxParallel) {
            var next = q[this.maxParallel - 1];
            next.$execute();
          }
          return;
        }
      }
      console.error('Could not find finished request in the queue:', request, q);
    },
  };


  // Sends an HTTP GET, possibly queuing the request.
  function getRequest(config) {
    var SLOW_REQUESTS = [
      '/ajax/complexView',
      '/ajax/histo',
      '/ajax/scalarValue',
      '/ajax/center',
      '/ajax/getDataFilesStatus',
      ];
    // Some requests may trigger substantial calculation on the backend. If we
    // make many slow requests in parallel we can easily exhaust the browser's
    // parallel connection limit. (The limit depends on the browser. It is 6 for
    // Chrome 45.) This can block fast requests from getting sent.
    //
    // For this reason we queue slow requests in the browser. With the number
    // of parallel slow requests limited, we expect to have enough connections
    // left for the fast requests.
    for (var i = 0; i < SLOW_REQUESTS.length; ++i) {
      var pattern = SLOW_REQUESTS[i];
      if (config.url.indexOf(pattern) !== -1) {
        return util.slowQueue.request(config);
      }
    }
    // Fast request.
    return sendRequest(config);
  }

  // Returns a self-populating object, like Angular's ngResource.
  function getResource(url, params, config) {
    // Create full $http request config.
    if (params === undefined) { params = { fake: 1 }; }
    var fullConfig = angular.extend({ method: 'GET', url: url, params: params }, config);
    // Send request.
    var resource = getRequest(fullConfig);
    // Populate the promise object with the result data, update $resolved.
    resource.then(
      function onSuccess(response) {
        angular.extend(resource, response.data);
        resource.$resolved = true;
      },
      function onError(failure) {
        resource.$resolved = true;
        resource.$status = failure.status;
        if (failure.status === 401) {  // Unauthorized.
          resource.$error = 'Redirecting to login page.';
          if ($location.protocol() === 'https') {
            $location.url('/login');
          } else {
            $window.location.href = 'https://' + $location.host() + '/login';
          }
        } else {
          resource.$error = util.responseToErrorMessage(failure);
        }
      });
    resource.$resolved = false;
    return resource;
  }

  // Sends an HTTP request immediately.
  function sendRequest(config) {
    var canceler = $q.defer();
    var fullConfig = angular.extend({ timeout: canceler.promise }, config);
    var req = $http(fullConfig);
    req.$config = fullConfig;  // For debugging.
    req.$abandon = function() { canceler.resolve(); };
    return req;
  }

  var util = {
    // This function is for code clarity, so we don't have a mysterious "true" argument.
    deepWatch: function(scope, expr, fun) {
      return scope.$watch(expr, fun, true);
    },

    // Json GET with caching and parameter wrapping.
    get: function(url, params) { return getResource(url, params, { cache: true }); },

    // Json GET with parameter wrapping and no caching.
    nocache: function(url, params) { return getResource(url, params, { cache: false }); },

    // Json POST with simple error handling.
    post: function(url, params, onSuccess) {
      var req = $http.post(url, params).then(onSuccess, function(failure) {
        util.ajaxError(failure);
      });
      // A promise of the success state, for flexibility.
      req.$status = req.then(function() { return true; }, function() { return false; });
      return req;
    },

    // Easier to read numbers. 1234 -> 1k
    human: function(x) {
      if (x === undefined) { return '?'; }
      if (typeof x !== 'number') { return x; }
      if (isNaN(x)) { return x; }
      for (var i = 0; true; ++i) {
        if (x < 1000 || i === siSymbols.length - 1) {
          return x + siSymbols[i];
        }
        x = Math.round(x / 1000);
      }
    },

    // Replaces underscores with spaces.
    spaced: function(s) {
      return s.replace(/_/g, ' ');
    },

    ajaxError: function(resp) {
      util.error(
        util.responseToErrorMessage(resp),
        { request: resp.config.url, data: resp.config.data });
    },

    error: function(message, details) {
      $rootScope.$broadcast('topAlert', { message: message, details: details });
    },

    clearAlerts: function() {
      $rootScope.$broadcast('clear topAlerts');
    },

    responseToErrorMessage: function(resp) {
      if (resp.data) {
        if (resp.data.error) {
          return resp.data.error;
        }
        return resp.data;
      } else if (resp.status === 0) {
        return 'The server (' + window.location.hostname + ') cannot be reached.';
      }
      return resp.config.url + ' ' + (resp.statusText || 'failed');
    },

    scopeTitle: function(scope, titleExpr) {
      scope.$watch(titleExpr, function(title) {
        angular.element('title').html(title);
      });
      scope.$on('$destroy', function() {
        angular.element('title').html('LynxKite');
      });
    },

    reportRequestError: function(request, details) {
      if (request) {
        util.reportError({
          message: request.$error,
          details: {
            url: request.config.url,
            params: request.config.params,
            details: details,
          },
        });
      } else {
        util.reportError({
          message: 'undefined request',
          details: details,
        });
      }
    },

    reportError: function(alert) {
      $modal.open({
        templateUrl: 'report-error.html',
        controller: 'ReportErrorCtrl',
        resolve: { alert: function() { return alert; } },
      });
    },

    projectPath: function(projectName) {
      if (!projectName) { return []; }
      return projectName.split('|').map(function(name) {
        return util.spaced(name);
      });
    },

    captureClick: function(event) {
      if (event) {
        event.originalEvent.alreadyHandled = true;
      }
    },

    slowQueue: new RequestQueue(2),
  };
  util.globals = util.get('/ajax/getGlobalSettings');
  return util;
});
