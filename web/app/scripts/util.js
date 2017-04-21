// Provides utility functions, most importantly the Ajax IO functions.
'use strict';

angular.module('biggraph').factory('util', function utilFactory(
      $location, $window, $http, $rootScope, $modal, $q) {
  var siSymbols = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
  // DataManager computation status codes. Keep these in sync
  // with EntityProgressManager.computeProgress
  var COMPUTE_PROGRESS_ERROR = -1.0;
  var COMPUTE_PROGRESS_NOT_STARTED = 0.0;
  var COMPUTE_PROGRESS_IN_PROGRESS = 0.5;
  var COMPUTE_PROGRESS_COMPLETED = 1.0;

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
          req.$abandon = function() {
            next.$abandon();  // Abandoning should now cancel the request.
          };
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


  // Sends an HTTP request immediately.
  function sendRequest(config) {
    var canceler = $q.defer();
    var fullConfig = angular.extend({ timeout: canceler.promise }, config);
    var req = $http(fullConfig);
    req.$config = fullConfig;  // For debugging.
    req.$abandon = function() { canceler.resolve(); };
    return req;
  }

  // Sends an HTTP GET, possibly queuing the request.
  function getRequest(config) {
    var SLOW_REQUESTS = [
      '/ajax/complexView',
      '/ajax/histo',
      '/ajax/scalarValue',
      '/ajax/center',
      '/ajax/getDataFilesStatus',
      '/ajax/model',
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

  // A GET request wrapped as a Resource.
  function getResource(url, params, config) {
    // Create full $http request config.
    if (params === undefined) { params = { fake: 1 }; }
    var fullConfig = angular.extend({ method: 'GET', url: url, params: { q: params } }, config);
    // Send request.
    var req = getRequest(fullConfig);
    // Return Resource.
    return toResource(req);
  }

  // Replaces a promise with another promise that behaves like Angular's ngResource.
  // It will populate itself with the response data and set $resolved, $error, and $statusCode.
  // It can be abandoned with $abandon(). $status is a Boolean promise of the success state.
  // $config describes the original request config.
  function toResource(promise) {
    var resource = promise.then(
      function onSuccess(response) {
        angular.extend(resource, response.data);
        resource.$resolved = true;
        return response.data;
      },
      function onError(failure) {
        resource.$resolved = true;
        resource.$statusCode = failure.status;
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
        return $q.reject(failure);
      });
    resource.$resolved = false;
    // Propagate $config and $abandon.
    resource.$config = promise.$config;
    resource.$abandon = function() { promise.$abandon(); };
    // A promise of the success state, for flexibility.
    resource.$status = resource.then(function() { return true; }, function() { return false; });
    return resource;
  }

  var scalarCache = {}; // Need to return the same object every time to avoid digest hell.

  var util = {
    // This function is for code clarity, so we don't have a mysterious "true" argument.
    deepWatch: function(scope, expr, fun) {
      return scope.$watch(expr, fun, true);
    },

    // Move move an element from one dictionary to another
    // After this completes, we want the src dictionary to not contain
    // the element, and the dst dictionary to contain the element.
    move: function(key, src, dst) {
        console.log('Moving: ' + key);
        if (key in src) {
            dst[key] = src[key];
            delete src[key];
        } else if(! (key in dst)) {
            console.log('key ' + key + ' not present in either dictionaries!');
        }
    },


    // Json GET with caching and parameter wrapping.
    get: function(url, params) { return getResource(url, params, { cache: true }); },

    // Json GET with parameter wrapping and no caching.
    nocache: function(url, params) { return getResource(url, params, { cache: false }); },

    // Json POST with simple error handling.
    post: function(url, params, options) {
      options = options || { reportErrors: true };
      var req = $http.post(url, params);
      if (options.reportErrors) {
        req = req.catch(function(failure) {
          util.ajaxError(failure);
          return $q.reject(failure);
        });
      }
      return toResource(req);
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

    asScalar: function(value) {
      if (scalarCache[value] === undefined) {
        scalarCache[value] = { value: {
          string: value !== undefined ? value.toString() : '',
          double: value,
        }};
      }
      return scalarCache[value];
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
            url: request.$config.url,
            params: request.$config.params,
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
        animation: false,  // Protractor does not like the animation.
      });
    },

    projectPath: function(projectName) {
      if (!projectName) { return []; }
      return projectName.split('|');
    },

    captureClick: function(event) {
      if (event) {
        event.originalEvent.alreadyHandled = true;
      }
    },

    // TODO: investigate and unify with the above function.
    stopEventPropagation: function(event) {
      if (event) {
        event.preventDefault();
        event.stopPropagation();
      }
    },

    // Gets the value of the scalar. If the value (or an error message is embdedded
    // in the scalar, then just takes it. Otherwise it fetches it from the server.
    // The return value is an object containing a 'value' property and a '$abandon'
    // function.
    // '$abandon' cancels any ongoing requests.
    // 'value' is a promise or an object that may be updated later by the user.
    //   It contains '$error', '$resolved', '$statusCode' and all the fields of
    //   DynamicValues from the Scala backend.
    // The content of 'value' may change after this function has returned in two cases:
    // 1. If a new value is fetched from the server initiated by this call.
    // 2. If a new value is fetched from the server initiated by calling
    //   value.retryFunction().
    lazyFetchScalarValue: function(
      scalar,  // An FEScalar returned from the backend. May or may not hold computed value.
      fetchNotReady  // Send backend request if scalar computation is in progress on not started.
      ) {

      var scalarValue = {
        value: undefined,
        $abandon: function() {
          if (scalarValue.value && scalarValue.value.$abandon) {
            scalarValue.value.$abandon();
          }
        }
      };  // result to return

      // This function can be exposed to the UI as 'click here to retry'.
      var retryFunction = function() {
        fetchScalarAndConstructValue();
      };

      // All the below sub-functions read scalar and write into scalarValue.

      // Fake scalar for non-existent scalars, e.g. projects with no vertices/edges.
      function constructValueForNoScalar() {
        scalarValue.value = {
          string: 'no',
        };
      }
      // Placeholder when the scalar has not been calculated yet.
      function constructValueForCalculationInProgress() {
        scalarValue.value = {
          $statusCode: 202, // Accepted.
          $error: 'Calculation in progress.',
          retryFunction: retryFunction,
        };
      }
      // Placeholder when the scalar has not been calculated yet.
      function constructValueForCalculationNotStarted() {
        scalarValue.value = {
          $statusCode: 404,
          $error: 'Not calculated yet',
          retryFunction: retryFunction,
        };
      }
      // Constructs scalar placeholder for an error message.
      function constructValueForError() {
        scalarValue.value = {
          $resolved: true,
          $statusCode: 500,
          $error: scalar.errorMessage,
          $config: {
            url: '/ajax/scalarValue',
            params: {
              q: {
                scalarId: scalar.id,
              },
            },
          },
          retryFunction: retryFunction,
        };
      }
      // Constructs a scalar placeholder when scalar already holds
      // the computed value.
      function constructValueForComputedScalar() {
        scalarValue.value = scalar.computedValue;
        scalarValue.value.$resolved = true;
      }
      // Fetches a new value for the scalar.
      function fetchScalarAndConstructValue() {
        var res = util.get('/ajax/scalarValue', {
          scalarId: scalar.id,
        });
        scalarValue.value = res;
        res.then(
          function() {  // Success.
          },
          function() {  // Failure.
            // Enable retry icon for the user.
            scalarValue.value.retryFunction = retryFunction;
          });
      }

      if (!scalar) {
        constructValueForNoScalar();
      } else if (scalar.computeProgress === COMPUTE_PROGRESS_COMPLETED) {
        // Server has sent us the computed value of this
        // scalar upfront with metadata.
        constructValueForComputedScalar();
      } else if (scalar.computeProgress === COMPUTE_PROGRESS_NOT_STARTED) {
        if (fetchNotReady) {
          fetchScalarAndConstructValue();
        } else {
          constructValueForCalculationNotStarted();
        }
      } else if (scalar.computeProgress === COMPUTE_PROGRESS_IN_PROGRESS) {
        if (fetchNotReady) {
          fetchScalarAndConstructValue();
        } else {
          constructValueForCalculationInProgress();
        }
      } else if (scalar.computeProgress === COMPUTE_PROGRESS_ERROR) {
        constructValueForError();
      } else {
        console.error('Unknown computation state for scalar in ', scalar);
      }

      return scalarValue;
    },

    slowQueue: new RequestQueue(2),

    showOverwriteDialog: function(confirmCallback) {
      window.sweetAlert({
        title: 'Entry already exists',
        text: 'Do you want to overwrite it?',
        type: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        cancelButtonText: 'No',
        confirmButtonText: 'Yes',
      },
      confirmCallback);
    }
  };
  util.globals = util.get('/ajax/getGlobalSettings');

  util.reloadUser = function() {
    util.user = util.nocache('/ajax/getUserData');
  };
  util.reloadUser();

  return util;
});
