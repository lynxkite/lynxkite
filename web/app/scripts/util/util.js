// Provides utility functions, most importantly the Ajax IO functions.
'use strict';
import '../app';
import reportErrorTemplate from '../report-error.html?url';
import firebase from 'firebase/app';
import 'firebase/analytics';

angular.module('biggraph').service('environment', function() {
  this.protractor = false; // If we want to handle tests specially somewhere.
  this.vegaConfig = { renderer: 'canvas' }; // Canvas looks better but SVG is more accessible for tests.
});
angular.module('biggraph')
  .factory('util', ['$location', '$window', '$http', '$rootScope', '$uibModal', '$q', '$route', '$timeout', function utilFactory($location, $window, $http, $rootScope, $uibModal, $q, $route, $timeout) {
    const siSymbols = ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    // DataManager computation status codes. Keep these in sync
    // with EntityProgressManager.computeProgress
    const COMPUTE_PROGRESS_ERROR = -1.0;
    const COMPUTE_PROGRESS_NOT_STARTED = 0.0;
    const COMPUTE_PROGRESS_COMPLETED = 1.0;

    // A request queue with a limit on the number of parallel requests.
    function RequestQueue(maxParallel) {
      this.maxParallel = maxParallel;
      this.queue = [];
    }
    RequestQueue.prototype = {
    // Adds a request to the queue, sending it immediately if possible.
      request: function(config) {
        let req;
        if (this.queue.length < this.maxParallel) {
          req = sendRequest(config);
        } else {
          const queuing = $q.defer();
          req = queuing.promise.then(function() {
            const next = sendRequest(config);
            req.$abandon = function() {
              next.$abandon(); // Abandoning should now cancel the request.
            };
            return next;
          });
          req.$config = config; // For debugging.
          req.$execute = function() { queuing.resolve(); };
          req.$abandon = function() {
            queuing.reject({ config: config, statusText: 'Abandoned.' });
          };
        }
        req.finally(() => this.finished(req));
        this.queue.push(req);
        return req;
      },

      // A request has finished. Send more requests if available.
      finished: function(request) {
        const q = this.queue;
        for (let i = 0; i < q.length; ++i) {
          if (q[i] === request) {
            q.splice(i, 1);
            if (i < this.maxParallel && q.length >= this.maxParallel) {
              const next = q[this.maxParallel - 1];
              next.$execute();
            }
            return;
          }
        }
        /* eslint-disable no-console */
        console.error('Could not find finished request in the queue:', request, q);
      },
    };


    // Sends an HTTP request immediately.
    function sendRequest(config) {
      if (config.url.startsWith('/')) {
        config.url = config.url.slice(1); // Use relative URLs.
      }
      const canceler = $q.defer();
      const fullConfig = angular.extend({ timeout: canceler.promise }, config);
      const req = $http(fullConfig).catch(error => {
        if (error.status === 504) { // Gateway timeout. Repeat the request.
          return sendRequest(config);
        } else {
          return $q.reject(error);
        }
      });
      req.$config = fullConfig; // For debugging.
      req.$abandon = function() { canceler.resolve(); };
      return req;
    }

    // Sends an HTTP GET, possibly queuing the request.
    function getRequest(config) {
      const SLOW_REQUESTS = [
        '/ajax/complexView',
        '/ajax/histo',
        '/ajax/scalarValue',
        '/ajax/center',
        '/ajax/getDataFilesStatus',
        '/ajax/model',
        '/ajax/getTableOutput',
      ];
      // Some requests may trigger substantial calculation on the backend. If we
      // make many slow requests in parallel we can easily exhaust the browser's
      // parallel connection limit. (The limit depends on the browser. It is 6 for
      // Chrome 45.) This can block fast requests from getting sent.
      //
      // For this reason we queue slow requests in the browser. With the number
      // of parallel slow requests limited, we expect to have enough connections
      // left for the fast requests.
      for (let i = 0; i < SLOW_REQUESTS.length; ++i) {
        const pattern = SLOW_REQUESTS[i];
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
      const fullConfig = angular.extend({ method: 'GET', url: url, params: { q: params } }, config);
      // Send request.
      const req = getRequest(fullConfig);
      // Return Resource.
      return toResource(req);
    }

    // Replaces a promise with another promise that behaves like Angular's ngResource.
    // It will populate itself with the response data and set $resolved, $error, and $statusCode.
    // It can be abandoned with $abandon(). $status is a Boolean promise of the success state.
    // $config describes the original request config.
    function toResource(promise) {
      const sendTime = new Date();
      // Collect statistics about backend requests.
      function collect(response, error) {
        const url = response.config && response.config.url;
        if (url && !url.includes('ajax/long-poll') && !url.includes('ajax/jsError')) {
          util.logUsage('request', {
            url,
            // The start of the error so we can see what kind of exception this is,
            // but won't accidentally capture data.
            error: error && error.toString().slice(0, 30),
            duration: new Date() - sendTime,
          });
        }
      }
      const resource = promise.then(
        function onSuccess(response) {
          collect(response);
          angular.extend(resource, response.data);
          resource.$resolved = true;
          return response.data;
        },
        function onError(failure) {
          resource.$resolved = true;
          resource.$statusCode = failure.status;
          resource.$error = util.responseToErrorMessage(failure);
          collect(failure, resource.$error);
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

    const scalarCache = {}; // Need to return the same object every time to avoid digest hell.

    const util = {
    // This function is for code clarity, so we don't have a mysterious "true" argument.
      deepWatch: function(scope, expr, fun) {
        return scope.$watch(expr, fun, true);
      },

      // Move an element from one dictionary to another
      // After this completes, we want the src dictionary to not contain
      // the element, and the dst dictionary to contain the element.
      move: function(key, src, dst) {
        if (key in src) {
          dst[key] = src[key];
          delete src[key];
        } else if (! (key in dst)) {
          /* eslint-disable no-console */
          console.error('Key "' + key + '" is not present in either dictionary!');
        }
      },

      // Json GET with caching and parameter wrapping.
      get: function(url, params) { return getResource(url, params, { cache: true }); },

      // Json GET with parameter wrapping and no caching.
      nocache: function(url, params) { return getResource(url, params, { cache: false }); },

      // Json POST with simple error handling.
      post: function(url, params, options) {
        options = options || { reportErrors: true };
        if (url.startsWith('/')) {
          url = url.slice(1); // Use relative URLs.
        }
        let req = $http.post(url, params);
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
        /* eslint-disable no-constant-condition */
        for (let i = 0; true; ++i) {
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
        } else if (resp.status === 401) {
          return 'You need to log in to access this feature.';
        } else if (resp.config) {
          return resp.config.url + ' ' + (resp.statusText || 'failed');
        } else {
          return resp.statusText || 'failed';
        }
      },

      scopeTitle: function(scope, title) {
        angular.element('title').html(title);
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
        $uibModal.open({
          templateUrl: reportErrorTemplate,
          controller: 'ReportErrorCtrl',
          resolve: { alert: function() { return alert; } },
          animation: false, // Protractor does not like the animation.
        });
      },

      projectPath: function(projectName) {
        if (!projectName) { return []; }
        return projectName.split('.');
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

      // Gets the value of the scalar. If the value (or an error message) is embedded
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
        scalar, // An FEScalar returned from the backend. May or may not hold computed value.
        fetchNotReady // Send backend request if scalar computation is in progress on not started.
      ) {

        const scalarValue = {
          value: undefined,
          $abandon: function() {
            if (scalarValue.value && scalarValue.value.$abandon) {
              scalarValue.value.$abandon();
            }
          }
        }; // result to return

        // This function can be exposed to the UI as 'click here to retry'.
        const retryFunction = function() {
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
          const res = util.get('/ajax/scalarValue', {
            scalarId: scalar.id,
          });
          scalarValue.value = res;
          res.then(
            function() { // Success.
            },
            function() { // Failure.
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
        } else if (COMPUTE_PROGRESS_NOT_STARTED < scalar.computeProgress
          && scalar.computeProgress < COMPUTE_PROGRESS_COMPLETED) {
          if (fetchNotReady) {
            fetchScalarAndConstructValue();
          } else {
            constructValueForCalculationInProgress();
          }
        } else if (scalar.computeProgress === COMPUTE_PROGRESS_ERROR) {
          constructValueForError();
        } else {
        /* eslint-disable no-console */
          console.error('Unknown computation state for scalar in ', scalar);
        }

        if (scalar && (scalar.title === '!vertex_count' || scalar.title === '!edge_count')) {
          // Async function to handle both promises and ready values.
          (async function() {
            const v = (await scalarValue.value).string;
            if (v) {
              // Collect approximate graph sizes.
              util.logUsage('scalar', {
                name: scalar.title,
                rounded: parseInt(v.slice(0, 2).padEnd(v.length, '0')),
              });
            }
          })();
        }
        return scalarValue;
      },

      slowQueue: new RequestQueue(2),
    };

    util.warning = function(title, text) {
      return window.sweetAlert({
        title: title,
        text: text,
        type: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        cancelButtonText: 'No',
        confirmButtonText: 'Yes',
      });
    };

    util.globals = util.get('/ajax/getGlobalSettings');
    util.frontendConfig = util.globals.then(g => JSON.parse(g.frontendConfig || '{}'));

    util.reloadUser = function() {
      util.user = {
        $resolved: true,
        then: f => f({}),
      };
    };
    util.reloadUser();

    util.qualitativeColorMaps = [
      'LynxKite Colors', 'Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2', 'Rainbow', 'Set1',
      'Set2', 'Set3'];

    util.sliderColorMaps = {
      'Blue to orange': ['#39bcf3', '#f80'],
      'Orange to blue': ['#f80', '#39bcf3'],
      'Visible to invisible': ['#39bcf3', 'transparent'],
      'Invisible to visible': ['transparent', '#39bcf3'],
    };

    util.baseName = function(p) {
      const lastSlash = p.lastIndexOf('/');
      return p.slice(lastSlash + 1);
    };
    util.dirName = function(p) {
      const lastSlash = p.lastIndexOf('/');
      return p.slice(0, lastSlash + 1);
    };

    // Call before $location change to avoid a controller reload.
    // Source: https://github.com/angular/angular.js/issues/1699
    util.skipReload = function() {
      const lastRoute = $route.current;
      const un = $rootScope.$on('$locationChangeSuccess', () => {
        $route.current = lastRoute;
        un();
      });
    };

    let queue;
    util.enqueue = function(fn) {
      function runQueue() {
        for (const fn of queue) {
          fn();
        }
        queue = undefined;
      }
      if (!queue) {
        queue = [fn];
        $timeout(runQueue);
      } else {
        queue.push(fn);
      }
    };

    function randomId() {
      const arr = new Uint8Array(20);
      window.crypto.getRandomValues(arr);
      return Array.from(arr, c => c.toString(16)).join('');
    }
    let dataCollectionId = localStorage.getItem('data collection id');
    if (!dataCollectionId) {
      dataCollectionId = randomId();
      localStorage.setItem('data collection id', dataCollectionId);
    }
    util.collectUsage = localStorage.getItem('allow data collection') === 'true';
    util.allowDataCollection = function(allow) {
      util.collectUsage = allow;
      localStorage.setItem('allow data collection', allow ? 'true' : 'false');
    };
    util.globals.then(() => {
      if (util.globals.dataCollectionMode === 'always') {
        util.allowDataCollection(true);
      } else if (util.globals.dataCollectionMode === 'never') {
        util.allowDataCollection(false);
      }
    });

    const usageLog = [];
    let submitUsageLogTimeout;
    // Saves an event to send as usage statistics, if the user has opted in.
    util.logUsage = function(kind, details) {
      if (!util.collectUsage) {
        return;
      }
      // Usage events are collected in "usageLog" and sent to the server at most once a minute.
      usageLog.push({
        id: dataCollectionId,
        tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
        version: util.globals && util.globals.version,
        browser: navigator.userAgent,
        time: new Date().toISOString(),
        kind, ...details,
      });
      if (!submitUsageLogTimeout) {
        submitUsageLogTimeout = setTimeout(submitUsageLog, 60000);
      }
    };
    let analytics;
    function submitUsageLog() {
      submitUsageLogTimeout = undefined;
      if (!analytics) {
        // Shared. All LynxKite instances collect stats here.
        const firebaseConfig = {
          apiKey: 'AIzaSyDWY_jW9nPFJP1Iwexa9jf3foRgK51OTXM',
          authDomain: 'external-lynxkite.firebaseapp.com',
          projectId: 'external-lynxkite',
          storageBucket: 'external-lynxkite.appspot.com',
          messagingSenderId: '422846954881',
          appId: '1:422846954881:web:53c06b4ed052166db7bc80',
          measurementId: 'G-5NDZSKY669',
        };
        firebase.initializeApp(firebaseConfig);
        analytics = firebase.analytics();
      }
      /* eslint-disable no-console */
      console.log('Submitting anonymous usage data...');
      for (const e of usageLog.slice(0, 100)) { // Log no more than 100 events.
        // Firestore does not accept undefined values.
        for (const k in e) {
          if (e[k] === undefined) {
            delete e[k];
          }
        }
        analytics.logEvent('usage_' + e.kind, e);
      }
      usageLog.length = 0;
    }

    return util;
  }]);
