'use strict';

angular.module('myApp.primaryView', ['ngRoute'])

.config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/primaryView', {
    templateUrl: 'primaryView/primaryView.html',
    controller: 'PrimaryViewCtrl'
  });
}])

.controller('PrimaryViewCtrl', ['$scope', '$q', '_', '$http', function($scope, $q, _, $http) {
    $scope.resource = {
      id: 'nodeSource[NODES:ny-cassandra-1].nodeSnmp[]',
      attribute: 'CpuRawUser'
    };

    $scope.status = "Ready";
    $scope.metrics = [];

    var onmsHost = "45.55.88.134",
      onmsPort = 8980,
      onmsUsername = 'admin',
      onmsPassword = 'fuckfuck',
      end = Date.now(),
      start = end - (24 * 60 * 60 * 1000); // 24 hours ago

    function getBaseHref() {
      return "http://" + onmsHost + ":" + onmsPort + "/opennms";
    }

    $scope.graph = function(resources) {
      var numResources = resources.length;
      var metrics = [];
      var series = [];
      for (var i = 0; i < numResources; i++) {
        var resource = resources[i];
        metrics.push({
          name: resource.attribute,
          resourceId: resource.id,
          attribute: resource.attribute,
          aggregation: "AVERAGE"
        });

        series.push({
          name: resource.attribute,
          metric: resource.attribute,
          type: "line"
        });
      }

      var ds = new Backshift.DataSource.OpenNMS({
        url: getBaseHref() + "/rest/measurements",
        username: onmsUsername,
        password: onmsPassword,
        metrics: metrics
      });

      // Build and render the graph
      var graph = new Backshift.Graph.C3({
        element: $('#primaryGraph')[0],
        start: start,
        end: end,
        height: 250,
        dataSource: ds,
        series: series
      });

      graph.render();
    };

    function onmsRequest(method, url, data) {
      var _this = this;
      var params = {};

      if (method === 'GET') {
        _.extend(params, data);
        data = null;
      }

      var options = {
        method: method,
        url: getBaseHref() + url,
        params: params,
        data: data
      };

      options.withCredentials = true;
      options.headers = options.headers || {};
      options.headers.Authorization = "Basic " + btoa(onmsUsername + ":" + onmsPassword);

      return $http(options);
    }

    function flattenResourcesWithAttributes(resources, resourcesWithAttributes) {
      _.each(resources, function (resource) {
        if (resource.rrdGraphAttributes !== undefined && Object.keys(resource.rrdGraphAttributes).length > 0) {
          resourcesWithAttributes.push(resource);
        }
        if (resource.children !== undefined && resource.children.resource.length > 0) {
          flattenResourcesWithAttributes(resource.children.resource, resourcesWithAttributes);
        }
      });
      return resourcesWithAttributes;
    }

    function doCorrelation(metric, metrics) {
      var options = {
        method: 'POST',
        url: "http://localhost:8181/cxf/karafsimple/correlator/correlate",
        params: {
          resource: metric.resource,
          metric: metric.metric,
          resolution: 36000000,
          from: start,
          to: end
        },
        data: metrics.slice(0, 10)
      };

      return $http(options);
    }

    $scope.correlatedResources = undefined;
    $scope.correlate = function(resource) {
      $scope.status = "Retrieving complete list of metrics...";
      var deferred = $q.defer();
      onmsRequest('GET', '/rest/resources', {
        depth: -1
      }).success(function (data) {
        var metrics = [];
        var resources = flattenResourcesWithAttributes(data.resource, []);
        _.each(resources, function(resource) {
          _.each(resource.rrdGraphAttributes, function(rrdGraphAttribute) {
            metrics.push({
              resource: rrdGraphAttribute.rrdFile,
              metric: rrdGraphAttribute.name,
              id: decodeURIComponent(resource.id)
            });
          });
        });
        deferred.resolve(metrics);
      }).error(function(err) {
        $scope.status = "Failed to retrieve the list of metrics: " + err;
      });

      deferred.promise.then(function(metrics) {
        $scope.metrics = metrics;
        $scope.status = "Found " + metrics.length + " metrics. Correlating...";

        var metricsWithoutId = [];
        _.each(metrics, function(metric) {
          metricsWithoutId.push({
            resource: metric.resource,
            metric: metric.metric
          });
        });

        var metric = _.find(metrics, function(metric){ return $scope.resource.id === metric.id && $scope.resource.attribute === metric.metric });
        if (metric === undefined) {
          $scope.status = "Could not find metric for resource " + JSON.stringify($scope.resource);
          return;
        }

        doCorrelation(metric, metricsWithoutId).success(function (correlatedMetrics) {
          var correlatedResources = [];

          _.each(correlatedMetrics, function(correlatedMetric) {
            var id = null;
            var metric = _.find(metrics, function(metric){ return correlatedMetric.resource === metric.resource; });
            if (metric !== undefined) {
              id = metric.id;
            }

            correlatedResources.push({
              id: id,
              attribute: correlatedMetric.metric,
              coefficient: correlatedMetric.coefficient
            });
          });

          $scope.correlatedResources = correlatedResources;
          $scope.status = "Correlation successful. Got " + correlatedResources.length + " resources. Ready!";
        }).error(function(err) {
          $scope.status = "Correlation failed: " + err;
        });
      }, function(err) {
        $scope.status = "Failed to retrieve metrics: " + err;
      });
    };

    $scope.graphRelated = function(relatedResource) {
      $scope.graph([$scope.resource, relatedResource]);
    };
}]);