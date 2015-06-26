'use strict';

angular.module('myApp.primaryView', ['ngRoute'])

.config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/primaryView', {
    templateUrl: 'primaryView/primaryView.html',
    controller: 'PrimaryViewCtrl'
  });
}])

.controller('PrimaryViewCtrl', ['$scope', '$q', function($scope, $q) {
    $scope.resource = {
      id: 'node[70].nodeSnmp[]',
      attribute: 'SysRawContext'
    };

    $scope.graph = function(resources) {
      var onmsHost = "demo.opennms.org",
        onmsPort = 80,
        onmsUsername = 'demo',
        onmsPassword = 'demo',
        end = Date.now(),
        start = end - (24 * 60 * 60 * 1000); // 24 hours ago

      function getBaseHref() {
        return "http://" + onmsHost + ":" + onmsPort;
      }

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
        url: getBaseHref() + "/opennms/rest/measurements",
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

    $scope.correlatedResources = undefined;
    $scope.correlate = function(resource) {
      var deferred = $q.defer();
      deferred.resolve([
        {
          id: 'node[70].nodeSnmp[]',
          attribute: 'CpuRawUser',
          coefficient: 0.9
        }
      ]);

      var promise = deferred.promise;
      promise.then(function(resources) {
        $scope.correlatedResources = resources;
      });
    };

    $scope.graphRelated = function(relatedResource) {
      $scope.graph([$scope.resource, relatedResource]);
    };
}]);