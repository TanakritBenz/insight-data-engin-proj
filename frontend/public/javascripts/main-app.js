var mainApp = angular.module('MainApp', ['ngAnimate']);

mainApp.controller('MainController', function MainController($scope, $sce, $http, $timeout) {
    $scope.toTrustedHTML = function(html) {
        return $sce.trustAsHtml(html);
    };
    $scope.search_topic = '';
    $scope.search_result = [];
    $scope.search_result_uuid = [];
    $scope.cancelled = true;
    $scope.result_counter = 0;
    $scope.lasttime = 'unknown';
    $scope.commitQuery = function() {
        console.log($scope.search_topic);
        $scope.cancelled = false;
        $scope.recTimeoutFunction = function($scope) {
            return $timeout(function($scope) {
                //only start new pulse if user hasn't cancelled
                if (!$scope.cancelled) {
                    $http({
                        method: 'GET',
                        url: '/search/' + $scope.search_topic + '/' + $scope.lasttime
                    }).then(function successCallback(response) {
                        // this callback will be called asynchronously
                        // when the response is available
                        for (var i = 0; i < response.data.response.length; i++) {
                            if ($scope.search_result_uuid.indexOf(response.data.response[i].created_utc_uuid) === -1) {
                                $scope.search_result_uuid.push(response.data.response[i].created_utc_uuid);
                                response.data.response[i].ui_id = $scope.result_counter;
                                response.data.response[i].created_utc = moment.utc(response.data.response[i].created_utc).format('MMMM Do YYYY, h:mm:ss a');
                                response.data.response[i].inserted_time = moment.utc(response.data.response[i].inserted_time).format('MMMM Do YYYY, h:mm:ss a');
                                var index = response.data.response[i].body.indexOf($scope.search_topic);
                                if (index >= 0) {
                                    response.data.response[i].body = response.data.response[i].body.substring(0, index) + "<span class='highlight'>" + response.data.response[i].body.substring(index, index + $scope.search_topic.length) + "</span>" + response.data.response[i].body.substring(index + $scope.search_topic.length);
                                }
                                $scope.search_result.push(response.data.response[i]);
                                $scope.result_counter++;
                            }
                        }
                        if (response.data.response.length > 0) {
                            $scope.lasttime = response.data.response[0].created_utc_uuid;
                            console.log('lasttime: ', $scope.lasttime);
                        }
                        console.log($scope.search_result);
                        $scope.recTimeoutFunction($scope);
                        // $scope.$apply();
                    }, function errorCallback(response) {
                        // called asynchronously if an error occurs
                        // or server returns response with an error status.
                        console.log('Search failed!');
                        console.log(response);
                    });
                }
            }, 1000, true, $scope);
        };
        //initially call the timeout function:
        $scope.recTimeoutFunction($scope);

        //function for the user to cancel pulsing messages
        $scope.cancelSearch = function() {
            console.log('Stop searching');
            $scope.cancelled = true;
        };
        $scope.changeTopic = function() {
            $scope.cancelSearch();
        };
    };

});
