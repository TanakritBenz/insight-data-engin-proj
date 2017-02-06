var mainApp = angular.module('MainApp', ['ngAnimate']);

mainApp.controller('MainController', function MainController($scope, $sce, $http, $timeout) {
    $scope.search_topic = '';
    $scope.search_result = [];
    $scope.search_result_uuid = [];
    $scope.search_result_docid = [];
    $scope.cancelled = true;
    $scope.result_counter = 0;
    $scope.lasttime = 'unknown';
    $scope.toTrustedHTML = function(html) {
        return $sce.trustAsHtml(html);
    };
    $scope.isEmpty = function(element) {
        return _.isEmpty(element);
    }
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
                        var data = response.data.response;
                        for (var i = 0; i < data.length; i++) {
                            if ($scope.search_result_uuid.indexOf(data[i].doc_id) === -1) {
                                $scope.search_result_uuid.push(data[i].doc_id);
                                data[i].ui_id = $scope.result_counter;
                                data[i].timestamp = moment.utc(data[i].created_utc).format('DD/MM/YY, h:mm:ss a');
                                $scope.search_result.push(data[i]);
                                $scope.result_counter++;
                            }
                        }
                        if (data.length > 0) {
                            $scope.lasttime = data[0].created_utc;
                            console.log('lasttime: ', $scope.lasttime, data);
                            $scope.search_result_docid = $($scope.search_result_uuid).not($scope.search_result_docid).get();
                            $scope.search_result_docid = $scope.search_result_docid.join();
                            if (!_.isEmpty($scope.search_result_docid)) {
                                $http({
                                    method: 'GET',
                                    url: '/search_docs/' + $scope.search_result_docid
                                }).then(function successCallback(response) {
                                    var data = response.data.response;
                                    console.log('49', data);
                                    for (var i = 0; i < data.length; i++) {
                                        var index = data[i].title.indexOf($scope.search_topic);
                                        if (index >= 0) {
                                            data[i].title = data[i].title.substring(0, index) + "<span class='highlight'>" + data[i].title.substring(index, index + $scope.search_topic.length) + "</span>" + data[i].title.substring(index + $scope.search_topic.length);
                                        }
                                        var target = _.find($scope.search_result, {'doc_id': data[i].doc_id});
                                        console.log('50', target, $scope.search_result);
                                        if (!_.isEmpty(target)) {
                                            target.title = data[i].title;
                                            target.author = data[i].author;
                                            target.permalink = data[i].permalink;
                                            target.url = data[i].url;
                                            target.ups = data[i].ups;
                                            target.downs = data[i].downs;
                                            target.score = data[i].score;
                                            target.gilded = data[i].gilded;
                                        }
                                    }
                                }, function errorCallback(response) {
                                    // called asynchronously if an error occurs
                                    // or server returns response with an error status.
                                    console.log('Search failed!');
                                    console.log(response);
                                });
                            }
                        }
                        console.log($scope.search_result);
                        $scope.recTimeoutFunction($scope);
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
            console.log('Cancel searching');
            $scope.cancelled = true;
            $scope.lasttime = 'unknown';
            $scope.search_result = [];
            $scope.search_result_uuid = [];
            $scope.result_counter = 0;
        };

        $scope.stopSearch = function() {
            console.log('Stop searching');
            $scope.cancelled = true;
        };
    };

});
