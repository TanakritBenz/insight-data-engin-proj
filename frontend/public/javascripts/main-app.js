var mainApp = angular.module('MainApp', ['ngAnimate']);

mainApp.controller('MainController', function MainController($scope, $sce, $http, $timeout) {
    $scope.search_topic = '';
    $scope.search_result = [];
    $scope.search_result_uuid = [];
    $scope.search_result_docid = [];
    $scope.cancelled = true;
    $scope.first_call = '1';
    $scope.result_counter = 0;
    $scope.lasttime = 'unknown';
    $scope.series = [];
    $scope.xAxis = [];
    $scope.toTrustedHTML = function(html) {
        return $sce.trustAsHtml(html);
    };
    $scope.isEmpty = function(element) {
        return _.isEmpty(element);
    };
    $scope.getGraphData = function(topic, time, cb) {
        $http({
            method: 'GET',
            url: '/get_graph_data/' + topic + '/' + moment(time).subtract(5, 'minutes').format() + '/' + time.format()
        }).then(function successCallback(response) {
            if (response.data.hasOwnProperty('res')) {
                var data = response.data.res;
                console.log('upvote data: ', data);
                if (!_.isEmpty(data)) {
                    var target = _.find($scope.series, function(o) {
                        return o.name === topic;
                    });
                    target.data.push(parseInt(data[0].count));
                    $scope.xAxis.push(time.utc().format());
                    $scope.xAxis = _.uniq($scope.xAxis);
                    console.log('plot :', $scope.series, $scope.xAxis);
                }
                cb();
            }
        }, function errorCallback(response) {
            console.log(response);
        });

    };

    $scope.commitQuery = function() {
        $scope.series = _.map($scope.search_topic.split(','), function(topic){ return {'name': topic, 'data': []}; });
        $scope.cancelled = false;
        $scope.recTimeoutFunction = function($scope) {
            return $timeout(function($scope) {
                //only start new pulse if user hasn't cancelled
                if (!$scope.cancelled) {
                    if (moment.isMoment($scope.lasttime)) {
                        $scope.lasttime = $scope.lasttime.utc().format();
                    }
                    $http({
                        method: 'GET',
                        url: '/search/' + $scope.search_topic + '/' + $scope.lasttime + '/' + $scope.first_call
                    }).then(function successCallback(response) {
                        if (response.data.hasOwnProperty('res')) {
                            var data = response.data.res;
                            if (data.length > 0) {
                                for (var i = 0; i < data.length; i++) {
                                    if ($scope.search_result_uuid.indexOf(data[i].doc_id) === -1) {
                                        $scope.search_result_uuid.push(data[i].doc_id);
                                        data[i].ui_id = $scope.result_counter;
                                        data[i].timestamp = moment.utc(data[i].created_utc).format('DD/MM/YY, h:mm:ss a');
                                        $scope.search_result.push(data[i]);
                                        $scope.result_counter++;
                                        if (moment.isMoment($scope.lasttime)) {
                                            $scope.lasttime = moment.max([$scope.lasttime, moment.utc(data[i].created_utc)]);
                                        } else {
                                            $scope.lasttime = moment.utc(data[i].created_utc);
                                        }
                                    }
                                }
                                $scope.first_call = '0';
                                $scope.search_result_docid = $($scope.search_result_uuid).not($scope.search_result_docid).get();
                                $scope.search_result_docid = $scope.search_result_docid.join();
                                if (!_.isEmpty($scope.search_result_docid)) {
                                    $http({
                                        method: 'GET',
                                        url: '/search_docs/' + $scope.search_result_docid
                                    }).then(function successCallback(response) {
                                        if (response.data.hasOwnProperty('res')) {
                                            var data = response.data.res;
                                            for (var i = 0; i < data.length; i++) {
                                                // _.forEach($scope.search_topic, function(topic) {
                                                //     var index = data[i].title.indexOf(topic);
                                                //     if (index >= 0) {
                                                //         data[i].title = data[i].title.substring(0, index) + "<span class='highlight'>" + data[i].title.substring(index, index + topic.length) + "</span>" + data[i].title.substring(index + topic.length);
                                                //     }
                                                // });

                                                var target = _.find($scope.search_result, {
                                                    'doc_id': data[i].doc_id
                                                });
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
                                        }
                                    }, function errorCallback(response) {
                                        // called asynchronously if an error occurs
                                        // or server returns response with an error status.
                                        console.log('Search failed!');
                                        console.log(response);
                                    });
                                }
                                _.forEach($scope.search_topic.split(','), function(topic) {
                                    if (moment.isMoment($scope.lasttime)) {
                                        $scope.getGraphData(topic, $scope.lasttime, function(){
                                            drawChart($scope);
                                        });
                                    }
                                });
                            }
                        }
                        $scope.recTimeoutFunction($scope);
                    }, function errorCallback(response) {
                        // called asynchronously if an error occurs
                        // or server returns response with an error status.
                        console.log('Search failed!');
                        console.log(response);
                    });
                }
            }, 2000, true, $scope);
        };
        //initially call the timeout function:
        $scope.recTimeoutFunction($scope);

        //function for the user to cancel pulsing messages
        $scope.cancelSearch = function() {
            console.log('Cancel searching');
            $scope.cancelled = true;
            $scope.lasttime = 'unknown';
            $scope.first_call = '1';
            $scope.search_result = [];
            $scope.search_result_uuid = [];
            $scope.result_counter = 0;
            $scope.series = [];
            $scope.xAxis = [];
        };

        $scope.stopSearch = function() {
            console.log('Stop searching');
            $scope.cancelled = true;
        };
    };
});
