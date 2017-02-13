var mainApp = angular.module('MainApp', ['ngAnimate']);

mainApp.controller('MainController', function MainController($scope, $sce, $http, $timeout) {
    $scope.search_topics = '';
    $scope.search_result = [];
    $scope.cancelled = true;
    $scope.result_counter = 0;
    $scope.lasttime_map = {};
    $scope.seriesOptions1 = [];
    $scope.seriesOptions2 = [];
    $scope.graphReady = false;
    $scope.toTrustedHTML = function(html) {return $sce.trustAsHtml(html);};
    $scope.isEmpty = function(element) {return _.isEmpty(element);};


    $scope.search_call_wrapper = function(topic, lasttime, cb) {
        $http({
            method: 'GET',
            url: '/search/' + topic + '/' + lasttime
        }).then(function successCallback(response) {
            if (response.data.hasOwnProperty('res')) {
                var data = response.data.res;
                if (data.length > 0) {
                    data = _.sortBy(data, function(datum) {return moment.utc(datum.created_utc);});
                    var doc_ids = _.map($scope.search_result, 'doc_id');
                    for (var i = 0; i < data.length; i++) {
                        if (_.includes(doc_ids, data[i].doc_id)) {
                            continue;
                        }
                        data[i].ui_id = $scope.result_counter;
                        data[i].timestamp = moment.utc(data[i].created_utc).format('DD/MM/YY, HH:mm:ss');
                        $scope.search_result.push(data[i]);
                        $scope.result_counter++;

                        var index = data[i].title.indexOf(topic);
                        if (index >= 0) {
                            data[i].title = data[i].title.substring(0, index) + "<span class='highlight'>" + data[i].title.substring(index, index + topic.length) + "</span>" + data[i].title.substring(index + topic.length);
                        }
                    }
                    var lasttime_max = moment.max([moment.utc(data[0].created_utc), moment.utc(data[data.length-1].created_utc)]);
                    cb(lasttime_max);
                }
            }
        }, function errorCallback(response) {
            // called asynchronously if an error occurs
            // or server returns response with an error status.
            console.log('Search failed!');
            console.log(response);
        });
    };

    $scope.regis_query_wrapper = function(topic) {
        $http({
            method: 'GET',
            url: '/register_query/' + topic
        }).then(function successCallback(response) {
            // console.log(response);
        }, function errorCallback(response) {
            console.log(response);
        });
    };

    $scope.get_chart_data_wrapper = function(topic, cb) {
        $http({
            method: 'GET',
            url: '/get_graph_data/' + topic
        }).then(function successCallback(response) {
            cb(response.data.res);
        }, function errorCallback(response) {
            console.log(response);
        });
    };

    $scope.commitQuery = function() {
        $scope.cancelled = false;
        var topics = $scope.search_topics.split(',');

        // Register each topic to ES
        _.forEach(topics, function(topic, i) {
            $scope.seriesOptions1[i] = { 'name': topic, 'data': [] };
            $scope.seriesOptions2[i] = { 'name': topic, 'data': [] };
            $scope.regis_query_wrapper(topic);
            $scope.get_chart_data_wrapper(topic, function(data){
                $scope.graphReady = true;
                var serie1 = _.find($scope.seriesOptions1, function(o) { return o.name == topic; });
                var serie2 = _.find($scope.seriesOptions2, function(o) { return o.name == topic; });
                var tmp_data1 = [];
                var tmp_data2 = [];
                console.log('data', data);
                _.forEach(data, function(datum){
                    tmp_data1.push([parseInt(datum['system.tounixtimestamp(time_utc)']), datum.ups]);
                    tmp_data2.push([parseInt(datum['system.tounixtimestamp(time_utc)']), datum.ups+datum.downs]);
                });
                serie1.data = tmp_data1;
                serie2.data = tmp_data2;
                if (i === topics.length-1){
                    setTimeout(function(){
                        var chart1 = drawChart1($scope);
                        var chart2 = drawChart2($scope);
                        // chart1.reflow();
                        // chart2.reflow();
                    }, 100);
                }
                console.log('series1', $scope.seriesOptions1);
                console.log('series2', $scope.seriesOptions2);
            });
        });

        // Loop call Cassandra for new Reddit posts data
        $scope.recTimeoutFunction = function($scope) {
            return $timeout(function($scope) {
                // $(window).resize();
                if (!$scope.cancelled) { //only start new pulse if user hasn't cancelled
                    _.forEach(topics, function(topic) {
                        if (moment.isMoment($scope.lasttime_map[topic])) {
                            $scope.search_call_wrapper(topic, $scope.lasttime_map[topic].utc().format(), function(lasttime_max){
                                $scope.lasttime_map[topic] = moment.max([lasttime_max, $scope.lasttime_map[topic]]);
                            });
                        } else {
                            var lasttime = 'unknown';
                            $scope.lasttime_map[topic] = 'unknown';
                            $scope.search_call_wrapper(topic, lasttime, function(lasttime_max){
                                // console.log(lasttime_max);
                                // console.log($scope.lasttime_map[topic]);
                                if ($scope.lasttime_map[topic] === 'unknown'){
                                    $scope.lasttime_map[topic] = lasttime_max;
                                } else {
                                    $scope.lasttime_map[topic] = moment.max([lasttime_max, $scope.lasttime_map[topic]]);
                                }
                            });
                        }
                    });
                    $scope.recTimeoutFunction($scope);
                }
            }, 500, true, $scope);
        };
        $scope.recTimeoutFunction($scope); //initially call the timeout function:

        $scope.cancelSearch = function() {
            console.log('Cancel searching');
            $scope.cancelled = true;
            $scope.lasttime_map = {};
            $scope.search_result = [];
            $scope.result_counter = 0;
            $scope.seriesOptions1 = [];
            $scope.seriesOptions2 = [];
            $scope.graphReady = false;
        };

        $scope.stopSearch = function() {
            console.log('Stop searching');
            $scope.cancelled = true;
        };

    };
});
