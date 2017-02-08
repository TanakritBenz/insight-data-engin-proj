var drawChart = function($scope) {
    Highcharts.chart('graph-container', {
        title: {
            text: 'Number of Upvotes per 5 Seconds',
            x: -20 //center
        },
        xAxis: {
            categories: $scope.xAxis
        },
        yAxis: {
            title: {
                text: 'upvotes'
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
        tooltip: {
            valueSuffix: 'upvotes'
        },
        legend: {
            layout: 'vertical',
            align: 'right',
            verticalAlign: 'middle',
            borderWidth: 0
        },
        series: $scope.series
    });
};
