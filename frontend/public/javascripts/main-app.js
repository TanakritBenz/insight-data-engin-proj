var mainApp = angular.module('MainApp', ['ngAnimate']);

mainApp.controller('MainController', function MainController($scope) {
    $scope.search_topic = '';
    $scope.commitQuery = function(){
        console.log($scope.search_topic);
    };
});
