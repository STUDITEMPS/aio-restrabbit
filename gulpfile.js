"use strict";

var basePaths = {
    css: 'static/css/'
};

var paths = {
    // SASS
    sass: 'sass/',
    templates: 'templates/'
};

var compass = require('compass-importer');

var gulp = require('gulp'),
    plugins = require('gulp-load-plugins')();


gulp.task('stylesheets', function () {
    return gulp.src([paths.sass+'*.scss', paths.sass+'pages/*.scss' ], {base: paths.sass} )
        .pipe(plugins.sass({outputStyle: 'compressed', importer:compass}).on('error', plugins.sass.logError))
        .pipe(gulp.dest(basePaths.css))
});


gulp.task('watch', function() {
    gulp.watch(['**/*.py'], []);
    gulp.watch([paths.sass+'**/*.scss'], ['stylesheets']);
    gulp.start('runserver');
});

gulp.task('runserver', function(){
    var process = require('child_process');
    var util = require('gulp-util');
    var runserver = process.spawn("python3", ["service.py"], {stdio: "inherit"});
    runserver.on('close', function(code) {
        if (code !== 0) {
            util.log('Service exited with error code: ' + code);
        } else {
            util.log('Service exited normally.');
        }
    });
});
