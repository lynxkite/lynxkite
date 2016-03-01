// This is the build configuration for the frontend stuff.
//
// Commands:
//   gulp            # Build everything in the "dist" directory.
//   gulp serve      # Start an auto-updating server backed by the real LynxKite.
//   gulp test       # Protractor tests.
//   gulp test:serve # Protractor tests against the server started with "gulp serve".
'use strict';
var browserSync = require('browser-sync').create();
var spawn = require('child_process').spawn;
var del = require('del');
var glob = require('glob');
var gulp = require('gulp');
var httpProxy = require('http-proxy');
var merge = require('merge-stream');
var wiredep = require('wiredep').stream;
var $ = require('gulp-load-plugins')();

gulp.task('asciidoctor', function () {
  // jshint camelcase: false
  var help = gulp.src('app/help/index.asciidoc')
    .pipe($.asciidoctor({
      base_dir: 'app/help',
      safe: 'safe',
      header_footer: false,
    }))
    .pipe($.rename('help.html'));
  var admin = gulp.src('app/admin-manual/index.asciidoc')
    .pipe($.asciidoctor({
      base_dir: 'app/admin-manual',
      safe: 'safe',
      header_footer: false,
    }))
    .pipe($.rename('admin-manual.html'));
  return merge(help, admin)
    .pipe(gulp.dest('.tmp'));
});

gulp.task('html', ['css', 'js'], function () {
  var css = gulp.src('.tmp/**/*.css', { read: false });
  var js = gulp.src('.tmp/**/*.js').pipe($.angularFilesort());
  return gulp.src('app/index.html')
    .pipe(wiredep())
    .pipe($.inject(css, { ignorePath: '.tmp' }))
    .pipe($.inject(js, { ignorePath: '.tmp' }))
    .pipe(gulp.dest('.tmp'))
    .pipe(browserSync.stream());
});

gulp.task('dist', ['clean:dist', 'html'], function () {
  var dynamicFiles = gulp.src('.tmp/**/*.html')
    .pipe($.useref())
    .pipe($.if('*.js', $.uglify()))
    .pipe($.if(['**/*', '!**/*.html'], $.rev()))
    .pipe($.revReplace())
    .pipe($.size({ showFiles: true, gzip: true }));
  var staticFiles = gulp.src([
    'app/*.{png,svg}',
    'app/images/*',
    'app/bower_components/zeroclipboard/dist/ZeroClipboard.swf',
    'app/bower_components/bootstrap/dist/fonts/*',
  ]);
  return merge(dynamicFiles, staticFiles)
    .pipe(gulp.dest('dist'));
});

gulp.task('sass', function () {
  return gulp.src('app/styles/*.scss')
    .pipe($.sass().on('error', $.sass.logError))
    .pipe(gulp.dest('.tmp/styles'))
    .pipe(browserSync.stream());
});

gulp.task('css', ['sass'], function () {
  return gulp.src('app/styles/*.css')
    .pipe($.autoprefixer())
    .pipe(gulp.dest('.tmp/styles'))
    .pipe(browserSync.stream());
});

gulp.task('js', function () {
  return gulp.src('app/scripts/**/*.js')
    .pipe($.ngAnnotate())
    .pipe(gulp.dest('.tmp/scripts'))
    .pipe(browserSync.stream());
});

gulp.task('jshint', function() {
  return gulp.src(['app/scripts/**/*.js', 'gulpfile.js', 'test/**/*.js'])
    .pipe($.jshint())
    .pipe($.jshint.reporter('default'));
});

gulp.task('clean:dist', function() {
  return del('dist');
});

gulp.task('serve', ['quick'], function() {
  // This is more complicated than it could be due to an issue:
  // https://github.com/BrowserSync/browser-sync/issues/933
  var proxy = httpProxy.createProxyServer();
  browserSync.init({
    server: ['.tmp', 'app'],
    ghostMode: false,
    online: false,
    notify: false,
  },
  function (err, bs) {
    bs.addMiddleware('*',
      function proxyMiddleware (req, res) {
        proxy.web(req, res, { target: 'http://localhost:2200' });
      }
    );
  });
  gulp.watch('app/styles/*.scss', ['sass']);
  gulp.watch('app/scripts/**/*.js', ['jshint', 'js']);
  gulp.watch('app/*.html', ['html']);
});

var protractorDir = 'node_modules/protractor/';
gulp.task('webdriver-update', function(done) {
  spawn(
    protractorDir + 'bin/webdriver-manager', ['update'],
    { stdio: 'inherit' }).once('close', done);
});

function runProtractor(args, done) {
  glob(protractorDir + 'selenium/selenium-server-standalone-*.jar', function(err, jars) {
    var jar = jars[jars.length - 1]; // Take the latest version.
    spawn(
      protractorDir + 'bin/protractor', [
      'test/protractor.conf.js',
      '--seleniumServerJar', jar].concat(args),
      { stdio: 'inherit' }).once('close', done);
  });
}

gulp.task('test', ['webdriver-update'], function(done) {
  runProtractor([], done);
});

gulp.task('test:serve', ['webdriver-update'], function(done) {
  runProtractor(['--baseUrl', 'http://localhost:3000/'], done);
});

gulp.task('default', ['jshint', 'asciidoctor', 'dist']);
gulp.task('quick', ['jshint', 'html']);
