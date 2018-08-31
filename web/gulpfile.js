// This is the build configuration for the frontend stuff.
//
// Commands:
//   gulp            # Build everything in the "dist" directory.
//   gulp serve      # Start an auto-updating server backed by the real LynxKite.
//   gulp test       # Protractor tests.
//   gulp test:serve # Protractor tests against the server started with "gulp serve".
'use strict';

// Port for LynxKite.
const LynxKitePort = 2200;
// Port for the development proxy.
const ProxyPort = 9090;
let LynxKiteURL;
let ProxyURL;
if (process.env.HTTPS_PORT) {
  LynxKiteURL = 'https://localhost:' + process.env.HTTPS_PORT;
  ProxyURL = 'https://localhost:' + ProxyPort;
} else {
  LynxKiteURL = 'http://localhost:' + LynxKitePort;
  ProxyURL = 'http://localhost:' + ProxyPort;
}

// The tools directory.
const tools = '../tools';

const browserSync = require('browser-sync').create();
const spawn = require('child_process').spawn;
const del = require('del');
const glob = require('glob');
const gulp = require('gulp');
const runSequence = require('run-sequence');
const fs = require('fs');
const httpProxy = require('http-proxy');
const lazypipe = require('lazypipe');
const merge = require('merge-stream');
const $ = require('gulp-load-plugins')();

// Builds HTML files from AsciiDoctor documentation.
gulp.task('asciidoctor', function () {
  // eslint-disable camelcase
  const docs = ['admin-manual', 'help'];
  const streams = [];
  for (let i = 0; i < docs.length; ++i) {
    const doc = docs[i];
    const stream = gulp.src('app/' + doc + '/index.asciidoc')
      .pipe($.asciidoctor({
        base_dir: 'app/' + doc,
        safe: 'safe',
        header_footer: false,
      }))
      .pipe($.rename(doc + '.html'));
    streams.push(stream);
  }
  return merge(streams)
    .pipe(gulp.dest('.tmp'))
    .pipe(browserSync.stream());
});

// Preprocesses HTML files.
gulp.task('html', ['css', 'js'], function () {
  const css = gulp.src('.tmp/**/*.css', { read: false });
  const js = gulp.src('.tmp/**/*.js').pipe($.angularFilesort());
  return gulp.src('app/index.html')
    .pipe($.inject(css, { ignorePath: '.tmp' }))
    .pipe($.inject(js, { ignorePath: '.tmp' }))
    .pipe(gulp.dest('.tmp'))
    .pipe(browserSync.stream());
});

// Performs the final slow steps for creating the ultimate files that are included in LynxKite.
// All the other tasks create intermediate outputs in .tmp. This task takes files from app and .tmp,
// optimizes them, and saves them in dist.
gulp.task('dist', ['asciidoctor', 'genTemplates', 'html'], function () {
  const beforeConcat = lazypipe().pipe($.sourcemaps.init, { loadMaps: true });
  const dynamicFiles = gulp.src('.tmp/**/*.html')
    .pipe($.useref({}, beforeConcat))
    .pipe($.if('*.js', $.uglifyEs.default()))
    .pipe($.if(['**/*', '!**/*.html'], $.rev()))
    .pipe($.revReplace())
    .pipe($.size({ showFiles: true, gzip: true }))
    .pipe($.sourcemaps.write('maps'));
  const staticFiles = gulp.src([
    'app/*.{png,svg}',
    'app/images/**',
    'app/**/*.html', '!app/index.html',
  ], { base: 'app' });
  // Move fonts to where the relative URLs will find them.
  const bootstrapFonts = gulp.src([
    'node_modules/bootstrap/dist/fonts/*',
  ], { base: 'node_modules/bootstrap/dist' });
  const fontAwesomeFonts = gulp.src([
    'node_modules/font-awesome/fonts/*',
  ], {base: 'node_modules/font-awesome'});
  const typefaces = gulp.src([
    'node_modules/typeface-exo-2/files/*',
  ], {base: 'node_modules/typeface-exo-2'});
  return merge(
    merge(dynamicFiles, staticFiles, bootstrapFonts, fontAwesomeFonts)
      .pipe(gulp.dest('dist')),
    typefaces.pipe(gulp.dest('dist/styles')));
});

// Compiles SCSS files into CSS.
gulp.task('sass', function () {
  return gulp.src('app/styles/*.scss')
    .pipe($.sass().on('error', $.sass.logError))
    .pipe(gulp.dest('.tmp/styles'))
    .pipe(browserSync.stream());
});

// Preprocesses CSS files.
gulp.task('css', ['sass'], function () {
  return gulp.src('app/styles/*.css')
    .pipe($.autoprefixer())
    .pipe(gulp.dest('.tmp/styles'))
    .pipe(browserSync.stream());
});

// Preprocesses JavaScript files.
gulp.task('js', function () {
  return gulp.src('app/scripts/**/*.js')
    .pipe($.ngAnnotate())
    .pipe(gulp.dest('.tmp/scripts'))
    .pipe(browserSync.stream());
});

// Lints JavaScript files.
gulp.task('eslint', function() {
  return gulp.src(['app/scripts/**/*.js', 'gulpfile.js', 'test/**/*.js'])
    .pipe($.eslint())
    .pipe($.eslint.format())
    .pipe($.eslint.failAfterError());
});

// Cleanup tasks.
gulp.task('clean:dist', function() {
  return del('dist');
});
gulp.task('clean:tmp', function() {
  return del('.tmp');
});

// Generates template files from AsciiDoc.
gulp.task('genTemplates', function(done) {
  spawn(tools + '/gen_templates.py', { stdio: 'inherit' }).once('close', done);
});

// Starts a development proxy.
// It connects to a real LynxKite server and forwards the AJAX requests to LynxKite. But it
// overlays the frontend files in .tmp, and watches the source files. Whenever you edit a source
// file, the right build task is run, and the browser automatically reloads the page. The proxy then
// serves the modified files. Very good for development.
gulp.task('serve', ['quick'], function() {
  // This is more complicated than it could be due to an issue:
  // https://github.com/BrowserSync/browser-sync/issues/933
  const proxy = httpProxy.createProxyServer();
  proxy.on('error', function(err, req, res) {
    // Lot of ECONNRESET when live-reloading for some reason. Ignore them.
    res.end();
  });
  browserSync.init({
    port: ProxyPort,
    https: LynxKiteURL.indexOf('https') === 0,
    server: ['.tmp', 'app', 'node_modules', 'dist'],
    ghostMode: false,
    online: false,
    notify: false,
  },
  function(err, bs) {
    bs.addMiddleware('',
      function pdfMiddleware(req, res, next) {
        if (req.url.indexOf('/pdf-') === 0) {
          req.url = '/index.html';
        }
        next();
      }, { override: true });
    bs.addMiddleware('',
      function proxyMiddleware(req, res) {
        proxy.web(req, res, { target: LynxKiteURL });
      });
  });
  gulp.watch('app/styles/*.{,s}css', ['css']);
  gulp.watch('app/scripts/**/*.js', ['eslint', 'js']);
  gulp.watch('app/**/*.html', ['html']);
  gulp.watch('app/**/*.asciidoc', ['asciidoctor', 'genTemplates']);
});

const protractorDir = 'node_modules/protractor/';
// Checks for webdriver updates.
gulp.task('webdriver-update', function(done) {
  const protractorConfig = require('./test/protractor.conf.js').config;
  fs.access('test/' + protractorConfig.chromeDriver, (err) => {
    if (err) {
      spawn(
        protractorDir + 'bin/webdriver-manager', ['update', '--versions.chrome=2.35'],
        { stdio: 'inherit' }).once('close', done);
    } else {
      done();
    }
  });
});

// Runs Protractor against a given port.
function runProtractor(url, done) {
  glob(protractorDir + 'selenium/selenium-server-standalone-*.jar', function(err, jars) {
    const jar = jars[jars.length - 1]; // Take the latest version.
    spawn(
      protractorDir + 'bin/protractor', [
        'test/protractor.conf.js',
        '--seleniumServerJar', jar,
        '--baseUrl', url + '/'],
      { stdio: 'inherit' }).once('close', done);
  });
}

// Runs the Protractor tests against LynxKite.
gulp.task('test', ['webdriver-update'], function(done) {
  runProtractor(LynxKiteURL, done);
});

// Runs the Protractor tests against a development proxy. (You have to start the proxy first.)
gulp.task('test:serve', ['webdriver-update'], function(done) {
  runProtractor(ProxyURL, done);
});

// The default task when you just run "gulp".
gulp.task('default', function(callback) {
  runSequence(
    ['eslint', 'clean:tmp', 'clean:dist'],
    ['dist'],
    callback);
});

// A quicker build that populates .tmp.
gulp.task('quick', ['eslint', 'html', 'asciidoctor']);
