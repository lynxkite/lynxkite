var autoprefixer = require('gulp-autoprefixer');
var asciidoctor = require('gulp-asciidoctor');
var browserSync = require('browser-sync').create();
var del = require('del');
var gulp = require('gulp');
var gulpif = require('gulp-if');
var httpProxy = require('http-proxy');
var inject = require('gulp-inject');
var jshint = require('gulp-jshint');
var merge = require('merge-stream');
var ngAnnotate = require('gulp-ng-annotate');
var rename = require('gulp-rename');
var rev = require('gulp-rev');
var revReplace = require('gulp-rev-replace');
var sass = require('gulp-sass');
var uglify = require('gulp-uglify');
var useref = require('gulp-useref');
var wiredep = require('wiredep').stream;

gulp.task('asciidoctor', function () {
  var help = gulp.src('app/help/index.asciidoc')
    .pipe(asciidoctor({
      base_dir: 'app/help',
      safe: 'safe',
      header_footer: false,
    }))
    .pipe(rename('help.html'));
  var admin = gulp.src('app/admin-manual/index.asciidoc')
    .pipe(asciidoctor({
      base_dir: 'app/admin-manual',
      safe: 'safe',
      header_footer: false,
    }))
    .pipe(rename('admin-manual.html'));
  return merge(help, admin)
    .pipe(gulp.dest('.tmp'));
});

gulp.task('html', ['css', 'js'], function () {
  var sources = gulp.src(['.tmp/**/*.js', '.tmp/**/*.css'], { read: false });
  return gulp.src('app/index.html')
    .pipe(wiredep())
    .pipe(inject(sources))
    .pipe(gulp.dest('.tmp'))
    .pipe(browserSync.stream());
});

gulp.task('dist', ['clean:dist', 'html'], function () {
  return gulp.src('.tmp/**/*.html')
    .pipe(useref())
    .pipe(gulpif('*.js', uglify()))
    .pipe(gulpif(['**/*', '!**/*.html'], rev()))
    .pipe(revReplace())
    .pipe(gulp.dest('dist'));
});

gulp.task('sass', function () {
  return gulp.src('app/styles/*.scss')
    .pipe(sass().on('error', sass.logError))
    .pipe(gulp.dest('.tmp/styles'))
    .pipe(browserSync.stream());
});

gulp.task('css', ['sass'], function () {
  return gulp.src('app/styles/*.css')
    .pipe(autoprefixer())
    .pipe(gulp.dest('.tmp/styles'))
});

gulp.task('js', function () {
  return gulp.src('app/scripts/**/*.js')
    .pipe(ngAnnotate())
    .pipe(gulp.dest('.tmp/scripts'))
});

gulp.task('jshint', function() {
  return gulp.src('app/scripts/**/*.js')
    .pipe(jshint())
    .pipe(jshint.reporter('default'));
});

gulp.task('clean:dist', function() {
  return del('dist');
});

gulp.task('serve', ['quick'], function() {
  // This is more complicated than it could be due to an issue:
  // https://github.com/BrowserSync/browser-sync/issues/933
  var proxy = httpProxy.createProxyServer({
    changeOrigin: true,
    autoRewrite: true,
    secure: false
  });
  browserSync.init({
    server: ['app', '.tmp'],
  },
  function (err, bs) {
    bs.addMiddleware('*',
      function proxyMiddleware (req, res) {
        proxy.web(req, res, { target: 'http://localhost:2200' });
      }
    );
  });

  gulp.watch('app/styles/*.scss', ['sass']);
  gulp.watch('app/**/*.js').on('change', browserSync.reload);
  gulp.watch('app/*.html').on('change', browserSync.reload);
});

gulp.task('default', ['jshint', 'asciidoctor', 'dist']);
gulp.task('quick', ['html']);
