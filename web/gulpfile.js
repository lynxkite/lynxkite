var autoprefixer = require('gulp-autoprefixer');
var browserSync = require('browser-sync').create();
var filter = require('gulp-filter');
var gulp = require('gulp');
var httpProxy = require('http-proxy');
var inject = require('gulp-inject');
var jshint = require('gulp-jshint');
var rev = require('gulp-rev');
var sass = require('gulp-sass');
var uglify = require('gulp-uglify');
var useref = require('gulp-useref');
var wiredep = require('wiredep').stream;

gulp.task('html', ['css'], function () {
  var sources = gulp.src(['app/**/*.js', '.tmp/**/*.css'], { read: false });
  return gulp.src('app/index.html')
    .pipe(wiredep())
    .pipe(inject(sources))
    .pipe(gulp.dest('dist'))
    .pipe(browserSync.stream());
});

gulp.task('prod', ['css', 'html'], function () {
  var jsFilter = filter('**/*.js');
  var userefAssets = useref.assets();
  return gulp.src('app/index.html')
    .pipe(userefAssets)
    .pipe(jsFilter)
    .pipe(uglify())
    .pipe(jsFilter.restore())
    .pipe(rev())
    .pipe(userefAssets.restore())
    .pipe(useref())
    .pipe(revReplace())
    .pipe(gulp.dest('dist'));
});

gulp.task('sass', function () {
  return gulp.src('app/styles/*.scss')
    .pipe(sass().on('error', sass.logError))
    .pipe(gulp.dest('.tmp/styles/'))
    .pipe(browserSync.stream());
});

gulp.task('css', ['sass'], function () {
  return gulp.src('app/styles/*.css')
    .pipe(autoprefixer())
    .pipe(gulp.dest('.tmp/styles/'))
});


gulp.task('jshint', function() {
  return gulp.src('app/scripts/**/*.js')
    .pipe(jshint())
    .pipe(jshint.reporter('default'));
});

gulp.task('clean', function() {
  return gulp.src('dist')
    .pipe(clean());
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

gulp.task('default', ['jshint', 'prod']);
gulp.task('quick', ['html', 'css']);
