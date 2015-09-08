// Generated on 2014-05-05 using generator-angular 0.8.0
'use strict';

// # Globbing
// for performance reasons we're only matching one level down:
// 'test/spec/{,*/}*.js'
// use this if you want to recursively match all subfolders:
// 'test/spec/**/*.js'

module.exports = function (grunt) {

  // Load grunt tasks automatically
  require('load-grunt-tasks')(grunt);

  // Time how long tasks take. Can help when optimizing build times
  require('time-grunt')(grunt);

  // Define the configuration for all the tasks
  grunt.initConfig({

    // Project settings
    yeoman: {
      // configurable paths
      app: require('./bower.json').appPath || 'app',
      dist: 'dist'
    },

    // Watches files for changes and runs tasks based on the changed files
    watch: {
      bower: {
        files: ['bower.json'],
        tasks: ['bowerInstall']
      },
      js: {
        files: ['<%= yeoman.app %>/scripts/{,*/}*.js'],
        tasks: ['newer:jshint:all'],
        options: {
          livereload: true
        }
      },
      jsTest: {
        files: ['test/spec/{,*/}*.js'],
        tasks: ['newer:jshint:test', 'karma']
      },
      styles: {
        files: ['<%= yeoman.app %>/styles/{,*/}*.css'],
        tasks: ['newer:copy:styles', 'autoprefixer']
      },
      sass: {
        files: ['<%= yeoman.app %>/styles/{,*/}*.scss'],
        tasks: ['newer:copy:styles', 'sass', 'autoprefixer']
      },
      help: {
        files: ['<%= yeoman.app %>/help/{,*/}*.asciidoc'],
        tasks: ['asciidoctor']
      },
      gruntfile: {
        files: ['Gruntfile.js']
      },
      livereload: {
        options: {
          livereload: '<%= connect.options.livereload %>'
        },
        files: [
          '<%= yeoman.app %>/{,*/}*.html',
          '.tmp/{,*/}*.html',
          '.tmp/styles/{,*/}*.css',
          '<%= yeoman.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
        ]
      }
    },

    // The actual grunt server settings
    connect: {
      options: {
        port: 9090,
        // Change this to '0.0.0.0' to access the server from outside.
        hostname: 'localhost',
        livereload: 35729,
        middleware: function(connect, options, mws) {
          // Custom behavior for some Ajax requests.
          mws.unshift(function(req, res, next) {
            req.method = 'GET';
            if (req.url.indexOf('/ajax/complexView') === 0) {
              if (req.url.indexOf('layout3D%22:true') !== -1) {
                req.url = req.url.replace('complexView', 'complexView-global');
              } else if (req.url.indexOf('sampled') !== -1) {
                req.url = req.url.replace('complexView', 'complexView-sampled');
              } else {
                req.url = req.url.replace('complexView', 'complexView-bucketed');
              }
            }
            if (req.url.indexOf('/ajax/createProject') === 0 ||
                req.url.indexOf('/ajax/jsError') === 0 ||
                req.url.indexOf('/ajax/saveWorkflow') === 0 ||
                req.url.indexOf('/logout') === 0) {
              res.end();
              return;
            }
            if (req.url.indexOf('/ajax/spark-status') === 0) {
              // Delay all but the first request to simulate long polling.
              if (req.url.indexOf('0') === -1) {
                setTimeout(next, 10000);
                return;
              }
            }
            next();
          });
          return mws;
        }
      },
      livereload: {
        options: {
          open: true,
          base: [
            '.tmp',
            'testdata',
            '<%= yeoman.app %>'
          ]
        }
      },
      test: {
        options: {
          port: 9002,
          base: [
            '.tmp',
            'testdata',
            '<%= yeoman.app %>'
          ]
        }
      },
      dist: {
        options: {
          base: [
            'testdata',
            '<%= yeoman.dist %>'
          ]
        }
      }
    },

    // Make sure code styles are up to par and there are no obvious mistakes
    jshint: {
      options: {
        jshintrc: '.jshintrc',
        reporter: require('jshint-stylish')
      },
      all: [
        'Gruntfile.js',
        '<%= yeoman.app %>/scripts/{,*/}*.js'
      ],
      test: {
        options: {
          jshintrc: 'test/.jshintrc'
        },
        src: ['test/spec/{,*/}*.js']
      }
    },

    // Empties folders to start fresh
    clean: {
      dist: {
        files: [{
          dot: true,
          src: [
            '.tmp',
            '<%= yeoman.dist %>/*',
            '!<%= yeoman.dist %>/.git*'
          ]
        }]
      },
      server: '.tmp'
    },

    // Compile Sass to CSS.
    sass: {
      dist: {
        files: [{
          expand: true,
          cwd: '.tmp/styles/',
          src: '{,*/}*.scss',
          dest: '.tmp/styles/',
          ext: '.css',
        }]
      }
    },

    // Add vendor prefixed styles
    autoprefixer: {
      options: {
        browsers: ['last 1 version']
      },
      dist: {
        files: [{
          expand: true,
          cwd: '.tmp/styles/',
          src: '{,*/}*.css',
          dest: '.tmp/styles/'
        }]
      }
    },

    // Compile AsciiDoc.
    asciidoctor: {
      options: {
        showNumberedHeadings: false,
        safeMode: 'safe', // Allow includes.
      },
      dist: {
        files: {
          '.tmp/help.html': ['<%= yeoman.app %>/help/index.asciidoc'],
        },
      },
    },

    // Automatically inject Bower components into the app
    bowerInstall: {
      app: {
        src: ['<%= yeoman.app %>/index.html'],
        ignorePath: '<%= yeoman.app %>/'
      }
    },

    // Renames files for browser caching purposes
    rev: {
      dist: {
        files: {
          src: [
            '<%= yeoman.dist %>/scripts/{,*/}*.js',
            '<%= yeoman.dist %>/styles/{,*/}*.css',
            '<%= yeoman.dist %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}',
            '<%= yeoman.dist %>/styles/fonts/*'
          ]
        }
      }
    },

    // Reads HTML for usemin blocks to enable smart builds that automatically
    // concat, minify and revision files. Creates configurations in memory so
    // additional tasks can operate on them
    useminPrepare: {
      html: '<%= yeoman.app %>/index.html',
      options: {
        dest: '<%= yeoman.dist %>',
        flow: {
          html: {
            steps: {
              js: ['concat'],
              css: ['cssmin']
            },
            post: {}
          }
        }
      }
    },

    // Performs rewrites based on rev and the useminPrepare configuration
    usemin: {
      html: ['<%= yeoman.dist %>/{,*/}*.html'],
      css: ['<%= yeoman.dist %>/styles/{,*/}*.css'],
      options: {
        assetsDirs: ['<%= yeoman.dist %>']
      }
    },

    // The following *-min tasks produce minified files in the dist folder
    cssmin: {
      options: {
        root: '<%= yeoman.app %>'
      }
    },

    imagemin: {
      dist: {
        files: [{
          expand: true,
          cwd: '<%= yeoman.app %>/images',
          src: '{,*/}*.{png,jpg,jpeg,gif}',
          dest: '<%= yeoman.dist %>/images'
        }]
      }
    },

    svgmin: {
      dist: {
        files: [{
          expand: true,
          cwd: '<%= yeoman.app %>/images',
          src: '{,*/}*.svg',
          dest: '<%= yeoman.dist %>/images'
        }]
      }
    },

    htmlmin: {
      dist: {
        options: {
          collapseWhitespace: true,
          conservativeCollapse: true,
          collapseBooleanAttributes: true,
          removeCommentsFromCDATA: true,
          removeOptionalTags: true
        },
        files: [{
          expand: true,
          cwd: '<%= yeoman.dist %>',
          src: ['*.html', 'views/{,*/}*.html'],
          dest: '<%= yeoman.dist %>'
        }]
      }
    },

    // ng-annotate adds and removes AngularJS dependency injection annotations.
    // It is non-intrusive so your source code stays exactly the same otherwise.
    // No lost comments or moved lines. Annotations are useful because with them
    // you're able to minify your source code using your favorite JS minifier.
    ngAnnotate: {
      dist: {
        files: [{
          expand: true,
          cwd: '.tmp/concat/scripts',
          src: '*.js',
          dest: '.tmp/concat/scripts'
        }]
      }
    },

    // Replace Google CDN references
    cdnify: {
      dist: {
        html: ['<%= yeoman.dist %>/*.html']
      }
    },

    // Copies remaining files to places other tasks can use
    copy: {
      dist: {
        files: [{
          expand: true,
          dot: true,
          cwd: '<%= yeoman.app %>',
          dest: '<%= yeoman.dist %>',
          src: [
            '*.{ico,png,jpg,svg,txt}',
            '.htaccess',
            '*.html',
            'views/{,*/}*.html',
            'images/{,*/}*.{webp}',
            'fonts/*',
            'bower_components/zeroclipboard/dist/ZeroClipboard.swf',
            'bower_components/bootstrap/dist/fonts/*'
          ]
        }, {
          expand: true,
          cwd: '.tmp/images',
          dest: '<%= yeoman.dist %>/images',
          src: ['generated/*']
        }, {
          expand: true,
          cwd: '.tmp',
          dest: '<%= yeoman.dist %>',
          src: ['{,*/}*.html']
        }]
      },
      styles: {
        expand: true,
        cwd: '<%= yeoman.app %>/styles',
        dest: '.tmp/styles/',
        src: '{,*/}*.{,s}css'
      }
    },

    // Run some tasks in parallel to speed up the build process
    concurrent: {
      server: [
        'copy:styles'
      ],
      test: [
        'copy:styles'
      ],
      dist: [
        'copy:styles',
        'imagemin',
        'svgmin'
      ]
    },

    // By default, your `index.html`'s <!-- Usemin block --> will take care of
    // minification. These next options are pre-configured if you do not wish
    // to use the Usemin blocks.
    // cssmin: {
    //   dist: {
    //     files: {
    //       '<%= yeoman.dist %>/styles/main.css': [
    //         '.tmp/styles/{,*/}*.css',
    //         '<%= yeoman.app %>/styles/{,*/}*.css'
    //       ]
    //     }
    //   }
    // },
    // uglify: {
    //   dist: {
    //     files: {
    //       '<%= yeoman.dist %>/scripts/scripts.js': [
    //         '<%= yeoman.dist %>/scripts/scripts.js'
    //       ]
    //     }
    //   }
    // },
    // concat: {
    //   dist: {}
    // },

    // Test settings
    karma: {
      unit: {
        configFile: 'karma.conf.js',
        singleRun: true
      }
    },

    protractor: {
      onGruntServe: {
        options: {
          configFile: 'test/protractor-on-grunt-serve.conf.js',
        },
      },
      onRealBackend: {
        options: {
          configFile: 'test/protractor-on-real-backend.conf.js',
          args: {
            baseUrl: 'http://localhost:' + getPort() + '/',
          },
        },
      },
    },

  });


  grunt.registerTask('serve', function (target) {
    if (target === 'dist') {
      return grunt.task.run(['build', 'connect:dist:keepalive']);
    }

    grunt.task.run([
      'clean:server',
      'bowerInstall',
      'concurrent:server',
      'sass',
      'autoprefixer',
      'asciidoctor',
      'connect:livereload',
      'watch'
    ]);
  });

  grunt.registerTask('test', [
    'clean:server',
    'bowerInstall',
    'copy:styles',
    'sass',
    'autoprefixer',
    'asciidoctor',
    'connect:test',
    'protractor:onGruntServe'
  ]);

  grunt.registerTask('test_e2e', [
    'protractor:onRealBackend'
  ]);
  function getPort() {
    return grunt.option('port') || 9000;
  }

  grunt.registerTask('build', [
    'clean:dist',
    'bowerInstall',
    'useminPrepare',
    'concurrent:dist',
    'sass',
    'autoprefixer',
    'asciidoctor',
    'concat',
    'ngAnnotate',
    'copy:dist',
    'cdnify',
    'cssmin',
    'rev',
    'usemin',
    'htmlmin'
  ]);

  grunt.registerTask('quick', [
    'clean:server',
    'bowerInstall',
    'copy:styles',
    'sass',
    'autoprefixer',
    'asciidoctor'
  ]);

  grunt.registerTask('default', [
    'newer:jshint',
    'build'
  ]);
};
