// A benchmark script for creating histograms (with focus on attribute caching).
project = lynx.newProject('random stuff')
project.newVertexSet(size: 4000000)
project.addRandomVertexAttribute(name: 'random', dist: 'Standard Normal', seed: 13)

start_time = System.currentTimeMillis()
println "histo10: ${ project.vertexAttributes['random'].histogram(10) }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"

start_time = System.currentTimeMillis()
println "histo11: ${ project.vertexAttributes['random'].histogram(11) }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"

start_time = System.currentTimeMillis()
println "histo12: ${ project.vertexAttributes['random'].histogram(12) }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"

start_time = System.currentTimeMillis()
println "histo13: ${ project.vertexAttributes['random'].histogram(13) }"
println "time: ${ (System.currentTimeMillis() - start_time) / 1000 } seconds"

System.console().readLine '...'
