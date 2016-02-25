LynxKite performance test results
=================================

This file is the result of running the script [test_performance.sh](https://github.com/biggraph/biggraph/blob/master/test_performance.sh).
The plan is to run `test_performance.sh` at least for each release,
and update this file with the new results so that the diffs can be
nicely tracked.


The commit for which this tast was last run is: `c11ec843ebd6dcfd5725a5324004f01cf8ea5bcc`. The results of that run are below:
````
@ batch.runScript("/home/hadoop/batch/script1_centrality.groovy")
centrality distribution: {"labelType":"between","labels":["0.0","13432.5","26865.1","40297.6","53730.2","67162.7","80595.2","94027.8","107460.3","120892.8","134325.4"],"sizes":[4300,0,0,0,0,50,1700,24570,139000,330380]}
time: 181.597 seconds

@ batch.runScript("/home/hadoop/batch/script2_js.groovy")
vertices: 10000000
x: {"labelType":"between","labels":["0.0","9999998000000.1","19999996000000.2","29999994000000.3","39999992000000.4","49999990000000.5","59999988000000.6","69999986000000.7","79999984000000.8","89999982000000.9","99999980000001.0"],"sizes":[3170000,1305000,1029000,836000,735000,675000,629000,577000,530000,510000]}
time: 22.614 seconds

@ batch.runScript("/home/hadoop/batch/script3_visualization.groovy")
1: visualize one graph
  vertex sets:
    0: size= 44
  edge bundles:
    0: size= 41
2. Visualize one graph with filters
  vertex sets:
    0: size= 36
  edge bundles:
    0: size= 33
3. Get visualization of a graph plus a segmentation
  vertex sets:
    0: size= 44
    1: size= 45
  edge bundles:
    0: size= 41
    1: size= 245
    2: size= 25
4. Bucketed view
  vertex sets:
    0: size= 100
  edge bundles:
    0: size= 10000
time: 74.759 seconds
````
