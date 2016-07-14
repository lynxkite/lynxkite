#!/usr/bin/env python

'''Simple script to generate a shell script that does many one-run.sh calls.'''


def gen_runs(globs, exec_mems, exec_coress, vs_per_partitions, caches):
  for glob in globs:
    for exec_mem in exec_mems:
      for exec_cores in exec_coress:
        for vs_per_partition in vs_per_partitions:
          for cache in caches:
            print "EXECUTOR_MEMORY=%s NUM_CORES_PER_EXECUTOR=%s KITE_VERTICES_PER_PARTITION=%s GLOB='%s' CACHE_IN_IMPORT=%s biggraphstage/tools/one-run.sh" % (exec_mem, exec_cores, vs_per_partition, glob, cache)

"""
gen_runs(
  ['S3$kite-benchmark/inputs/fast-random-10M-100M-2/data/part-*'],
  ['10g', '20g'],
  [1,8],
  [200, 400, 800, 1600, 3200],
  ['false'])
"""
gen_runs(
    ['S3$kite-benchmark/inputs/scale-free-random-1M-10M/data/part-*'],
    ['10g', '20g'],
    [1, 8],
    [1000000, 100000, 10000],
    ['false', 'true'])
