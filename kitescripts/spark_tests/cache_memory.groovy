import org.apache.spark.storage.StorageLevel

lynx.sparkTests.cacheTest(
  storageLevel: "MEMORY_ONLY",
  dataSize: params.dataSize)
