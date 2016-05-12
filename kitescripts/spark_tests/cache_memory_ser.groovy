import org.apache.spark.storage.StorageLevel

lynx.sparkTests.cacheTest(
  storageLevel: "MEMORY_ONLY_SER",
  dataSize: params.dataSize)
