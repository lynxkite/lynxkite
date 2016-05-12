import org.apache.spark.storage.StorageLevel

lynx.sparkTests.cacheTest(
  storageLevel: "MEMORY_AND_DISK_SER",
  dataSize: params.dataSize)
