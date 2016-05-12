import org.apache.spark.storage.StorageLevel

lynx.sparkTests.cacheTest(
  storageLevel: "DISK_ONLY",
  dataSize: params.dataSize)
