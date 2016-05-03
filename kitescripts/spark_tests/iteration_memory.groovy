import org.apache.spark.storage.StorageLevel

lynx.sparkTests.iterativeTest(
  storageLevel: StorageLevel.MEMORY_ONLY(),
  dataSize: params.dataSize)
