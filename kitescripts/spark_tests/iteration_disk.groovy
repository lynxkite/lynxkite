import org.apache.spark.storage.StorageLevel

lynx.sparkTests.iterativeTest(
  storageLevel: StorageLevel.DISK_ONLY(),
  dataSize: params.dataSize)
