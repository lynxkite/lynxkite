import org.apache.spark.storage.StorageLevel

lynx.sparkTests.iterativeTest(
  storageLevel: "DISK_ONLY",
  dataSize: params.dataSize)
