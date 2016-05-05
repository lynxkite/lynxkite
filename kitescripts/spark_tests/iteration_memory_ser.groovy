import org.apache.spark.storage.StorageLevel

lynx.sparkTests.iterativeTest(
  storageLevel: "MEMORY_ONLY_SER",
  dataSize: params.dataSize)
