package govind.incubator

import org.apache.spark.sql.SparkSession

object DatasetExample {
	val path = "E:\\EclipseWPFroWin10\\govind-incubator\\src\\main\\scala\\resources\\people.json"

	def main(args: Array[String]): Unit = {
		println("Dataset Test")

		val sc = SparkSession.builder()
			.master("local")
			.appName("DataSetTest")
			.getOrCreate()


		sc.read.json(path).show()
		sc.stop
	}
}
