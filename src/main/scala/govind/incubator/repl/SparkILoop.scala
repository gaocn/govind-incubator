package govind.incubator.repl


import org.apache.spark.internal.Logging

import scala.tools.nsc.interpreter.LoopCommands

/**
	* @Author: 高文文
	*          Project Name: govind-incubator
	*          Date: 2019-11-7
	*
	*
	*
	*/
abstract class SparkILoop extends AnyRef with LoopCommands with SparkILoopInit with Logging {

	def process(args: Array[String]): Unit = ???

}
