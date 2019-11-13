package govind.incubator.repl

/**
	* @Author: 高文文
	*          Project Name: govind-incubator
	*          Date: 2019-11-7
	*/
object Main {
	private var _interp: SparkILoop =  _

	def interp = _interp

	def interp_=(other: SparkILoop) {_interp = other}


	def main(args: Array[String]): Unit = {
//		_interp = new SparkILoop
//		_interp.process(args)
	}
}
