package govind.incubator

import org.scalatest.{FunSuite, Outcome}

abstract class AbstractIncubatorSuite extends FunSuite{
	override protected def withFixture(test: NoArgTest): Outcome = {
		val testName = test.text
		val suiteName =this.getClass.getName

		try{
			println(s"\n\n===== TEST OUTPUT FOR ${suiteName}: '$testName' =====\n")
			test()
		} finally {
			println(s"\n\n===== FINISHED ${suiteName}: '$testName' =====\n")
		}
	}
}
