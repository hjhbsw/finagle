package com.twitter.finagle.loadbalancer.aperture

import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ProcessCoordinateTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import ProcessCoordinate._

  test("update coordinate") {
    var coordinate: Option[Coord] = None
    val closable = ProcessCoordinate.changes.respond(coordinate = _)
    ProcessCoordinate.setCoordinate(1, 2, 10)
    assert(coordinate.isDefined)

    assert(ProcessCoordinate() == coordinate)

    ProcessCoordinate.unsetCoordinate()
    assert(coordinate.isEmpty)
  }

  test("setCoordinate") {
    val offset = 0
    val numInstances = 10

    ProcessCoordinate.setCoordinate(offset, 1, numInstances)
    val coord0 = ProcessCoordinate()

    ProcessCoordinate.setCoordinate(offset, 2, numInstances)
    val coord1 = ProcessCoordinate()

    assert(coord0.isDefined)
    assert(coord1.isDefined)
    assert(coord0 != coord1)
  }

  test("setCoordinate range") {
    forAll { (peerOffset: Int, instanceId: Int, numInstances: Int) =>
      whenever(numInstances > 0) {
        ProcessCoordinate.setCoordinate(peerOffset, instanceId, numInstances)
        val sample = ProcessCoordinate()
        assert(sample.isDefined)
        val offset = sample.get.offset
        val rng = new scala.util.Random(12345L)
        val width = sample.get.width(units = rng.nextInt(numInstances))
        assert(offset >= 0 && offset < 1.0)
        assert(width >= 0 && width < 1.0)
      }
    }
  }
}
