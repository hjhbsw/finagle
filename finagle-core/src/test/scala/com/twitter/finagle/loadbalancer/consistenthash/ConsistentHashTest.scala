package com.twitter.finagle.loadbalancer.consistenthash

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.ConsistentHash.ConsistentHashBalancer
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Activity, Future, Time, Var}
import org.scalatest.FunSuite

class ConsistentHashTest extends FunSuite {

  case class MyServiceFactory(port: Int) extends EndpointFactory[Unit, Int] {
    def remake() = {}

    def address = Address("127.0.0.1", port)

    /**
      * Reserve the use of a given service instance. This pins the
      * underlying channel and the returned service has exclusive use of
      * its underlying connection. To relinquish the use of the reserved
      * [[Service]], the user must call [[Service.close()]].
      */
    override def apply(conn: ClientConnection): Future[Service[Unit, Int]] = {

      Future.value(new Service[Unit, Int] {
        def apply(req: Unit): Future[Int] = Future.value(1)

        override def close(deadline: Time): Future[Unit] = {
          Future.Done
        }
      })

    }

    override def close(deadline: Time): Future[Unit] = Future.Done

    override def status: Status = Status.Open
  }

  protected val noBrokers: NoBrokersAvailableException = new NoBrokersAvailableException

  def newBal(
              fs: Var[Vector[MyServiceFactory]],
              sr: StatsReceiver = NullStatsReceiver
            ): ConsistentHashBalancer[Unit, Int] = new ConsistentHashBalancer(
    Activity(fs.map(Activity.Ok(_))),
    statsReceiver = sr,
    emptyException = noBrokers,
    maxEffort = 1,
    virtualNodeNum = 5
  )

  // number of servers
  val N: Int = 10
  // number of reqs
  val R: Int = 10

  test("Balances evenly") {
    val init = Vector.tabulate(N) { i =>
      MyServiceFactory(i)
    }
    val bal = newBal(Var(init))
    for (index <- 0 until R){
      var callId = "session-id"+index

      bal(HeadConnection(callId))

    }

  }
}
