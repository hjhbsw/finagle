package com.twitter.finagle.loadbalancer.IpAndRoundRobin

import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.ConsistHash.{Balancer2, DistributorT2, Updating2}
import com.twitter.finagle.loadbalancer._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.{Activity, Future, Time}

import scala.collection.immutable.Stream.Empty
import scala.collection.mutable

/**
 * A simple round robin balancer that chooses the next backend in
 * the list for each request. This balancer doesn't take a node's
 * load into account and as such doesn't mix-in a load metric.
 */
private[loadbalancer] final class IpAndRoundRobinBalancer[Req, Rep](
  protected val endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]],
  protected val statsReceiver: StatsReceiver,
  protected val emptyException: NoBrokersAvailableException,
  protected val maxEffort: Int = 5
) extends ServiceFactory[Req, Rep]
    with Balancer2[Req, Rep]
    with Updating2[Req, Rep] {

  protected[this] val maxEffortExhausted: Counter =
    statsReceiver.counter("max_effort_exhausted")

  protected class Node(val factory: EndpointFactory[Req, Rep])
      extends ServiceFactoryProxy[Req, Rep](factory)
      with NodeT[Req, Rep] {
    // Note: These stats are never updated.
    def load: Double = 0.0
    def pending: Int = 0

    override def close(deadline: Time): Future[Unit] = factory.close(deadline)
    override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = factory(conn)
  }

  /**
   * A simple round robin distributor.
   */
  protected class Distributor(vector: Vector[Node]) extends DistributorT2[Node](vector) {
    type This = Distributor

    /**
     * Indicates if we've seen any down nodes during `pick` which
     * we expected to be available
     */
    @volatile
    protected[this] var sawDown = false

    /**
     * `up` is the Vector of nodes that were `Status.Open` at creation time.
     * `down` is the Vector of nodes that were not `Status.Open` at creation time.
     *
     *  Since RR is not probabilistic in its selection, we're relying on partition
     *  and select only from healthy nodes.
     */
    private[this] val (up: Vector[Node], down: Vector[Node]) =
      vector.partition(_.isAvailable)

    /**
     * Utility used by [[pick()]].
     *
     * If all nodes are down, we might as well try to send requests somewhere
     * as our view of the world may be out of date.
     */
    protected[this] val selections: Vector[Node] =
      if (up.isEmpty) down else up


    private[this] val ipMap: mutable.Map[String,Node] = mutable.Map()

    /**
      * init loadBalancer``
      */
    for(oneNode <- selections){
      val nodeStr = oneNode.factory.address.toString

      ipMap(nodeStr) = oneNode

      DefaultLogger.log(Level.INFO,"init ip-round-robin balancer ip="+nodeStr)

    }

    private[this] val currentNode = new AtomicLong()

    // For each node that's requested, we move the currentNode index
    // around the wheel using mod arithmetic. This is the round robin
    // of our balancer.
    private def chooseNext(vecSize: Int): Int = {
      val next = currentNode.getAndIncrement()
      math.abs(next % vecSize).toInt
    }

    def pick(conn: ClientConnection): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)

      var node : Node = null;
      if(conn.isInstanceOf[HeadConnection] && !sawDown)
      {
        var ip = conn.asInstanceOf[HeadConnection].stickyId

        val one = ipMap.get(ip);

        one match {
          case None => node = selections(chooseNext(selections.size))
          case Some(i) => node = i;
        }
      }else{
        node = selections(chooseNext(selections.size))
      }

      if (node.status != Status.Open)
        sawDown = true
      node
    }

    def rebuild(): This = new Distributor(vector)
    def rebuild(vector: Vector[Node]): This = new Distributor(vector)

    def needsRebuild: Boolean = {
      // while the `nonEmpty` check isn't necessary, it is an optimization
      // to avoid the iterator allocation in the common case where `down`
      // is empty.
      sawDown || (down.nonEmpty && down.exists(_.isAvailable))
    }
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty)

  protected def newNode(factory: EndpointFactory[Req, Rep]): Node = new Node(factory)
  protected def failingNode(cause: Throwable): Node = new Node(new FailingEndpointFactory(cause))
}
