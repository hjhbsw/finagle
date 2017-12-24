package com.twitter.finagle.loadbalancer.ConsistentHash

import java.util
import java.util.logging.Level

import com.twitter.finagle.loadbalancer._
import com.twitter.finagle.stats.{Counter, StatsReceiver}
import com.twitter.finagle.{HeadConnection, _}
import com.twitter.finagle.loadbalancer.ConsistHash.{Balancer2, DistributorT2, Updating2}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.{Activity, Future, Time}


private[loadbalancer] final class ConsistentHashBalancer[Req, Rep](
  protected val endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]],
  protected val statsReceiver: StatsReceiver,
  protected val emptyException: NoBrokersAvailableException,
  protected val maxEffort: Int = 1,
  val virtualNodeNum: Int = 5
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


    private val map = new util.TreeMap[Int,Node]()

    private def getHash(str: String) = {
      val p = 16777619
      var hash = 2166136261l.toInt

      for(c <- str){
        hash = (hash ^ c) * p
      }
      hash += hash << 13
      hash ^= hash >> 7
      hash += hash << 3
      hash ^= hash >> 17
      hash += hash << 5
      // 如果算出来的值为负数则取其绝对值
      if (hash < 0) hash = Math.abs(hash)
      hash
    }


    /**
      * Utility used by [[pick()]].
      *
      * If all nodes are down, we might as well try to send requests somewhere
      * as our view of the world may be out of date.
      */
    protected[this] val selections: Vector[Node] =
      if (up.isEmpty) down else up

    /**
      * init loadBalancer``
      */
    for(oneNode <- selections){
       val nodeStr = oneNode.factory.address.toString

       for(i <- 0 until virtualNodeNum){
         val virtualNode = nodeStr + "&&VN" + i

         val hash = getHash(virtualNode)

         map.put(hash,oneNode)

         DefaultLogger.log(Level.INFO,"init consistent-hash balancer hash="+hash+",virtualNode="+virtualNode)
       }
    }

    var intIndex = 0
    override def pick(conn: ClientConnection): Node = {
      if (vector.isEmpty )
        return failingNode(emptyException)

      var callId = ""

      if(conn.isInstanceOf[HeadConnection])
      {
        callId = conn.asInstanceOf[HeadConnection].stickyId

      }else{

        DefaultLogger.log(Level.INFO,"pick without no head")
        intIndex += 1
        callId = intIndex.toString
      }

      val hash: Int = getHash(callId)

      DefaultLogger.log(Level.INFO,"pick with session-id ="+callId+",hash="+hash)

      val sub: util.SortedMap[Int,Node] = map.tailMap(hash)

      var resultNode: Node= null
      var key:Int = 0
      if(!sub.isEmpty){

        key = sub.firstKey()

        resultNode = sub.get(key)

      }else{

        key = map.firstKey()
        resultNode = map.firstEntry.getValue
      }

      DefaultLogger.log(Level.INFO,"ConsistenHash loadbalancer " +
        "choose node ="+resultNode.factory.address.toString+",hash="+key)

      resultNode

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