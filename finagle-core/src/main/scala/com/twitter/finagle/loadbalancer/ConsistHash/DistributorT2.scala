package com.twitter.finagle.loadbalancer.ConsistHash

import com.twitter.finagle.ClientConnection

/**
  * The base type of the load balancer distributor. Distributors are
  * updated nondestructively, but, as with nodes, may share some
  * data across updates.
  *
  * @param vector the vector of nodes over which the balancer is balancing.
  */
abstract class DistributorT2[Node](val vector: Vector[Node]) {

  type This <: DistributorT2[Node]

  /**
    * Pick the next node.
    *
    * This is the main entry point for a load balancer implementation.
    */
  def pick(conn:ClientConnection): Node

  /**
    * True if this distributor needs to be rebuilt. (For example, it
    * may need to be updated with current availabilities.)
    */
  def needsRebuild: Boolean

  /**
    * Rebuild this distributor.
    */
  def rebuild(): This

  /**
    * Rebuild this distributor with a new vector.
    */
  def rebuild(vector: Vector[Node]): This
}

