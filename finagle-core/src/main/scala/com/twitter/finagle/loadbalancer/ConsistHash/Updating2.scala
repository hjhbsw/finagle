package com.twitter.finagle.loadbalancer.ConsistHash

import java.util.logging.Level

import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.{Activity, Future, Time}

import scala.util.control.NonFatal

/**
  * A Balancer mix-in which provides the collection over which to load balance
  * by observing `endpoints`.
  */
trait Updating2[Req, Rep] extends Balancer2[Req, Rep] {

  /**
    * An activity representing the active set of EndpointFactories.
    */
  protected def endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]]

  /*
   * Subscribe to the Activity and dynamically update the load
   * balancer as it (successfully) changes.
   *
   * The observation is terminated when the Balancer is closed.
   */
  private[this] val observation = endpoints.states.respond {
    case Activity.Ok(newList) =>
      // We log here for completeness. Since this happens out-of-band
      // of requests, the failures are not easily exposed.
      try update(newList)
      catch {
        case NonFatal(exc) =>
          DefaultLogger.log(Level.WARNING, "Failed to update balancer", exc)
      }

    case Activity.Failed(exc) =>
      DefaultLogger.log(Level.WARNING, "Activity Failed", exc)

    case Activity.Pending => // nop
  }

  override def close(deadline: Time): Future[Unit] = {
    observation.close(deadline).before { super.close(deadline) }
  }
}