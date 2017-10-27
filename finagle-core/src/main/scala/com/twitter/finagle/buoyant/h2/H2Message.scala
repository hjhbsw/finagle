package com.twitter.finagle.buoyant.h2

trait H2Header {
  def headers: Headers
}

trait Headers {
  def toSeq: Seq[(String, String)]
  def contains(k: String): Boolean
  def get(k: String): Option[String]
  def getAll(k: String): Seq[String]
  def add(k: String, v: String): Unit
  def set(k: String, v: String): Unit
  def remove(key: String): Seq[String]

  /** Create a deep copy. */
  def dup(): Headers
}