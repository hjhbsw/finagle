package com.twitter.finagle.http

trait MessageHeader {

  def headerMap: HeaderMap
}
