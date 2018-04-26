package com.supersonic.util

import sun.misc.Signal

object SignalingUtil {
  def registerHandler(block: => Unit): Unit = {
    val _ = Signal.handle(new Signal("INT"), _ => block)
  }
}
