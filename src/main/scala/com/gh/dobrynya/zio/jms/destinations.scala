package com.gh.dobrynya.zio.jms

import javax.jms._

case class Queue(name: String) extends DestinationFactory {
  override def apply(session: Session): Destination = session.createQueue(name)
}

case class Topic(name: String) extends DestinationFactory {
  override def apply(session: Session): Destination = session.createTopic(name)
}

case object TemporaryQueue extends DestinationFactory {
  override def apply(session: Session): Destination = session.createTemporaryQueue()
}

case object TemporaryTopic extends DestinationFactory {
  override def apply(session: Session): Destination = session.createTemporaryQueue()
}
