package io.github.dobrynya.zio.jms

import zio.{ IO, ZIO }
import javax.jms._

case class Queue(name: String) extends DestinationFactory {
  override def apply(session: Session): IO[JMSException, Destination] =
    ZIO.attempt(session.createQueue(name)).refineToOrDie[JMSException]
}

case class Topic(name: String) extends DestinationFactory {
  override def apply(session: Session): IO[JMSException, Destination] =
    ZIO.attempt(session.createTopic(name)).refineToOrDie[JMSException]
}

case object TemporaryQueue extends DestinationFactory {
  override def apply(session: Session): IO[JMSException, Destination] =
    ZIO.attempt(session.createTemporaryQueue()).refineToOrDie[JMSException]
}

case object TemporaryTopic extends DestinationFactory {
  override def apply(session: Session): IO[JMSException, Destination] =
    ZIO.attempt(session.createTemporaryQueue()).refineToOrDie[JMSException]
}
