package com.gh.dobrynya.zio.jms

import javax.jms.{Destination, JMSException, Message, Session, Connection => JMSConnection}
import zio._
import zio.stream.ZSink

class JmsProducer[R, E >: JMSException, A](session: Session, sender: A => ZIO[R, E, Message]) {
  def produce(message: A): ZIO[R, E, (A, Message)] = sender(message).map(message -> _)
  def commit: IO[JMSException, Unit]               = Task(session.commit()).refineToOrDie
  def rollback: IO[JMSException, Unit]             = Task(session.rollback()).refineToOrDie
}

object JmsProducer {
  def sink[R, E >: JMSException, A](
    destination: DestinationFactory,
    encoder: (A, Session) => ZIO[R, E, Message],
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZSink[R with BlockingConnection, E, A, A, Unit] =
    ZSink.managed[R with BlockingConnection, E, A, JmsProducer[R, E, A], A, Unit](
      make(destination, encoder, transacted, acknowledgementMode)
    ) { jmsProducer =>
      ZSink.foreach(jmsProducer.produce)
    }

  def make[R, E >: JMSException, A](
    destination: DestinationFactory,
    encoder: (A, Session) => ZIO[R, E, Message],
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZManaged[R with BlockingConnection, JMSException, JmsProducer[R, E, A]] =
    for {
      connection <- ZIO.service[JMSConnection].toManaged_
      session    <- session(connection, transacted, acknowledgementMode)
      d          <- Task(destination(session)).refineToOrDie.toManaged_
      mp         <- producer(session)
    } yield
      new JmsProducer[R, E, A](session,
                               message =>
                                 encoder(message, session).map { encoded =>
                                   mp.send(d, encoded)
                                   encoded
                               })

  def routerSink[R, E >: JMSException, A](
    encoderAndRouter: (A, Session) => ZIO[R, E, (Destination, Message)],
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZSink[R with BlockingConnection, E, A, A, Unit] =
    ZSink.managed[R with BlockingConnection, E, A, JmsProducer[R, E, A], A, Unit](
      for {
        connection <- ZIO.service[JMSConnection].toManaged_
        session    <- session(connection, transacted, acknowledgementMode)
        mp         <- producer(session)
      } yield
        new JmsProducer[R, E, A](session,
                                 message =>
                                   encoderAndRouter(message, session).map {
                                     case (d, encoded) =>
                                       mp.send(d, encoded)
                                       encoded
                                 })
    ) { jmsProducer =>
      ZSink.foreach(jmsProducer.produce)
    }
}
