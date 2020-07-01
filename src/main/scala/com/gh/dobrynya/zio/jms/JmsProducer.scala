package com.gh.dobrynya.zio.jms

import javax.jms.{ Destination, JMSException, Message, Session, Connection => JMSConnection }
import zio._
import zio.stream.ZSink

class JmsProducer[R, E >: JMSException, A](session: Session, sender: A => ZIO[R, E, Message]) {
  def produce(message: A): ZIO[R, E, (A, Message)] = sender(message).map(message -> _)
  def commit: IO[JMSException, Unit]               = Task(session.commit()).refineToOrDie
  def rollback: IO[JMSException, Unit]             = Task(session.rollback()).refineToOrDie
}

object JmsProducer {

  /**
   * Creates a sink for sending messages. It commits a transaction automatically in case of a transactional session.
   * @param destination specifies destination
   * @param encoder creates a JMS message from a provided message
   * @param transacted specifies whether to use a transaction
   * @param acknowledgementMode specifies acknowledgement mode of a session
   * @tparam R dependencies
   * @tparam E errors
   * @tparam A message type
   * @return a newly created sink
   */
  def sink[R, E >: JMSException, A](
    destination: DestinationFactory,
    encoder: (A, Session) => ZIO[R, E, Message],
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZSink[R with BlockingConnection, E, A, A, Unit] =
    ZSink.managed[R with BlockingConnection, E, A, JmsProducer[R, E, A], A, Unit](
      make(destination, encoder, transacted, acknowledgementMode)
    ) { jmsProducer =>
      ZSink.foreach(message => jmsProducer.produce(message) <* jmsProducer.commit.when(transacted))
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
      d          = destination(session)
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
      ZSink.foreach(message => jmsProducer.produce(message) <* jmsProducer.commit.when(transacted))
    }

  /**
   * Creates a sink for implementing Request - Reply integration pattern.
   * It sends messages to the specified destination and enriches JMSReplyTo header with provided response destination.
   * You need to specify JMSCorrelationID manually if required by provided encoder.
   * @param destination specifies destination
   * @param replyTo specifies response destination
   * @param encoder converts a message to an appropriate JMS message
   * @param transacted specifies session transactionality
   * @param acknowledgementMode specifies session acknowledgement mode
   * @tparam R specifies dependencies
   * @tparam E specifies possible errors
   * @tparam A message type
   * @return a new sink
   */
  def requestSink[R, E >: JMSException, A](
    destination: DestinationFactory,
    replyTo: DestinationFactory,
    encoder: (A, Session) => ZIO[R, E, Message],
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZSink[R with BlockingConnection, E, A, A, Unit] =
    ZSink.managed[R with BlockingConnection, E, A, JmsProducer[R, E, A], A, Unit](
      for {
        connection  <- ZIO.service[JMSConnection].toManaged_
        session     <- session(connection, transacted, acknowledgementMode)
        mp          <- producer(session)
        d           = destination(session)
        replyHeader = replyTo(session)
      } yield
        new JmsProducer[R, E, A](session,
          message =>
            encoder(message, session).map { encoded =>
              encoded.setJMSReplyTo(replyHeader)
              mp.send(d, encoded)
              encoded
            })
    ) { jmsProducer =>
      ZSink.foreach(message => jmsProducer.produce(message) <* jmsProducer.commit.when(transacted))
    }
}
