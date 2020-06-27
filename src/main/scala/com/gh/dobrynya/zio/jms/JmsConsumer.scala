package com.gh.dobrynya.zio.jms

import javax.jms.{JMSException, Message, MessageConsumer, Session, Connection => JMSConnection}
import zio._
import zio.blocking._
import zio.stream.ZStream

class JmsConsumer[T](session: Session, consumer: MessageConsumer) {

  /**
   * Consumes the specified destination and emits received message with this consumer to provide helpful operations.
   * @return a stream of received messages and a session to commit/rollback messages when working transactionally
   */
  def consume(enrich: (Message, JmsConsumer[T]) => T): ZStream[Blocking, JMSException, T] =
    ZStream.repeatEffect(effectBlockingInterrupt(enrich(consumer.receive(), this)).refineToOrDie)

  def commit: IO[JMSException, Unit] =
    Task(session.commit()).refineToOrDie

  def rollback: IO[JMSException, Unit] =
    Task(session.rollback()).refineToOrDie
}

class TransactionalMessage(val message: Message, consumer: JmsConsumer[TransactionalMessage]) {
  def commit: IO[JMSException, Unit]   = consumer.commit
  def rollback: IO[JMSException, Unit] = consumer.rollback
}

object JmsConsumer {

  def consume(destination: DestinationFactory,
              acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE): ZStream[BlockingConnection, JMSException, Message] =
    ZStream
      .managed(make[Message](destination, transacted = false, acknowledgementMode))
      .flatMap(_.consume((m, _) => m))

  def make[A](destination: DestinationFactory,
              transacted: Boolean,
              acknowledgementMode: Int): ZManaged[BlockingConnection, JMSException, JmsConsumer[A]] =
    for {
      connection <- ZIO.service[JMSConnection].toManaged_
      session    <- session(connection, transacted, acknowledgementMode)
      mc         <- consumer(session, destination(session))
    } yield new JmsConsumer(session, mc)

  def consumeTx(destination: DestinationFactory): ZStream[BlockingConnection, JMSException, TransactionalMessage] =
    ZStream
      .managed(make[TransactionalMessage](destination, transacted = true, Session.SESSION_TRANSACTED))
      .flatMap(_.consume(new TransactionalMessage(_, _)))

  def consumeWith[R, E >: JMSException](
    destination: DestinationFactory,
    processor: Message => ZIO[R, E, Any],
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZIO[R with BlockingConnection, E, Unit] =
    make[Message](destination, transacted = false, acknowledgementMode)
      .use(_.consume((m, _) => m).foreach(processor).unit)

  /**
   * Consumes the specified destination and provides the processor with received message.
   * Automatically commits successfully processed messages or rollbacks a message in case of failure.
   * @param destination consume from
   * @param processor a function to proceed with a message
   * @tparam R specifies dependencies
   * @tparam E specifies possible error type
   * @return unit
   */
  def consumeTxWith[R, E >: JMSException](
    destination: DestinationFactory,
    processor: Message => ZIO[R, E, Any],
  ): ZIO[R with BlockingConnection, E, Unit] =
    make[TransactionalMessage](destination, transacted = true, Session.AUTO_ACKNOWLEDGE)
      .use(
        _.consume(new TransactionalMessage(_, _))
          .foreach { tm =>
            processor(tm.message)
              .tapBoth(_ => tm.rollback, _ => tm.commit)
          }
          .unit
      )
}
