package io.github.dobrynya.zio.jms

import javax.jms.{ JMSException, Message, MessageConsumer, Session, Connection => JMSConnection }
import zio._
import zio.stream._

class JmsConsumer[T] private[jms] (session: Session,
                                   consumer: MessageConsumer,
                                   semaphore: Semaphore,
                                   timeout: Option[Long] = None) {

  /**
   * Consumes the specified destination and emits received message with this consumer to provide helpful operations.
   * @return a stream of received messages and a session to commit/rollback messages when working transactionally
   */
  def consume(enrich: (Message, JmsConsumer[T]) => T): Stream[JMSException, T] =
    ZStream
      .repeatZIO(
        semaphore.withPermit(
          ZIO.attemptBlocking(timeout.map(consumer.receive).getOrElse(consumer.receive()))
            .tapError(th => ZIO.logWarning(s"An error occurred during receiving a message: ${th.getMessage}!"))
            .refineToOrDie
        )
      )
      .filter(_ != null)
      .map(enrich(_, this))

  def commitSession: IO[JMSException, Unit] = semaphore.withPermit(commit(session))

  def rollbackSession: IO[JMSException, Unit] = semaphore.withPermit(rollback(session))
}

class TxMessage(val message: Message, consumer: JmsConsumer[TxMessage]) {
  def commit: IO[JMSException, Unit]   = consumer.commitSession
  def rollback: IO[JMSException, Unit] = consumer.rollbackSession
}

object JmsConsumer {

  def make[A](destination: DestinationFactory,
              transacted: Boolean,
              acknowledgementMode: Int,
              timeout: Option[Long] = None): ZIO[Scope & JMSConnection, JMSException, JmsConsumer[A]] =
    for {
      connection <- ZIO.service[JMSConnection]
      session    <- session(connection, transacted, acknowledgementMode)
      d          <- destination(session)
      mc         <- consumer(session, d)
      semaphore  <- Semaphore.make(1)
    } yield new JmsConsumer(session, mc, semaphore, timeout)

  def consume(
    destination: DestinationFactory,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE,
    timeout: Option[Long] = Some(300)
  ): ZStream[JMSConnection, JMSException, Message] =
    ZStream
      .scoped[JMSConnection](make[Message](destination, transacted = false, acknowledgementMode, timeout))
      .flatMap(_.consume((m, _) => m))

  def consumeTx(destination: DestinationFactory, timeout: Option[Long] = Some(300)): ZStream[JMSConnection, JMSException, TxMessage] =
    ZStream
      .scoped[JMSConnection](make[TxMessage](destination, transacted = true, Session.SESSION_TRANSACTED, timeout))
      .flatMap(_.consume(new TxMessage(_, _)))

  def consumeAndReplyWith[R, E >: JMSException](
    destination: DestinationFactory,
    responder: (Message, Session) => ZIO[R, E, Option[Message]],
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE,
    timeout: Option[Long] = Some(300)
  ): ZIO[R & JMSConnection, Any, Unit] =
    createPipeline(destination, responder, transacted, acknowledgementMode, timeout)

  private[jms] def createPipeline[R, E](
    destination: DestinationFactory,
    responder: (Message, Session) => ZIO[R, E, Option[Message]],
    transacted: Boolean,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE,
    timeout: Option[Long] = Some(300)
  ): ZIO[R & JMSConnection, Any, Unit] = {
    val consumerAndProducer = for {
      connection <- ZIO.service[JMSConnection]
      session    <- session(connection, transacted, acknowledgementMode)
      d          <- destination(session)
      mc         <- consumer(session, d)
      mp         <- producer(session)
      semaphore  <- Semaphore.make(1)
    } yield (session, mc, mp, semaphore)

    ZStream
      .scoped(consumerAndProducer)
      .flatMap {
        case (session, mc, mp, semaphore) =>
          new JmsConsumer[Message](session, mc, semaphore, timeout)
            .consume((m, _) => m)
            .mapZIO { request =>
              for {
                response <- responder(request, session)
                _ <- response
                      .filter(_ => request.getJMSReplyTo != null)
                      .map { response =>
                        ZIO.attemptBlocking {
                          response.setJMSCorrelationID(request.getJMSCorrelationID)
                          mp.send(request.getJMSReplyTo, response)
                        }.tapError(_ => rollback(session).when(transacted))
                      }
                      .getOrElse(ZIO.unit)
                _ <- acknowledge(request).unless(transacted) *> commit(session).when(transacted)
              } yield ()
            }
      }
      .runDrain
  }

  def consumeWith[R, E >: JMSException](
    destination: DestinationFactory,
    processor: Message => ZIO[R, E, Any],
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE,
    timeout: Option[Long] = Some(300)
  ): ZIO[R with JMSConnection, E, Unit] =
    ZStream
      .scoped[R with JMSConnection](make[Message](destination, transacted = false, acknowledgementMode, timeout))
      .flatMap(_.consume((m, _) => m))
      .foreach(m => processor(m) *> acknowledge(m))

  /**
   * Consumes the specified destination and provides the processor with received message.
   * Automatically commits successfully processed messages or rollbacks a message in case of failure.
   * @param destination consume from
   * @param processor a function to proceed with a message
   * @tparam R specifies dependencies
   * @tparam E specifies possible error type
   * @return unit
   */
  def consumeTxWith[R, E >: JMSException](destination: DestinationFactory,
                                          processor: Message => ZIO[R, E, Any],
                                          timeout: Option[Long] = Some(300)): ZIO[R & JMSConnection, E, Unit] =
    ZStream
      .scoped[R with JMSConnection](
        make[TxMessage](destination, transacted = true, Session.SESSION_TRANSACTED, timeout)
      )
      .flatMap(_.consume(new TxMessage(_, _)))
      .foreach { tm =>
        processor(tm.message).tapBoth(e => ZIO.logDebug(s"Rolling back ${tm.message} because of $e!") *> tm.rollback,
                                      _ => ZIO.logDebug("Committing a message") *> tm.commit)
      }
}
