package io.github.dobrynya.zio

import javax.jms._
import zio._

package object jms {
  type DestinationFactory = Session => IO[JMSException, Destination]

  def connection(connectionFactory: ConnectionFactory,
                 clientId: Option[String] = None,
                 credentials: Option[(String, String)] = None): ZIO[Scope, JMSException, Connection] =
    ZIO.acquireRelease(ZIO.attemptBlocking {
      val connection = credentials
        .map(creds => connectionFactory.createConnection(creds._1, creds._2))
        .getOrElse(connectionFactory.createConnection())
      clientId.foreach(connection.setClientID)
      connection.start()
      connection
    })(c => ZIO.succeedBlocking(c.close())).refineToOrDie

  def makeConnection(clientId: Option[String] = None,
                     credentials: Option[(String, String)] = None): ZLayer[ConnectionFactory, JMSException, Connection] =
    ZLayer.scoped(ZIO.service[ConnectionFactory].flatMap(connection(_, clientId, credentials)))

  def textMessageEncoder: (String, Session) => IO[JMSException, TextMessage] =
    (text: String, session: Session) => ZIO.attempt(session.createTextMessage(text)).refineToOrDie

  def onlyText: PartialFunction[Message, String] = {
    case text: TextMessage => text.getText
  }

  def acknowledge(message: Message): IO[JMSException, Unit] =
    ZIO.attemptBlocking(message.acknowledge()).refineToOrDie

  private[jms] def session(connection: Connection,
                           transacted: Boolean,
                           acknowledgeMode: Int): ZIO[Scope, JMSException, Session] =
    ZIO.acquireRelease(ZIO.attemptBlocking(connection.createSession(transacted, acknowledgeMode)).refineToOrDie)(s =>
      ZIO.succeedBlocking(s.close()))

  private[jms] def producer(session: Session): ZIO[Scope, JMSException, MessageProducer] = {
    val acquire: IO[JMSException, MessageProducer] =
      ZIO.attemptBlocking(session.createProducer(null)).refineToOrDie
    ZIO.acquireRelease(acquire)(p => ZIO.succeedBlocking(p.close()))
  }

  private[jms] def consumer(session: Session, destination: Destination): ZIO[Scope, JMSException, MessageConsumer] = {
    def acquire: IO[JMSException, MessageConsumer] =
      ZIO.attemptBlocking(session.createConsumer(destination)).refineToOrDie[JMSException]
    ZIO.acquireRelease(acquire)(c => ZIO.succeedBlocking(c.close()))
  }

  private[jms] def commit(session: Session): IO[JMSException, Unit] =
    ZIO.attemptBlocking(session.commit()).refineToOrDie

  private[jms] def rollback(session: Session): IO[JMSException, Unit] =
    ZIO.attemptBlocking(session.rollback()).refineToOrDie
}
