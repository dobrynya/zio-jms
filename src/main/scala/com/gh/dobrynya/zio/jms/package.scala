package com.gh.dobrynya.zio

import javax.jms._
import zio._
import zio.blocking._

package object jms {

  type DestinationFactory = Session => Destination
  type MessageFactory[T]  = (T, Session) => Message
  type BlockingConnection = Blocking with Has[Connection]

  def connection(connectionFactory: ConnectionFactory,
                 credentials: Option[(String, String)] = None): ZManaged[Blocking, JMSException, Connection] =
    Managed.make {
      effectBlockingInterrupt {
        val connection = credentials
          .map(creds => connectionFactory.createConnection(creds._1, creds._2))
          .getOrElse(connectionFactory.createConnection())
        connection.start()
        connection
      }
    }(c => Task(c.close()).ignore).refineToOrDie

  def textMessageEncoder: (String, Session) => IO[JMSException, TextMessage] =
    (text: String, session: Session) => Task(session.createTextMessage(text)).refineToOrDie

  def onlyText: PartialFunction[Message, String] = {
    case text: TextMessage => text.getText
  }

  def acknowledge(message: Message): ZIO[Blocking, JMSException, Message] =
    effectBlockingInterrupt {
      message.acknowledge()
      message
    }.refineToOrDie

  private[jms] def session(connection: Connection,
                           transacted: Boolean,
                           acknowledgeMode: Int): ZManaged[Blocking, JMSException, Session] =
    Managed
      .make(effectBlockingInterrupt(connection.createSession(transacted, acknowledgeMode)))(
        s => Task(s.close()).ignore
      )
      .refineToOrDie

  private[jms] def producer(session: Session): ZManaged[Blocking, JMSException, MessageProducer] =
    Managed.make(effectBlockingInterrupt(session.createProducer(null)).refineToOrDie)(p => UIO(p.close()))

  private[jms] def consumer(session: Session,
                            destination: Destination): ZManaged[Blocking, JMSException, MessageConsumer] =
    Managed.make(effectBlockingInterrupt(session.createConsumer(destination)).refineToOrDie)(c => UIO(c.close()))
}
