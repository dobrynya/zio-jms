package com.gh.dobrynya.zio.jms

import javax.jms.{Queue => _, _}
import zio.{Queue => _, _}
import zio.blocking.Blocking
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test.environment._

object JmsSpec extends DefaultRunnableSpec with ConnectionAware {
  def spec: ZSpec[TestEnvironment, Any] =
    suite("JMS")(
      testM("Messages should be sent and received in order of sending") {
        checkM(Gen.listOf(Gen.anyString)) { messages =>
          val received = jmsObjects("JmsSpec-1").use {
            case (s, p, mc, d) =>
              ZIO.foreach(messages)(send(p, s, d, _)) *> ZIO.foreach(1 to messages.size)(_ => receiveText(mc))
          }
          assertM(received)(equalTo(messages))
        }
      },
      testM("Transactional producer should send messages transactionally") {
        checkM(Gen.anyString) {
          message =>
            val received: ZIO[Blocking, Throwable, List[String]] =
              jmsObjects("JmsSpec-2", transacted = true, Session.SESSION_TRANSACTED).use {
                case (s, p, _, d) =>
                  send(p, s, d, message) *> Task(s.rollback()) *> send(p, s, d, message) *> Task(s.commit())
              } *>
                jmsObjects("JmsSpec-2").use {
                  case (_, _, mc, _) =>
                    ZIO.collectAll(List(receiveText(mc, Some(100)), receiveText(mc, Some(100))))
                }
            assertM(received)(equalTo(List(message, null)))
        }
      },
      testM("Transactional consumer should rollback session and get the same message twice") {
        checkM(Gen.anyString) { message =>
          val received: ZIO[Blocking, Throwable, List[String]] =
            jmsObjects("JmsSpec-3").use {
              case (s, p, _, d) => send(p, s, d, message)
            } *>
              ZIO.foreach(1 to 2) { i =>
                jmsObjects("JmsSpec-3", transacted = true, Session.SESSION_TRANSACTED).use {
                  case (s, _, mc, _) => receiveText(mc) <* Task(if (i == 1) s.rollback() else s.commit())
                }
              }
          assertM(received)(equalTo(List(message, message)))
        }
      },
      testM("Acknowledging consumer should get the same message twice without acknowledgement") {
        checkM(Gen.anyString) { message =>
          val received: ZIO[Blocking, Throwable, List[String]] =
            jmsObjects("JmsSpec-4").use {
              case (s, p, _, d) => send(p, s, d, message)
            } *>
              ZIO.foreach(1 to 2) { i =>
                jmsObjects("JmsSpec-4", transacted = false, Session.CLIENT_ACKNOWLEDGE).use {
                  case (s, _, mc, _) =>
                    receive(mc)
                      .tap(m => Task(if (i == 2) m.acknowledge()))
                      .map(onlyText orElse nullableMessage)
                }
              }
          assertM(received)(equalTo(List(message, message)))
        }
      }
    ) @@ around(brokerService)(stopBroker) @@ sequential @@ timeout(1.minute) @@ timed

  def receive(consumer: MessageConsumer, timeout: Option[Long] = None): ZIO[Any, JMSException, Message] =
    Task(timeout.map(consumer.receive).getOrElse(consumer.receive())).refineToOrDie

  def receiveText(consumer: MessageConsumer, timeout: Option[Long] = None): ZIO[Any, JMSException, String] =
    receive(consumer, timeout).map(onlyText orElse nullableMessage)

  private def send(p: MessageProducer, s: Session, d: Destination, message: String) =
    Task(p.send(d, s.createTextMessage(message)))

  def jmsObjects(
    dest: String,
    transacted: Boolean = false,
    acknowledgementMode: Int = Session.AUTO_ACKNOWLEDGE
  ): ZManaged[Blocking, Throwable, (Session, MessageProducer, MessageConsumer, Destination)] =
    for {
      c  <- managedConnection
      s  <- session(c, transacted, acknowledgementMode)
      d  = Queue(dest)(s)
      p  <- producer(s)
      mc <- consumer(s, d)
    } yield (s, p, mc, d)

  def nullableMessage[T <: AnyRef]: PartialFunction[Message, T] = {
    case null => null.asInstanceOf[T]
  }
}
