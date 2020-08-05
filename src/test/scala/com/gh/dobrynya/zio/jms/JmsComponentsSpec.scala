package com.gh.dobrynya.zio.jms

import javax.jms.{Queue => _, _}
import zio.{Queue => ZQueue, _}
import zio.blocking.Blocking
import zio.duration._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object JmsComponentsSpec extends DefaultRunnableSpec with ConnectionAware {
  val connectionLayer: ZLayer[Blocking, JMSException, Blocking with Has[Connection]] =
    ZLayer.fromManaged(managedConnection).passthrough

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("JMS components")(
      testM("Messages should be sent and received in order of sending") {
        checkM(Gen.listOf(Gen.anyString)) { messages =>
          val received = jmsObjects("JmsSpec-1").use {
            case (s, p, mc, d) =>
              ZIO.foreach(messages)(send(p, s, d, _)) *> ZIO.foreach((1 to messages.size).toList)(_ => receiveText(mc))
          }
          assertM(received)(equalTo(messages))
        }
      },
      testM("Transactional producer should send messages transactionally") {
        checkM(Gen.anyString) {
          message =>
            for {
              received <- jmsObjects("JmsSpec-2", transacted = true, Session.SESSION_TRANSACTED).use {
                case (s, p, _, d) =>
                  send(p, s, d, message) *> Task(s.rollback()) *> send(p, s, d, message) *> Task(s.commit())
              } *>
                jmsObjects("JmsSpec-2").use {
                  case (_, _, mc, _) =>
                    ZIO.collectAll(List(receiveText(mc, Some(100)), receiveText(mc, Some(100))))
                }
            } yield assert(received)(equalTo(List(message, null)))
        }
      },
      testM("Transactional consumer should rollback session and get the same message twice") {
        checkM(Gen.anyString) { message =>
          for {
            received <- jmsObjects("JmsSpec-3").use {
              case (s, p, _, d) => send(p, s, d, message)
            } *>
              ZIO.foreach(1 to 2) { i =>
                jmsObjects("JmsSpec-3", transacted = true, Session.SESSION_TRANSACTED).use {
                  case (s, _, mc, _) => receiveText(mc) <* Task(if (i == 1) s.rollback() else s.commit())
                }
              }
          } yield assert(received.toList)(equalTo(List(message, message)))
        }
      },
      testM("Acknowledging consumer should get the same message twice without acknowledgement") {
        checkM(Gen.anyString) {
          message =>
            for {
              received <- jmsObjects("JmsSpec-4").use {
                case (s, p, _, d) => send(p, s, d, message)
              } *>
                ZIO.foreach(List(1, 2)) { i =>
                  jmsObjects("JmsSpec-4", transacted = false, Session.CLIENT_ACKNOWLEDGE).use {
                    case (s, _, mc, _) =>
                      receive(mc)
                        .tap(m => Task(if (i == 2) m.acknowledge()))
                        .map(onlyText orElse nullableMessage)
                  }
                }
            } yield assert(received)(equalTo(List(message, message)))
        }
      },
      testM("All sent messages should be received with auto acknowledgements") {
        val dest = Queue("JmsComponentsSpec-1")

        checkM(Gen.listOf(Gen.alphaNumericString)) { messages =>
          (for {
            _ <- send(messages, dest)
            received <- JmsConsumer
                         .consume(dest)
                         .take(messages.size)
                         .collect(onlyText)
                         .runCollect
          } yield assert(received.toList)(equalTo(messages)))
            .provideCustomLayer(connectionLayer)
        }
      },
      testM("Not committed messages should arrive twice") {
        val dest = Queue("JmsComponentsSpec-2")

        checkM(Gen.listOf(Gen.alphaNumericString)) { messages =>
          (for {
            _ <- send(messages, dest)
            received <- ZIO.foreach((1 to 2).toList)(
                         i =>
                           JmsConsumer
                             .consumeTx(dest)
                             .take(messages.size)
                             .tap(_.commit.when(i == 2))
                             .map(_.message)
                             .collect(onlyText)
                             .runCollect
                       )
          } yield assert(received.flatten)(equalTo(messages ++ messages)))
            .provideCustomLayer(connectionLayer)
        }
      },
      testM("Not acknowledged messages should arrive twice") {
        val dest = Queue("JmsComponentsSpec-3")

        checkM(Gen.listOf(Gen.alphaNumericString)) { messages =>
          (for {
            _ <- send(messages, dest)
            received <- ZIO.foreach(List(1, 2))(
                         i =>
                           JmsConsumer
                             .consume(dest, Session.CLIENT_ACKNOWLEDGE)
                             .take(messages.size)
                             .tap(acknowledge(_).when(i == 2))
                             .collect(onlyText)
                             .runCollect
                       )
          } yield assert(received.flatten)(equalTo(messages ++ messages)))
            .provideCustomLayer(connectionLayer)
        }
      },
      testM("Processor should collect all sent messages") {
        val dest = Queue("JmsComponentsSpec-4")

        checkM(Gen.listOf(Gen.alphaNumericString)) { messages =>
          (for {
            _         <- send(messages, dest)
            collector <- ZQueue.unbounded[String]
            _         <- JmsConsumer.consumeWith(dest, message => collector.offer(onlyText(message))).fork
            received  <- ZStream.fromQueue(collector).take(messages.size).runCollect
          } yield assert(received.toList)(equalTo(messages)))
            .provideCustomLayer(connectionLayer)
        }
      },
      testM("Failing processor should not lose any message") {
        val dest = Queue("JmsComponentsSpec-5")

        checkM(Gen.listOf(Gen.alphaNumericString).filter(_.nonEmpty)) {
          messages =>
            (
              for {
                _         <- send(messages, dest)
                collector <- ZQueue.unbounded[String]
                _ <- JmsConsumer
                      .consumeTxWith(dest, message => ZIO.fail(s"Some processing error: ${onlyText(message)}!"))
                      .ignore
                _ <- JmsConsumer
                      .consumeTxWith(dest, message => collector.offer(onlyText(message)))
                      .fork
                received <- ZStream.fromQueue(collector).take(messages.size).runCollect.map(_.toList)
              } yield assert(received)(equalTo(messages))
            ).provideCustomLayer(connectionLayer)
        }
      },
      testM("Client requires a response to be sent to a dedicated queue via JMSReplyTo header") {
        val requestDestination = Queue("JmsComponentsSpec-6")
        val replyDestination   = Queue("JmsComponentsSpec-7")

        checkM(Gen.listOf(Gen.alphaNumericString).filter(_.nonEmpty)) {
          messages =>
            (for {
              _ <- ZStream
                    .fromIterable(messages)
                    .run(JmsProducer.requestSink(requestDestination, replyDestination, textMessageEncoder))
              _ <- JmsConsumer
                    .consumeAndReplyWith(requestDestination,
                                         (message, session) =>
                                           textMessageEncoder(onlyText(message).toUpperCase, session).map(Some.apply))
                    .fork
              received <- JmsConsumer.consume(replyDestination).take(messages.size).collect(onlyText).runCollect
            } yield assert(received.toList)(equalTo(messages.map(_.toUpperCase))))
              .provideCustomLayer(connectionLayer)
        }
      },
      testM("JmsProducer should commit messages automatically in case of a transactional session") {
        checkM(Gen.listOf(Gen.alphaNumericString)) {
          messages =>
            val destination = Queue("JmsComponentsSpec-8")

            (for {
              _ <- ZStream
                    .fromIterable(messages)
                    .run(
                      JmsProducer.sink(destination, textMessageEncoder, transacted = true, Session.SESSION_TRANSACTED)
                    )
              received <- JmsConsumer.consume(destination).collect(onlyText).take(messages.size).runCollect
            } yield assert(received.toList)(equalTo(messages)))
              .provideCustomLayer(connectionLayer)
        }
      }
    ) @@ timeout(3.minute) @@ timed @@ sequential @@ around(brokerService)(stopBroker)

  private def send(messages: List[String], destination: DestinationFactory) =
    ZStream
      .fromIterable(messages)
      .run(JmsProducer.sink(destination, textMessageEncoder))

  private def receive(consumer: MessageConsumer, timeout: Option[Long] = None): ZIO[Any, JMSException, Message] =
    Task(timeout.map(consumer.receive).getOrElse(consumer.receive())).refineToOrDie

  private def receiveText(consumer: MessageConsumer, timeout: Option[Long] = None): ZIO[Any, JMSException, String] =
    receive(consumer, timeout).map(onlyText orElse nullableMessage)

  private def send(p: MessageProducer, s: Session, d: Destination, message: String) =
    Task(p.send(d, s.createTextMessage(message)))

  private def jmsObjects(
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

  private def nullableMessage[T <: AnyRef]: PartialFunction[Message, T] = {
    case null => null.asInstanceOf[T]
  }
}
