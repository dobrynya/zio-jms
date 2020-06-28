package com.gh.dobrynya.zio.jms

import javax.jms.{ Queue => _, _ }
import zio.{ Queue => ZQueue, _ }
import zio.blocking.Blocking
import zio.console._
import zio.duration._
import zio.random._
import zio.stream._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object JmsComponentsSpec extends DefaultRunnableSpec with ConnectionAware {
  val connectionLayer: ZLayer[Blocking, JMSException, Blocking with Has[Connection]] =
    ZLayer.fromManaged(managedConnection).passthrough

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("JMS components")(
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
            .provideSomeLayer[Blocking with Console](connectionLayer)
        }
      },
      testM("Not committed messages should arrive twice") {
        val dest = Queue("JmsComponentsSpec-2")

        checkM(Gen.listOf(Gen.alphaNumericString)) { messages =>
          (for {
            _ <- send(messages, dest)
            received <- ZIO.foreach(1 to 2)(
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
            .provideSomeLayer[Blocking with Console](connectionLayer)
        }
      },
      testM("Not acknowledged messages should arrive twice") {
        val dest = Queue("JmsComponentsSpec-3")

        checkM(Gen.listOf(Gen.alphaNumericString)) { messages =>
          (for {
            _ <- send(messages, dest)
            received <- ZIO.foreach(1 to 2)(
                         i =>
                           JmsConsumer
                             .consume(dest, Session.CLIENT_ACKNOWLEDGE)
                             .take(messages.size)
                             .tap(acknowledge(_).when(i == 2))
                             .collect(onlyText)
                             .runCollect
                       )
          } yield assert(received.flatten)(equalTo(messages ++ messages)))
            .provideSomeLayer[Blocking with Console](connectionLayer)
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
            .provideSomeLayer[Console with Blocking](connectionLayer)
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
            ).provideSomeLayer[Blocking with Console with Random](connectionLayer)
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
              .provideSomeLayer[Console with Blocking](connectionLayer)
        }
      }
    ) @@ timeout(3.minute) @@ timed @@ sequential @@ around(brokerService)(stopBroker)

  private def send(messages: List[String], destination: DestinationFactory) =
    ZStream
      .fromIterable(messages)
      .run(JmsProducer.sink(destination, textMessageEncoder))
}
