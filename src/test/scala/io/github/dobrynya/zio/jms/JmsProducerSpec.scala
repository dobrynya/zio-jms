package io.github.dobrynya.zio.jms

import zio.{ durationInt, Scope, ZIO }
import zio.stream.ZStream
import zio.test._
import TestAspect._
import org.apache.activemq.command.ActiveMQQueue
import JmsTestKitAware._

object JmsProducerSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("JmsProducer test suite")(
      test("requestSink should set JMSReplyTo header") {
        val request = Queue("test-jp-1")
        val reply   = Queue("test-jp-2")
        check(Gen.alphaNumericString) { message =>
          for {
            _ <- ZIO.scoped(
                  ZStream
                    .from(message)
                    .run(JmsProducer.requestSink(request, reply, textMessageEncoder))
                )
            received <- JmsConsumer.consume(request).runHead
          } yield assertTrue(received.map(_.getJMSReplyTo).contains(new ActiveMQQueue(reply.name)))
        }
      },
      test("routerSink should send messages basing on message content") {
        check(Gen.fromIterable(1 to 2)) { queueNumber =>
          for {
            _ <- ZIO.scoped(
                  ZStream
                    .from(queueNumber.toString)
                    .run(
                      JmsProducer.routerSink((m: String, s) => Queue(s"test-jp-2-$m")(s).zip(textMessageEncoder(m, s)))
                    )
                )
            received <- JmsConsumer.consume(Queue(s"test-jp-2-$queueNumber")).collect(onlyText).runHead
          } yield assertTrue(received.contains(s"$queueNumber"))
        }
      }
    ).provideCustomShared(brokerLayer >>> connectionFactoryLayer >>> makeConnection()) @@
      withLiveEnvironment @@ timed @@ timeout(3.minutes) @@ sequential
}
