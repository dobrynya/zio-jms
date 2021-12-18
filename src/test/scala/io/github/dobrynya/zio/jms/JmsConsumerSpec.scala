package io.github.dobrynya.zio.jms

import zio.{ Queue => ZQueue, _ }
import stream._
import test._
import TestAspect._
import JmsTestKitAware._
import javax.jms.{ Connection, Message, Session }

object JmsConsumerSpec extends ZIOSpecDefault {
  private val randomMessages = Gen.listOf(Gen.alphaNumericString.filter(s => s != null && s.nonEmpty)).filter(_.nonEmpty)

  override def spec: Spec[TestEnvironment, Throwable] =
    suite("JmsConsumer test suite")(
      test("JmsConsumer should consume messages in order of sending") {
        val q = Queue("test-1")
        check(randomMessages) { messages =>
          for {
            _ <- send(q, messages)
            received <- JmsConsumer
                         .consume(q)
                         .take(messages.size)
                         .collect(onlyText)
                         .runCollect
                         .map(_.toList)
          } yield assertTrue(received == messages)
        }
      },
      test("JmsConsumer should consume message transactionally") {
        val q = Queue("test-2")
        check(randomMessages) { messages =>
          for {
            _ <- send(q, messages)
            notComitted <- JmsConsumer
                            .consumeTx(q)
                            .take(messages.size)
                            .map(_.message)
                            .collect(onlyText)
                            .runCollect
                            .map(_.toList)
            comitted <- JmsConsumer
                         .consumeTx(q)
                         .take(messages.size)
                         .tap(_.commit)
                         .map(_.message)
                         .collect(onlyText)
                         .runCollect
                         .map(_.toList)
          } yield assertTrue(comitted == messages && notComitted == messages)
        }
      },
      test("JmsConsumer should consume all sent messages") {
        val q = Queue("test-3")
        check(randomMessages) { messages =>
          for {
            _        <- send(q, messages)
            queue    <- ZQueue.unbounded[Message]
            consumer <- JmsConsumer.consumeWith(q, m => queue.offer(m)).fork
            received <- queue.takeN(messages.size).map(_.toList.collect(onlyText)) <* consumer.interrupt
          } yield assertTrue(received == messages)

        }
      },
      test("JmsConsumer should consume all sent messages transactionally") {
        check(randomMessages, Gen.fromIterable(1 to 1005000).map(i => s"test-4-$i")) {
          (messages, queueName) =>
            val q = Queue(queueName)
            for {
              _         <- send(q, messages)
              collector <- ZQueue.unbounded[String]
              _ <- JmsConsumer
                    .consumeTxWith(q, message => ZIO.fail(s"Expected: ${onlyText(message)}!"))
                    .ignore
              consumer <- JmsConsumer
                           .consumeTxWith(q, message => collector.offer(onlyText(message)))
                           .fork
              received <- collector.takeN(messages.size).map(_.toList) <* consumer.interrupt
            } yield assertTrue(received == messages)
        }
      },
      test("Consuming a queue should be interruptable") {
        val q = Queue("test-5")
        for {
          collector       <- Ref.make(false)
          consumer        <- JmsConsumer.consumeWith(q, _ => collector.set(true)).fork
          receivedMessage <- consumer.interrupt.delay(1.second) *> collector.get
        } yield assertTrue(!receivedMessage)
      },
      test("Transactional consuming a queue should be interruptable") {
        val q = Queue("test-6")
        for {
          collector       <- Ref.make(false)
          consumer        <- JmsConsumer.consumeTxWith(q, _ => collector.set(true)).fork
          receivedMessage <- consumer.interrupt.delay(1.second) *> collector.get
        } yield assertTrue(!receivedMessage)
      },
      test("Closing a MessageConsumer should interrupt waiting for a message") {
        val q = Queue("test-7")
        ZIO.scoped {
          for {
            c      <- ZIO.service[Connection]
            s      <- session(c, transacted = false, Session.AUTO_ACKNOWLEDGE)
            d      <- q(s)
            mc     <- consumer(s, d)
            flag   <- Ref.make(false)
            _      <- ZIO.attemptBlocking(mc.receive()).flatMap(m => flag.set(m == null)).fork
            _      <- ZIO.attemptBlocking(mc.close()).delay(1.second)
            result <- flag.get.delay(1.second)
          } yield assertTrue(result)
        }
      },
      test("JmsConsumer should reply to the dedicated destination when processing messages") {
        val request = Queue("test-8-request")
        val reply   = Queue("test-8-reply")
        check(randomMessages) {
          messages =>
            for {
              _ <- ZIO.scoped(
                    ZStream.fromIterable(messages).run(JmsProducer.requestSink(request, reply, textMessageEncoder))
                  )
              consumer <- JmsConsumer
                           .consumeAndReplyWith(
                             request,
                             (message, session) =>
                               textMessageEncoder(onlyText(message).toUpperCase, session).map(Some.apply)
                           )
                           .fork
              received <- JmsConsumer.consume(reply).take(messages.size).collect(onlyText).runCollect.map(_.toList) <*
                           consumer.interrupt
            } yield assertTrue(received == messages.map(_.toUpperCase))
        }
      },
      test("JmsConsumer should reply to the dedicated destination when processing messages transactionally") {
        val request = Queue("test-9-request")
        val reply   = Queue("test-9-reply")
        check(randomMessages) {
          messages =>
            for {
              _ <- ZIO.scoped(
                    ZStream.fromIterable(messages).run(JmsProducer.requestSink(request, reply, textMessageEncoder))
                  )
              consumer <- JmsConsumer
                           .consumeAndReplyWith(request,
                                                (message, session) =>
                                                  textMessageEncoder(onlyText(message).toUpperCase, session)
                                                    .map(Some.apply),
                                                transacted = true,
                                                Session.SESSION_TRANSACTED)
                           .fork
              received <- JmsConsumer.consume(reply).collect(onlyText).take(messages.size).runCollect.map(_.toList) <*
                           consumer.interrupt
            } yield assertTrue(received == messages.map(_.toUpperCase))
        }
      }
    ).provideCustomShared(brokerLayer >>> connectionFactoryLayer >>> makeConnection()) @@
      withLiveEnvironment @@ timed @@ timeout(3.minutes) @@ sequential

  def send(destination: DestinationFactory, messages: List[String]): RIO[Connection, Unit] =
    ZIO.scoped(ZStream.fromIterable(messages).run(JmsProducer.sink(destination, textMessageEncoder)))
}
