ZIO-JMS
===

ZIO-JMS adapts JMS API to ZIO streams and makes it working more conveniently and seamlessly integrates ZIO

---
# Add library to your project
* Add Maven Central Repository to your build script
* Add dependency on the library "io.github.dobrynya" %% "zio-jms" % "0.2". 
zio-jms is available for Scala 2 & 3

# Receive messages

For receiving messages it needs to create a message consumer using utility methods in JmsConsumer object

```scala
import zio.{ZIO, Chunk}
import javax.jms.{Connection, Message, JMSException}
import io.github.dobrynya.zio.jms._

val received: ZIO[Connection, JMSException, Chunk[Message]] = 
    JmsConsumer.consume(Queue("test-queue"))
                 .take(5)
                 .collect(onlyText)
                 .runCollect
```

You can process a stream of messages transactionally like follows

```scala
import zio.{ZIO, Chunk, UIO}
import javax.jms.{Connection, Message, JMSException}
import io.github.dobrynya.zio.jms._

def messageProcessor(message: Message): UIO[Unit] = ??? 

val received: ZIO[Connection, JMSException, Unit] = 
    JmsConsumer.consumeTx(Queue("test-queue"))
                 .take(5)
                 .tap(transactionalMessage => messageProcessor(transactionalMessage.message) <* transactionalMessage.commit)
                 .runDrain
``` 

Another ability to process input messages without using ZIO streams is more concise. A message is being acknowledged after 
successful processing

```scala
import zio._

import javax.jms.{Connection, JMSException, Message}
import io.github.dobrynya.zio.jms._
import zio.Console.printLine

def someMessageProcessor(message: Message): ZIO[Console, Exception, Unit] = 
    printLine(s"Received message $message")

val processing: ZIO[Console with Connection, Exception, Unit] = 
    JmsConsumer.consumeWith(Topic("test-topic"), someMessageProcessor)
```

In case of possible failures during processing a message I recommend using a transactional consumer which commits a message
when it is processed successfully and rolls back when it fails

```scala
import zio.{ZIO, IO}
import javax.jms.{Connection, Message, JMSException}
import io.github.dobrynya.zio.jms._

def someMessageProcessor(message: Message): IO[String, Unit] = 
   IO.fail(s"Error occurred during processing a message $message")

val processing: ZIO[Connection, Any, Unit] = 
   JmsConsumer.consumeTxWith(Topic("test-topic"), someMessageProcessor)
```

# Send messages

For sending messages it needs to create sinks providing a destination and a message encoder as follows

```scala
import zio.stream.ZStream
import io.github.dobrynya.zio.jms._

val messages = (1 to 100).map(i => s"Message $i")

ZStream.fromIterable(messages).run(JmsProducer.sink(Queue("test-queue"), textMessageEncoder))
```

# Connection

The last thing is to provide a connection like follows

```scala
import zio._
import javax.jms.{Connection, ConnectionFactory, JMSException}
import io.github.dobrynya.zio.jms._

def connectionFactory: ConnectionFactory = ???

val connectionLayer: Layer[JMSException, Connection] =
  ZLayer.fromManaged(connection(connectionFactory))

val consuming = JmsConsumer
  .consume(Queue("test-queue"))
  .runDrain
  .provideSomeLayer(connectionLayer)
```

# Request - Reply integration pattern

## From a client's perspective

```scala
import io.github.dobrynya.zio.jms._
import zio.stream._

val request = Queue("request")
val response = Queue("response")

val messages: List[String] = ??? 

ZStream.fromIterable(messages)
    .run(JmsProducer.requestSink(request, response, textMessageEncoder))
```

## From a server's perspective

```scala
import io.github.dobrynya.zio.jms._
import zio.stream._

val request = Queue("request")
JmsConsumer.consumeAndReplyWith(request,
  (message, session) => textMessageEncoder(onlyText(message).toUpperCase, session).asSome)
```

Here is a responder handling input messages and optionally responding to them to a destination specified in 
`JMSReplyTo` header. It automatically copy `JMSCorellationID` header before sending.    
