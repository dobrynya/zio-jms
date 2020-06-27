ZIO-JMS
===

ZIO-JMS adapts JMS API to ZIO streams and makes it working more conveniently and seamlessly integrates ZIO

---

For receiving messages it needs to create a message consumer using utility methods in JmsConsumer object

```scala
import zio.{ZIO, Has, Chunk}
import zio.blocking._
import javax.jms.{Connection, Message, JMSException}
import com.gh.dobrynya.zio.jms._

val received: ZIO[Has[Connection] with Blocking, JMSException, Chunk[Message]] = 
    JmsConsumer.consume(Queue("test-queue"))
                 .take(5)
                 .collect(onlyText)
                 .runCollect
```

You can process a stream of messages transactionally like follows

```scala
import zio.{ZIO, Has, Chunk, UIO}
import zio.blocking._
import javax.jms.{Connection, Message, JMSException}
import com.gh.dobrynya.zio.jms._

def messageProcessor(message: Message): UIO[Unit] = ??? 

val received: ZIO[Has[Connection] with Blocking, JMSException, Unit] = 
    JmsConsumer.consumeTx(Queue("test-queue"))
                 .take(5)
                 .tap(transactionalMessage => messageProcessor(transactionalMessage.message) <* transactionalMessage.commit)
                 .runDrain
``` 

Another ability to process input messages without using ZIO streams is more concise

```scala
import zio.{ZIO, Has}
import zio.blocking._
import zio.console._
import javax.jms.{Connection, Message, JMSException}
import com.gh.dobrynya.zio.jms._

def someMessageProcessor(message: Message): ZIO[Console, Exception, Unit] = 
    putStrLn(s"Received message $message")

val processing: ZIO[Console with Blocking with Has[Connection]] = 
    JmsConsumer.consumeWith(Topic("test-topic"), someMessageProcessor)
```

In case of possible failures during processing a message I recommend using a transactional consumer which commits a message
when it is processed successfully and rolls back when it fails

```scala
import zio.{ZIO, IO, Has}
import zio.blocking._
import javax.jms.{Connection, Message, JMSException}
import com.gh.dobrynya.zio.jms._

def someMessageProcessor(message: Message): IO[String, Unit] = 
   IO.fail(s"Error occurred during processing a message $message")

val processing: ZIO[Blocking with Has[Connection], Any, Unit] = 
   JmsConsumer.consumeTxWith(Topic("test-topic"), someMessageProcessor)
```

For sending messages it needs to create sinks providing a destination and a message encoder as follows

```scala
import zio.stream.ZStream
import com.gh.dobrynya.zio.jms._

val messages = (1 to 100).map(i => s"Message $i")

ZStream.fromIterable(messages).run(JmsProducer.sink(Queue("test-queue"), textMessageEncoder))
```

The last thing is to provide a connection like follows

```scala
import zio.{ZLayer, Has}
import zio.blocking._
import javax.jms.{ConnectionFactory, Connection, JMSException}
import com.gh.dobrynya.zio.jms.connection

def connectionFactory: ConnectionFactory = ???

val connection: ZLayer[Blocking, JMSException, Blocking with Has[Connection]] = 
    ZLayer.fromManaged(connection(connectionFactory)).passthrough
```