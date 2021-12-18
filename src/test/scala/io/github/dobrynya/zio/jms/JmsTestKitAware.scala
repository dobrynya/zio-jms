package io.github.dobrynya.zio.jms

import jmstestkit.JmsBroker
import zio.{ Scope, ULayer, URIO, URLayer, ZIO, ZLayer }

import javax.jms.ConnectionFactory

object JmsTestKitAware {
  val broker: URIO[Scope, JmsBroker] =
    ZIO.acquireRelease(ZIO.succeed(JmsBroker()))(b => ZIO.succeed(b.stop()))

  val brokerLayer: ULayer[JmsBroker] = ZLayer.scoped(broker)

  val connectionFactory: URIO[JmsBroker, ConnectionFactory] =
    ZIO.serviceWith[JmsBroker](_.createConnectionFactory)

  val connectionFactoryLayer: URLayer[JmsBroker, ConnectionFactory] =
    ZLayer.fromZIO(connectionFactory)
}
