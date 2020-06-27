package com.gh.dobrynya.zio.jms

import javax.jms.{Connection, JMSException}
import org.apache.activemq.broker.BrokerService
import org.apache.activemq.ActiveMQConnectionFactory
import zio._
import zio.blocking.Blocking

trait ConnectionAware {
  val connectionFactory = new ActiveMQConnectionFactory("vm://localhost")
  val managedConnection: ZManaged[Blocking, JMSException, Connection] = connection(connectionFactory)

  val brokerService: IO[Any, BrokerService] = Task {
    val brokerService = new BrokerService()
    brokerService.setUseJmx(true)
    brokerService.setPersistent(false)
    brokerService.setUseShutdownHook(true)
    brokerService.start()
    brokerService
  }

  val stopBroker: BrokerService => UIO[Unit] = (brokerService: BrokerService) => UIO(brokerService.stop())
}
