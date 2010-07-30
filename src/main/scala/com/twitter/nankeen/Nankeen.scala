package com.twitter.nankeen

import com.twitter.jackhammer._
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import net.lag.logging._
import net.lag.smile._
import scala.collection.mutable._

class Reader(client: MemcacheClient[String], queueName: String, numMessages: Int, expectedMessages: Set[String]) extends Runnable{

  def run() = {
    while (expectedMessages.size > 0) {
      get()
      Thread.sleep(10)
    }
    client.shutdown()
  }

  def get() = {
    client.get(queueName) match {
      case Some(msg) => {
        expectedMessages.synchronized {
          expectedMessages - msg
        }
      }
      case None => //noop
    }
  }
}

class Writer(client: MemcacheClient[String], queueName: String, numMessages: Int, messages: LinkedBlockingQueue[String]) extends Runnable{

  def run(): Unit = {
    while (true) {
      val msg = messages.poll()
      if (msg == null) {
        client.shutdown()
        return
      }
      put(msg)
    }
  }

  def put(data: String) = {
    client.set(queueName, data)
  }
}

class Loader(servers: Array[String], queueName: String, numWriters: Int, 
             numReaders: Int, numMessages: Int) extends Runnable{

  val distribution = "ketama"
  val hash = "fnv1a-64"
  def client() = {
    val locator = NodeLocator.byName(distribution) match {
      case (hashName, factory) =>
        factory(KeyHasher.byName(hash))
    }
    val client = new MemcacheClient(locator, MemcacheCodec.UTF8)
    val pool = new ServerPool
    val connections = for (s <- servers) yield ServerPool.makeConnection(s, pool)
    pool.servers = connections
    client.setPool(pool)
    client
  }

  val messagesSet = Set((1 to numMessages).map(i => Nankeen.messagePrefix + i):_*)

  val messagesQueue = new LinkedBlockingQueue[String](messagesSet.size)
  messagesSet.foreach(messagesQueue.offer(_))

  val readers = (1 to numReaders).map(i => new Reader(client, queueName, numMessages, messagesSet)).toList
  val writers = (1 to numWriters).map(i => new Writer(client, queueName, numMessages, messagesQueue)).toList

  def run() = {
    val readerThreads = readers.map(r => new Thread(r))
    val writerThreads = writers.map(r => new Thread(r))
    readerThreads.foreach(_.start)
    writerThreads.foreach(_.start)
    readerThreads.foreach(_.join)
    writerThreads.foreach(_.join)
  }
}

object Nankeen extends LoggingLoadTest {
  val messagePrefix = "Nankeen Load Test Message "
  val log = Logger.get("nankeen")
  def main(args: Array[String]) = {
    if (args.length != 6) {
      Console.println("usage: general-loader")
      Console.println("    spin up a number of loaders that each")
      Console.println("    spin up M writers and have them write N messages to a queue")
      Console.println("    for Z loops")
      Console.println("    spin up O writers to drain the queue")
      Console.println("    example: general-loader queue-name 1 10 10 100")
      System.exit(1)
    }

    val queueName = args(0).toString
    var loops = args(1).toInt
    val numQueues = args(2).toInt
    val numReaders = args(3).toInt
    val numWriters = args(4).toInt
    val numMessages = args(5).toInt

    val timingsFile = new File("timings.log")
    log.info("Using %d writers and %d readers to write %d messages to %d queues prefaced by %s".
                    format(numWriters, numReaders, numMessages, numQueues, queueName))
    val queues = (1 to numQueues).toList

    while(loops != 0) {
      loops -= 1
      val loaderThreads = queues.map {i =>
        val loader = new Loader(Array("localhost:22133"), queueName + i, numWriters, numReaders, numMessages)
        new Thread(loader)
      }
      runWithTiming {
        loaderThreads.foreach(_.start)
        loaderThreads.foreach(_.join)
        log.info("finished run")
        dumpLogOutput(timingsFile)
      }
    }
  }  

}
