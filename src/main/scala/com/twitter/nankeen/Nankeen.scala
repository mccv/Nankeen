package com.twitter.nankeen

import com.twitter.jackhammer._
import com.twitter.grabbyhands.{Config => GrabbyConfig}
import com.twitter.grabbyhands.GrabbyHands
import com.twitter.grabbyhands.Write
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import net.lag.logging._
import net.lag.smile._
import scala.collection.mutable._
import java.util.concurrent.TimeUnit

trait Reader extends Runnable{
  val log = Logger.get("nankeen")
  val expectedMessages: Set[String]
  def sleepTime = Some(10)
  val queueName: String

  def get(): Unit

  def run(): Unit = {
    try {
      while (expectedMessages.size > 0) {
        get()
      }
    } catch {
      case e => {
        log.error("error reading messages: %s", e)
        e.printStackTrace()
      }
    }
  }
}

class SmileReader(client: MemcacheClient[String], val queueName: String, val expectedMessages: Set[String]) extends Reader{
  def get() = {
    client.get(queueName) match {
      case Some(msg) => {
        expectedMessages.synchronized {
          expectedMessages - msg
        }
      }
      case None => //noop
    }
    sleepTime.foreach(Thread.sleep(_))
  }
}

class GrabbyReader(grabbyHands: GrabbyHands, val queueName: String, val expectedMessages: Set[String]) extends Reader{
  val sleeepTime = None
  def get() = {
    val value = grabbyHands.getRecvQueue(queueName).poll(sleepTime, TimeUnit.SECONDS)
    if ( value != null ) {
      val msg = new String(value.array)
      expectedMessages.synchronized {
        expectedMessages - msg
      }
    }
  }
}

class GrabbyTransactionalReader(grabbyHands: GrabbyHands, val queueName: String, val expectedMessages: Set[String]) extends Reader{
  val sleeepTime = None
  def get() = {
    val value = grabbyHands.getRecvTransQueue(queueName).poll(sleepTime, TimeUnit.SECONDS)
    if ( value != null ) {
      val msg = new String(value.message.array)
      expectedMessages.synchronized {
        expectedMessages - msg
      }
	  value.close
    }
  }
}

trait Writer extends Runnable {
  val log = Logger.get("nankeen")
  val queueName: String
  val messages: LinkedBlockingQueue[String]
  def run(): Unit = {
    try {
      while (true) {
        val msg = messages.poll()
        if (msg == null) {
          return
        }
        put(msg)
      }
    } catch {
      case e => {
        // clear these out so readers stop
        log.error("error writing messages: %s", e)
        e.printStackTrace()
        messages.clear
        return
      }
    }
  }

  def put(data: String)
}

class SmileWriter(client: MemcacheClient[String], val queueName: String, val messages: LinkedBlockingQueue[String]) extends Writer{
  def put(data: String) = {
    client.set(queueName, data)
  }
}
class GrabbyWriter(grabbyHands:GrabbyHands, val queueName: String, val messages: LinkedBlockingQueue[String]) extends Writer{
  def put(data: String) = {
    val write = new Write(data)
    grabbyHands.getSendQueue(queueName).put(write)
  }
}

class Loader(readers: List[Reader], writers: List[Writer]) extends Runnable {
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
    if (args.length != 7 && args.length != 8  && args.length != 9) {
      Console.println("Nankeen")
      Console.println("    spin up a number of loaders that each")
      Console.println("    spin up M writers and have them write N messages to a queue")
      Console.println("    for Z loops")
      Console.println("    spin up O writers to drain the queue")
      Console.println("    (optional true/false) use grabby hands instead of smile for load test.  default is false")
      Console.println("    (optional true/false) use transactional grabby hand reader default is false")
      Console.println("usage:")
      Console.println("    java -jar nankeen-0.1.jar localhost:22133 test 10 1 1 1 1 true")
      System.exit(1)
    }

    val hostName = args(0).toString
    val queueName = args(1).toString
    var loops = args(2).toInt
    val numQueues = args(3).toInt
    val numReaders = args(4).toInt
    val numWriters = args(5).toInt
    val numMessages = args(6).toInt
    var useGrabbyHands = false
    if ( args.length > 7 ) {
      useGrabbyHands = args(7).toBoolean
    }
	var useGrabbyHandsTransRead = false
	if ( args.length > 8 ) {
      useGrabbyHandsTransRead = args(7).toBoolean
    }

    val timingsFile = new File("timings.log")
    log.info("Using %d writers and %d readers to write %d messages to %d queues prefaced by %s".
                    format(numWriters, numReaders, numMessages, numQueues, queueName))
    val queues = (1 to numQueues).toList

    val grabbyHands = {
      if (useGrabbyHands) {
        val grabbyConfig = new GrabbyConfig
        grabbyConfig.addServers(Array(hostName))
        grabbyConfig.addQueues(queues.map {i => queueName + i} )
        grabbyConfig.recvTransactional = useGrabbyHandsTransRead
        Some(new GrabbyHands(grabbyConfig))
      } else {
        None
      }
    }

    // only need to start up a memcache client if we aren't using grabby hands
    val memcacheClient = grabbyHands match  {
      case None => {
        val distribution = "ketama"
        val hash = "fnv1a-64"
        val locator = NodeLocator.byName(distribution) match {
          case (hashName, factory) =>
            factory(KeyHasher.byName(hash))
        }
        val client = new MemcacheClient(locator, MemcacheCodec.UTF8)
        val pool = new ServerPool
        val connections = Array(ServerPool.makeConnection(hostName, pool))
        pool.servers = connections
        client.setPool(pool)
        Some(client)
      }
      case _ => None
    }

    while(loops > 0) {
      log.debug("running loop %d", loops)
      loops -= 1
      val messagesSet = Set((1 to numMessages).map(i => Nankeen.messagePrefix + i):_*)
      val messagesQueue = new LinkedBlockingQueue[String](messagesSet.size)
      messagesSet.foreach(messagesQueue.offer(_))

      val loaderThreads = queues.map (i => {
        val (readers, writers) = grabbyHands match {
          case Some(grabby) => {
            val readers = (1 to numReaders).map(i => if (useGrabbyHandsTransRead) { new GrabbyTransactionalReader(grabby, queueName + i, messagesSet)).toList } else { new GrabbyReader(grabby, queueName + i, messagesSet)).toList}
            val writers = (1 to numWriters).map(i => new GrabbyWriter(grabby, queueName + i, messagesQueue)).toList
            (readers, writers)
          }
          case None => {
            val readers = (1 to numReaders).map(i => new SmileReader(memcacheClient.get, queueName + i, messagesSet)).toList
            val writers = (1 to numWriters).map(i => new SmileWriter(memcacheClient.get, queueName + i, messagesQueue)).toList
            (readers, writers)
          }
        }
        val loader = new Loader(readers, writers)
        new Thread(loader)
      })
      runWithTiming {
        loaderThreads.foreach(_.start)
        loaderThreads.foreach(_.join)
        log.info("finished run")
        dumpLogOutput(timingsFile)
      }
    }
    log.info("finished runs, exiting")
    memcacheClient.foreach(_.shutdown())
    grabbyHands.foreach(_.halt())
  }
}
