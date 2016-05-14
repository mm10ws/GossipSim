import akka.actor._
import akka.routing.RoundRobinRouter
import akka.util._
import java.math.BigInteger
import java.util.Base64
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.net._
import java.io._
import scala.io._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math._

sealed trait Message
case class boot() extends Message
case class setNeighbors(newNeighbors: ArrayBuffer[ActorRef]) extends Message
case class gossipMessage(boss: ActorRef) extends Message
case class retransmit(boss: ActorRef) extends Message
case class stop() extends Message
case class end() extends Message
case class pushMessage(boss: ActorRef, s: Double, w: Double) extends Message
case class setS(s: Double) extends Message
case class superStop(finalValue: Double) extends Message
case class heartBeat(stopCount: Int) extends Message

class GossipActor extends Actor {
  var count = 0
  var quit = false
  var first = true
  var neighbor = new ArrayBuffer[ActorRef]()
  var s: Double = 0
  var w: Double = 1
  var currentValue: Double = 0
  var previousValue: Double = 0
  var doneCount = 0

  def receive = {
    case setNeighbors(newNeighbors) =>
      neighbor ++= newNeighbors

    case setS(newS) =>
      s = newS

    case pushMessage(boss, newS, newW) =>
      s = (s + newS) / 2
      w = (w + newW) / 2
      println("s/w = " + s / w)
      previousValue = currentValue
      currentValue = s / w
      if (abs(currentValue - previousValue) < 1e-10) {
        doneCount = doneCount + 1
      } else {
        doneCount = 0
      }

      if (doneCount < 3) {
        neighbor(Random.nextInt(neighbor.size)) ! pushMessage(boss, s, w)
      } else {
        boss ! superStop(currentValue)
      }

    //context.system.scheduler.scheduleOnce(500 milliseconds, neighbor(Random.nextInt(neighbor.size)), pushMessage(boss,s,w))

    case gossipMessage(boss) =>
      if (count >= 100) {
        //do nothing
        if (!quit) {
          println(self.path.name + " quit after getting 10 messages")
          boss ! stop()
          quit = true
        }

      } else {
        count = count + 1
        //println(self.path.name + " got message")

        if (first) {
          neighbor(Random.nextInt(neighbor.size)) ! gossipMessage(boss)
          //println(self.path.name + " got first message")
          context.system.scheduler.scheduleOnce(10 milliseconds, self, retransmit(boss))
          first = false;
        }

      }
    case retransmit(boss) =>
      if (quit) {
        //do nothing

        //boss ! stop
      } else {
        neighbor(Random.nextInt(neighbor.size)) ! gossipMessage(boss)
        //println(self.path.name + " sent message")
        context.system.scheduler.scheduleOnce(10 milliseconds, self, retransmit(boss))
      }

    case end() =>
      context.stop(self)

  }

}

class Master(numNodes: Int, topo: String, algo: String) extends Actor {
  val masterActorList = new ArrayBuffer[ActorRef]()
  var stopCount = 0
  var startTime: Long = 0
  var stopTime: Long = 0

  for (i <- 0 until numNodes) {
    masterActorList += context.actorOf(Props(new GossipActor()), name = "GossipActor" + i)
  }

  for (i <- 0 until numNodes) {
    masterActorList(i) ! setS(i)
  }

  topo match {
    case "full" =>
      for (i <- 0 until numNodes) {
        masterActorList(i) ! setNeighbors(masterActorList - masterActorList(i))
      }
    case "line" =>
      for (i <- 0 until numNodes) {
        if (i == 0) {
          var tempList = new ArrayBuffer[ActorRef]()
          tempList += masterActorList(i + 1)
          masterActorList(i) ! setNeighbors(tempList)
        } else if (i == numNodes - 1) {
          var tempList = new ArrayBuffer[ActorRef]()
          tempList += masterActorList(i - 1)
          masterActorList(i) ! setNeighbors(tempList)

        } else {
          var tempList = new ArrayBuffer[ActorRef]()
          tempList += masterActorList(i - 1)
          tempList += masterActorList(i + 1)
          masterActorList(i) ! setNeighbors(tempList)
        }

      }
    case "3D" =>
      //var temp = new ArrayBuffer[Int]()
      for (i <- 0 until numNodes) {
        var temp = gridNeighbors(i, cbrt(numNodes).toInt)
        var tempList = new ArrayBuffer[ActorRef]()
        for (j <- 0 until temp.size) {
          tempList += masterActorList(temp(j))
        }
        masterActorList(i) ! setNeighbors(tempList)
      }

    case "imp3D" =>

      for (i <- 0 until numNodes) {
        var temp = gridNeighbors(i, cbrt(numNodes).toInt)
        var randomOne = 0
        do {
          randomOne = Random.nextInt(numNodes)
        } while (randomOne == i || temp.contains(randomOne))

        temp += randomOne
        var tempList = new ArrayBuffer[ActorRef]()
        for (j <- 0 until temp.size) {
          tempList += masterActorList(temp(j))
        }
        masterActorList(i) ! setNeighbors(tempList)
      }

    case _ =>

      println("incorrect topology chosen")
      for (i <- 0 until numNodes) {
        masterActorList(i) ! end()
      }
      context.stop(self)
      context.system.shutdown()

  }

  def gridNeighbors(nodeNumber: Int, b: Int): ArrayBuffer[Int] = {
    if (nodeNumber < 0 || nodeNumber > (Math.pow(b, 3)).toInt - 1) {
      return null
    }

    for (i <- 0 until b) {
      var topLeftCorner = (i * Math.pow(b, 2)).toInt;
      var topRightCorner = (i * Math.pow(b, 2) + b - 1).toInt;
      var bottomLeftCorner = ((i + 1) * (Math.pow(b, 2)) - b).toInt;
      var bottomRightCorner = ((i + 1) * (Math.pow(b, 2)) - 1).toInt;
      var leftSide = new ArrayBuffer[Int]()
      var rightSide = new ArrayBuffer[Int]()

      for (j <- 1 until b - 1) {
        leftSide += (topLeftCorner + b * j)
        rightSide += (topRightCorner + b * j)
      }

      if (nodeNumber == topLeftCorner) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber + 1)
        temp += (nodeNumber + b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (nodeNumber == topRightCorner) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber - 1)
        temp += (nodeNumber + b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (nodeNumber < topRightCorner && nodeNumber > topLeftCorner) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber - 1)
        temp += (nodeNumber + 1)
        temp += (nodeNumber + b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (nodeNumber == bottomLeftCorner) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber + 1)
        temp += (nodeNumber - b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (nodeNumber == bottomRightCorner) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber - 1)
        temp += (nodeNumber - b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (nodeNumber < bottomRightCorner && nodeNumber > bottomLeftCorner) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber - 1)
        temp += (nodeNumber + 1)
        temp += (nodeNumber - b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (leftSide.contains(nodeNumber)) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber + 1)
        temp += (nodeNumber - b)
        temp += (nodeNumber + b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else if (rightSide.contains(nodeNumber)) {
        var temp = new ArrayBuffer[Int]()
        temp += (nodeNumber - 1)
        temp += (nodeNumber - b)
        temp += (nodeNumber + b)
        if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
          temp += nodeNumber + Math.pow(b, 2).toInt
        }
        if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
          temp += nodeNumber - Math.pow(b, 2).toInt
        }
        return temp;
      } else {
        //continue
      }
    }

    var temp = new ArrayBuffer[Int]()
    temp += (nodeNumber - 1)
    temp += (nodeNumber + 1)
    temp += (nodeNumber - b)
    temp += (nodeNumber + b)
    if (nodeNumber + Math.pow(b, 2).toInt < Math.pow(b, 3).toInt) {
      temp += nodeNumber + Math.pow(b, 2).toInt
    }
    if (nodeNumber - Math.pow(b, 2).toInt >= 0) {
      temp += nodeNumber - Math.pow(b, 2).toInt
    }
    return temp;

  }

  def receive = {
    case boot() =>
      if (algo.equals("gossip")) {
        startTime = System.currentTimeMillis
        masterActorList(Random.nextInt(numNodes)) ! gossipMessage(self)
        //self ! heartBeat(stopCount)
        context.system.scheduler.scheduleOnce(5000 milliseconds, self, heartBeat(stopCount))
      } else if (algo.equals("push-sum")) {
        startTime = System.currentTimeMillis
        masterActorList(Random.nextInt(numNodes)) ! pushMessage(self, 0, 0)
      } else {
        println("incorrect choice of algorithm")
        for (i <- 0 until numNodes) {
          masterActorList(i) ! end()
        }
        context.stop(self)
        context.system.shutdown()
      }

    //start gossip
    case stop() =>
      //println("#" + stopCount)
      if (stopCount == numNodes - 3) {
        stopTime = System.currentTimeMillis
        for (i <- 0 until numNodes) {
          masterActorList(i) ! end()
        }

        println("time(ms):" + (stopTime - startTime))
        context.stop(self)
        context.system.shutdown()
      } else {
        stopCount = stopCount + 1

      }

    case superStop(value) =>
      stopTime = System.currentTimeMillis
      for (i <- 0 until numNodes) {
        masterActorList(i) ! end()
      }

      println("Avg: " + value)
      println("time(ms):" + (stopTime - startTime))

      context.stop(self)
      context.system.shutdown()

    case heartBeat(value) =>
      if (stopCount > value) {
        //continue
        context.system.scheduler.scheduleOnce(5000 milliseconds, self, heartBeat(stopCount))
      } else {
        //timeout
        if (stopCount != 0) {
          
          for (i <- 0 until numNodes) {
            masterActorList(i) ! end()
          }
          stopTime = System.currentTimeMillis
          println("timed out")
          println("time(ms):" + (stopTime - startTime - 5000)) //adjusted for wasted time
          context.stop(self)
          context.system.shutdown()
        }
        else{
          context.system.scheduler.scheduleOnce(5000 milliseconds, self, heartBeat(stopCount))
        }
      }

  }

}

object project2 {

  def main(args: Array[String]) {
    try {
      var numNodes = args(0).toInt
      var topology = args(1)
      var algo = args(2)

      if (topology.equals("3D") || topology.equals("imp3D")) {
        var adjustValue = pow(ceil(cbrt(numNodes)), 3).toInt
        numNodes = adjustValue
        //println(adjustValue)

      }
      val system = ActorSystem("GossipSystem")
      val master = system.actorOf(Props(new Master(numNodes, topology, algo)), name = "master")
      //do job
      master ! boot()

    } catch {
      case _ =>
        println("Usage: project2 numNodes topology algorithm")

    }

  }

}