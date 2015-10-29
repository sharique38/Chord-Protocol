/**
 * @author Prateek
 */

import scala.util.control.Breaks._
import java.util.concurrent.TimeUnit
import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer
import akka.actor._
import collection.mutable
import java.security.MessageDigest
import scala.util.Random
import akka.routing.BalancingPool
import akka.routing._
import akka.util._
import com.typesafe.config.ConfigFactory
import java.io.File
import akka.pattern.Patterns._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.commons.codec.binary._
import scala.collection.immutable.ListMap
import collection.immutable.SortedMap
case class startjob(numOfRequests: Int)
case class findfingerTableSuccessor(entry: Int, pos: Int, initiator: ActorRef)
case class generateRequests(numOfRequests: Int)
case object fingerTableInit
case class notifySucessorPredecessor(succ: ActorRef, pred: ActorRef)
case object printNeighbours
case class nodeJoin(existingNode: ActorRef)
case class setNeighbours(p: ActorRef, s: ActorRef)
case class updateFingerTable(pos: Int, self: ActorRef)
case class findFileWithHop(initiator: Int, k: Int, count: Int)
case class updateOthers(initiator: ActorRef, pred: ActorRef, succ: ActorRef)
case class increment(count: Int)
//case class updateFingertable(newNode:ActorRef)
case class findSuccessor(k: Int, initiator: ActorRef)
case class fixSuccsessor(nodes: ActorRef)
case class fixPredecessor(nodes: ActorRef)
//case class findFile(k:BigInt)
case class setFingerTable(map: SortedMap[Int, ActorRef])

object project3 extends App {

  override def main(args: Array[String]) {

    println("enter the parameters")
    if (args.length != 2) {
      println("invalid parameters")
      System.exit(1)
    } else {
      val system = ActorSystem("ChordSimulator")

      var Env = system.actorOf(Props(new Chord(args(0).trim.toInt, args(1).trim.toInt)), name = "Chord")
      //println("adadssdad")
      Env ! "initialize"

    }

  }
}

class Nodes(Id: String, numNodes: Int) extends Actor {
  val num = numNodes
  val ID = Id
  val hsahedKey: String = ""
  var fingerTable = SortedMap.empty[Int, ActorRef]
  var pred: ActorRef = null
  var succ: ActorRef = null
  var hashedID = Integer.parseInt(Id, 16)
  var mod = hashedID % math.round(math.pow(2, numNodes))
  var hopCount = 0
  var server: ActorRef = null
  var requestCounter = 0
  var tick: Cancellable = null
  var m = 8 * (((((math.log(numNodes) / math.log(2)) / 8).toFloat).ceil).toInt)
  def hashKey(ID: String): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    val hexString = new StringBuffer();
    //      println("creating SHA hashes"  )
    val stringwithseed = ID
    sha.update(stringwithseed.getBytes("UTF-8"))
    var digest = sha.digest();
    var digestj = java.util.Arrays.copyOf(digest, m / 8)
    //println(" Chord digestj " + Hex.encodeHexString(digestj) + " digestj len= " + digestj.length)
    // println("digest length->"+digest.length)
    //var x:BigInt =BigInt(Hex.encodeHexString(digest),16)
    return (Hex.encodeHexString(digestj))
    // var y:Int= (x % 10).toInt  //parseInt(Hex.encodeHexString(digest),16)
    //return y
  }

  def receive = {
    case generateRequests(numOfRequests) => {

      if (requestCounter == numOfRequests) {
        tick.cancel()
      } else {

        requestCounter += 1

        var file = Integer.parseInt(hashKey("file" + requestCounter + Random.nextInt(10000000)), 16)
        println("Search started at " + Integer.parseInt(self.path.name) + " for " + file)
        self ! findFileWithHop(Integer.parseInt(self.path.name), file, 0)

      }

    }
    case startjob(numOfRequests) => {

      server = sender
      tick = context.system.scheduler.schedule(Duration.create(10, TimeUnit.MILLISECONDS), Duration.create(1000, TimeUnit.MILLISECONDS), self, generateRequests(numOfRequests))

    }
    /*
    case updateFingertable(newNode:ActorRef)=> {
     
      var entry:Int=0
     var  newNodeID=Integer.parseInt(self.path.name)
       for ((key,value) <- fingerTable) {
         entry=((Integer.parseInt(value.path.name)+math.pow(2,key-1))% (math.pow(2,m).toInt)).toInt
         if(entry<=newNodeID && newNodeID<Integer.parseInt(value.path.name)){
           var fingerTableMap=scala.collection.mutable.Map[Int,ActorRef]() ++= fingerTable
           fingerTableMap(key)=newNode
           fingerTable=SortedMap.empty[Int,ActorRef] ++ fingerTableMap
         }

       }
      
    }
*/
    case updateOthers(initiator, p, s) => {
      var successor = Integer.parseInt(s.path.name)
      var predecessor = Integer.parseInt(p.path.name)
      var newNodeId = Integer.parseInt(initiator.path.name)
      println(predecessor + " " + newNodeId + " " + successor)
      if (!initiator.equals(self)) {
        var entry: Int = 0
        var newNodeID = Integer.parseInt(initiator.path.name)
        for ((key, value) <- fingerTable) {
          if (value.equals(s)) {
            entry = (((Integer.parseInt(self.path.name) + math.pow(2, key - 1).toInt)) % (math.pow(2, m).toInt)).toInt
            if(predecessor < successor){
              if (newNodeId < successor) {
                if (entry <= newNodeId) {
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap

                }
              }
            }
            else{//Boundary case
              if(newNodeId>=0 && newNodeId < successor){
                if(entry > predecessor){
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
                }
                else if(entry<=newNodeID){
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
                }
              }
              else{
                if(entry <= newNodeID && entry > predecessor){
                  var fingerTableMap = scala.collection.mutable.Map[Int, ActorRef]() ++= fingerTable
                  fingerTableMap(key) = initiator
                  fingerTable = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
                }
              }
            }

          }
          // entry=(((Integer.parseInt(self.path.name)+math.pow(2,key-1).toInt))% (math.pow(2,m).toInt)).toInt
          // println("entry->"+entry)

        }
        println("updated finger table for " + self + " node->" + fingerTable)
        succ ! updateOthers(initiator, p, s)
      } else {
        println("all finger tables updated")
        server ! "networkReady"
      }
    }

    case updateFingerTable(pos, node) => {

      fingerTable += (pos -> node)
      if (fingerTable.size == m) {
        pred ! fixSuccsessor(self)
        succ ! fixPredecessor(self)

      }

    }
    case fixSuccsessor(nodes) => {
      succ = nodes

    }
    case fixPredecessor(nodes) => {
      var oldPred = pred
      pred = nodes
      self ! updateOthers(nodes, oldPred, self)
    }

    case findFileWithHop(initiator, k, cnt) => {
      var count = cnt

      var flag = false

      if (k <= Integer.parseInt(self.path.name)) {
        if (k == Integer.parseInt(self.path.name)) {
          println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
          flag = true;
          server ! increment(count)
          // count=count+1
        } else if (k > Integer.parseInt(pred.path.name)) {
          println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
          flag = true;
          server ! increment(count)
          // count=count+1
        } else if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name))) {
          println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
          flag = true;
          server ! increment(count)
          // count=count+1
        }
      }

      if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name)) && k > Integer.parseInt(self.path.name) && k > Integer.parseInt(pred.path.name)) {
        println("file request from " + initiator + ": key= " + k + " found in " + self.path.name)
        flag = true;
        server ! increment(count)
      }

      var minIndex = 0
      var min = k - (Integer.parseInt(self.path.name))
      if(min < 0)
        min += (math.pow(2, m).toInt)
      if (!flag) {
        for ((key, value) <- fingerTable) {
          var x = k - (Integer.parseInt(value.path.name)).toInt
          if (x < 0)
            x += (math.pow(2, m).toInt)
          if (x < min) {
            min = x
            minIndex = key
          }
        }

        if (minIndex != 0) {
          flag = true
          count += 1
          fingerTable(minIndex) ! findFileWithHop(initiator, k, count)
        }
      }

      if (!flag) {
        //println((k) + "not found in current node" + self.path.name + "sending query to " + succ)
        count += 1
        succ ! findFileWithHop(initiator, k, count)

      }
    }

    case findSuccessor(k, initiator) => {
      //var count = cnt

      var flag = false

      if (k <= Integer.parseInt(self.path.name)) {
        if (k == Integer.parseInt(self.path.name)) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! notifySucessorPredecessor(self, pred)
          // count=count+1
        } else if (k > Integer.parseInt(pred.path.name)) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! notifySucessorPredecessor(self, pred)
          // count=count+1
        } else if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name))) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! notifySucessorPredecessor(self, pred)
          // count=count+1
        }
      }

      if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name)) && k > Integer.parseInt(self.path.name) && k > Integer.parseInt(pred.path.name)) {
        //println("Successor found at->" + self.path.name)
        flag = true;
        initiator ! notifySucessorPredecessor(self, pred)
      }

      var minIndex = 0
      var min = k - (Integer.parseInt(self.path.name))
      if (!flag) {
        for ((key, value) <- fingerTable) {
          var x = k - (Integer.parseInt(value.path.name)).toInt
          if (x < 0)
            x += (math.pow(2, m).toInt)
          if (x < min) {
            min = x
            minIndex = key
          }
        }

        if (minIndex != 0) {
          flag = true
          //  count+=1
          fingerTable(minIndex) ! findSuccessor(k, initiator)
        }
      }

      if (!flag) {
        ///println((k) + "not found in current node" + self.path.name + "sending query to " + succ)
        // count+=1
        succ ! findSuccessor(k, initiator)

      }
    }

    case `printNeighbours` => {
      //println(pred + "->" + self.path.name + "->" + succ)
    }

    case setNeighbours(p, s) => {
      this.pred = p
      this.succ = s
      //println("Neighbours of " + Integer.parseInt(self.path.name) + " = " + Integer.parseInt(pred.path.name) + ", " + Integer.parseInt(succ.path.name))
    }

    case "fingerTable" => {

    }

    case setFingerTable(fingTable) => {
      fingerTable = fingTable
      //println("fingertable set:" + self.path.name + "fingertable->" + fingerTable)
    }
    case notifySucessorPredecessor(s, p) => {

      succ = s
      pred = p
      //println("successor" + succ + "predecessor" + pred)
      self ! "init_fingerTable"

    }
    case nodeJoin(existingNode) => {
      server = sender
      existingNode ! findSuccessor(Integer.parseInt(self.path.name), self)

    }

    case "init_fingerTable" => {
      var n = Integer.parseInt(self.path.name)
      for (l <- 1 to m) {
        var entry = ((n + math.pow(2, l - 1)) % (math.pow(2, m).toInt)).toInt //sharique wrong
        succ ! findfingerTableSuccessor(entry, l, self)

      }

    }

    case findfingerTableSuccessor(k, pos, initiator) => {
      //var count = cnt

      var flag = false

      if (k <= Integer.parseInt(self.path.name)) {
        if (k == Integer.parseInt(self.path.name)) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! updateFingerTable(pos, self)
          // count=count+1
        } else if (k > Integer.parseInt(pred.path.name)) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! updateFingerTable(pos, self)
          // count=count+1
        } else if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name))) {
          //println("Successor found at->" + self.path.name)
          flag = true;
          initiator ! updateFingerTable(pos, self)
          // count=count+1
        }
      }

      if ((Integer.parseInt(pred.path.name) > Integer.parseInt(self.path.name)) && k > Integer.parseInt(self.path.name) && k > Integer.parseInt(pred.path.name)) {
        //println("Successor found at->" + self.path.name)
        flag = true;
        initiator ! updateFingerTable(pos, self)
      }

      var minIndex = 0
      var min = k - (Integer.parseInt(self.path.name))
      if (!flag) {
        for ((key, value) <- fingerTable) {
          var x = k - (Integer.parseInt(value.path.name)).toInt
          if (x < 0)
            x += (math.pow(2, m).toInt)
          if (x < min) {
            min = x
            minIndex = key
          }
        }

        if (minIndex != 0) {
          flag = true
          //  count+=1
          fingerTable(minIndex) ! findfingerTableSuccessor(k, pos, initiator)
        }
      }

      if (!flag) {
        ///println((k) + "not found in current node" + self.path.name + "sending query to " + succ)
        // count+=1
        succ ! findfingerTableSuccessor(k, pos, initiator)

      }

    }

  }

}

class Chord(totalnodes: Int, NumRequests: Int) extends Actor {
  var hops = 0
  var requestsProcessed = 0
  var numNodes = totalnodes
  var nameStrings = new ArrayBuffer[String]()
  var nodes = new ArrayBuffer[ActorRef]()
  var m = 0
  var indexMap = new mutable.HashMap[Int, Int]()

  def sortByMod(s1: ActorRef, s2: ActorRef) = {
    (Integer.parseInt(s1.path.name)) < (Integer.parseInt(s2.path.name))
  }

  def populateFingertable(nodes: ArrayBuffer[ActorRef], numNodes: Int) = {
    val sortedNodes = nodes.sortWith(sortByMod)
    //sortedNodes

    for (i <- 0 to numNodes - 1) {
      //println("------" + i)
      var fingerTableMap = SortedMap[Int, ActorRef]()
      var k = Integer.parseInt(sortedNodes(i).path.name)
      var n = (k) //% math.pow(2,numNodes).toInt).toInt
      var l = 1
      // n is ith node ; fill its fingertable
      while (l <= m) {

        var flag = true
        // println("n->"+n)
        var entry = ((n + math.pow(2, l - 1)) % (math.pow(2, m).toInt)).toInt //sharique wrong
        var min = 99999
        var min_id = 0

        var count = 0
        var r = 0

        if (indexMap.contains(entry)) {
          r = indexMap(entry)
        } else {
          entry = (entry + 1) % (math.pow(2, m).toInt)
          while (indexMap.contains(entry) != true) {
            entry = (entry + 1) % (math.pow(2, m).toInt)
          }

          r = indexMap(entry)
        }

        fingerTableMap += (l -> sortedNodes(r))

        l = l + 1

      }

      //nodes(i)! fingerTable(fingerTableMap)
      val fingerTableSortedMap = SortedMap.empty[Int, ActorRef] ++ fingerTableMap
      //fingerTableMap= mutable.Map.empty[Int,ActorRef]

      // fingerTableMap ++=  fingerTableSortedMap
      println("fingertable for node " + sortedNodes(i).path.name + " -> " + fingerTableSortedMap)
      sortedNodes(i) ! setFingerTable(fingerTableSortedMap)

    }

  }

  def hashKey(ID: String): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    val hexString = new StringBuffer();
    //      println("creating SHA hashes"  )
    val stringwithseed = ID
    sha.update(stringwithseed.getBytes("UTF-8"))
    var digest = sha.digest();
    var digestj = java.util.Arrays.copyOf(digest, m / 8)
    //println(" Chord digestj " + Hex.encodeHexString(digestj) + " digestj len= " + digestj.length)
    // println("digest length->"+digest.length)
    //var x:BigInt =BigInt(Hex.encodeHexString(digest),16)
    return (Hex.encodeHexString(digestj))
    // var y:Int= (x % 10).toInt  //parseInt(Hex.encodeHexString(digest),16)
    //return y
  }

  def receive = {
    case increment(count: Int) => {
      requestsProcessed += 1
      hops += count
      println("requestsProcessed = " + requestsProcessed)
      if (requestsProcessed >= numNodes * NumRequests) {
        println("Total hops for " + numNodes * NumRequests + " requests are " + hops + ", Average hops = " + (hops / (numNodes * NumRequests).toFloat))
        context.system.shutdown()
      }

    }
    case "initialize" => {
      m = 8 * (((((math.log(numNodes) / math.log(2)) / 8).toFloat).ceil).toInt)
      for (i <- 0 to numNodes - 1) {
        var st: String = hashKey("node " + i)
        var name = Integer.parseInt(st, 16).toString
        nameStrings += name
      }
      nameStrings = nameStrings.distinct
      //numNodes = nameStrings.length

      while (nameStrings.length < numNodes) {
        var st: String = hashKey("node " + Random.nextInt(10000000))
        var name = Integer.parseInt(st, 16).toString
        if (nameStrings.contains(name) == false)
          nameStrings += name
      }
      println("creating nodes m = " + m + " numNodes = " + numNodes)
      for (i <- 0 to numNodes - 1) {
        var st: String = hashKey("node " + i)
        nodes += context.actorOf(Props(new Nodes(st, numNodes)), name = nameStrings(i))
        println(Integer.parseInt(st, 16).toString)
      }

      val sortedNodes = nodes.sortWith(sortByMod)

      nodes = sortedNodes

      for (i <- 0 to numNodes - 1) {
        indexMap += Integer.parseInt(nodes(i).path.name) -> i
      }

      sortedNodes(0) ! setNeighbours(sortedNodes(numNodes - 1), sortedNodes(1))
      for (i <- 1 to numNodes - 2)
        sortedNodes(i) ! setNeighbours(sortedNodes(i - 1), sortedNodes(i + 1))
      sortedNodes(numNodes - 1) ! setNeighbours(sortedNodes(numNodes - 2), sortedNodes(0))

      populateFingertable(nodes, numNodes)

      // self ! fingerTableInit
      for (o <- 0 to numNodes - 1) {
        //  sortedNodes(o) ! startjob(NumRequests)
      }

      //----------------Node Join---------------
      var newNodeName: String = sortedNodes(0).path.name
      var newNode: ActorRef = null
      breakable{
        while (true) {
          var str: String = hashKey("Node " + Random.nextInt(10000000))
          newNodeName = Integer.parseInt(str, 16).toString
          if (nameStrings.contains(newNodeName) == false)
          {
            nameStrings += newNodeName
            newNode = context.actorOf(Props(new Nodes(newNodeName, numNodes)), name = newNodeName)
            break;
          }
        }
      }
      println("node to join->" + newNode)
      newNode ! nodeJoin(sortedNodes(0))

      nodes = sortedNodes
      //----------------------------------------

    }

    case "networkReady" => {
      nodes += sender
      numNodes += 1

      for (o <- 0 to numNodes - 1) {
        nodes(o) ! startjob(NumRequests)
      }
    }

    case `fingerTableInit` => {

    }
  }
}
