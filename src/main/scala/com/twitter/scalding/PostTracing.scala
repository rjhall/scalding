package com.twitter.scalding

import scala.collection.JavaConverters._

import cascading.flow.FlowDef
import cascading.operation.Identity
import cascading.pipe.{Each, Every, CoGroup, GroupBy, Pipe}
import cascading.pipe.assembly.AggregateBy
import cascading.tap.Tap
import cascading.tuple.{Fields,Tuple,TupleEntry}

import com.twitter.algebird.{BF, BloomFilterMonoid, Monoid}
import com.twitter.algebird.Operators._

object PostTracing extends Serializable {
  
  var pipe_map : Map[Pipe, Pipe] = Map[Pipe,Pipe]()
  val field : Fields = new Fields("__source_data__")
  var head : List[Pipe] = List[Pipe]()

  // stuff needed to aggregate bloomfilters.
  import Dsl._
  implicit val bfm = new BloomFilterMonoid(5, 1 << 24, 0)
  type BM = Map[String,BF]
  val bmsetter = implicitly[TupleSetter[BM]]
  val bmconv = implicitly[TupleConverter[BM]]
  val bmmonoid = implicitly[Monoid[BM]]

  def reset : Unit = {
    pipe_map = Map[Pipe,Pipe]()
    head = List[Pipe]()
  }

  def apply(flowDef : FlowDef, sm : Map[Source,Source]) : FlowDef = {
    reset
    println("orig flow:")
    print_flow(flowDef.getTails.asScala.head)
    // Convert source map.
    val st = sm.map{ x : (Source, Source) => (x._1.toString, x._2.createTap(Write)) }.toMap[String, Tap[_,_,_]]
    // Build new flow starting with tails.
    val tails = flowDef.getTails.asScala.map{ recurseUp(_) }.toList
    // Add stages to post process the tail contents.
    val tm = tracingTails(tails)
    // Hook up tail pipes to sinks.
    val fd = new FlowDef()
    flowDef.getTails.asScala.foreach{ p : Pipe =>
      val q = RichPipe(pipe_map(p)).discard(field)
      fd.addTail(q)
      fd.addSink(q.getName, flowDef.getSinks.get(q.getName))
      println(q.toString + " -> " + flowDef.getSinks.get(q.getName).toString)
    }
    tm.foreach{ x : (String, Pipe) => 
      val q = new Pipe(x._1 + "filter", x._2)
      fd.addTail(q); fd.addSink(x._1+"filter", st(x._1))
      println("tracing: " + q + " -> " + st(x._1).toString)
    }
    // Connect head taps.
    head.foreach{ p : Pipe => 
      fd.addSource(p.getName, flowDef.getSources.get(p.getName))
      println("head: " + p.toString + " -> " + flowDef.getSources.get(p.getName))
    }
    // Return new flow def.
    println("new flow:")
    print_flow(fd.getTails.asScala.head)
    fd 
  }

  // Build the part of the flow which filters each input file with the built bloomfilters.
  def tracingTails(tails : List[Pipe]) : Map[String,Pipe] = {
    implicit def p2rp(p : Pipe) : RichPipe = RichPipe(p)
    val filters = tracingFilters(tails)
    var m = Map[String,Pipe]()
    head.foreach{ p : Pipe => 
      val s = p.getName; 
      val q = p.map(Fields.ALL -> '__str){ t : TupleEntry => t.getTuple.toString }
               .crossWithTiny(filters(s))
               .filter('__str, field){ x : (String, BF) => x._2.contains(x._1).isTrue }
               .discard('__str, field) 
      m += (s -> q)
    }
    m
  }

  // Join together all the bloom filters from all the tails.
  def tracingFilters(tails : List[Pipe]) : Map[String,Pipe] = {
    implicit def p2rp(p : Pipe) : RichPipe = RichPipe(p)
    var m = Map[String,Pipe]()
    tails.foreach{ p : Pipe => 
      head.map{ _.getName }.foreach{ name : String =>
        val q = p.map(field -> field){ x : Map[String,BF] => if(x.contains(name)) x(name) else bfm.zero }
                .groupAll{ _.plus[BF](field -> field) }
        if(m.contains(name)) {
          val nf = new Fields("__source_2__")
          val j = p.rename(field -> nf).crossWithTiny(m(name)).mapTo(field.append(nf) -> field){ x : (BF, BF) => x._1 ++ x._2 }
          m += (name -> j)
        } else {
          m += (name -> q)
        }
      }
    }
    m
  }

  // Do surgery to instrument the flow for tracing.
  def recurseUp(pipe : Pipe) : Pipe = {
    implicit def p2rp(p : Pipe) : RichPipe = RichPipe(p)
    if(pipe_map.contains(pipe)) {
      println("reached " + pipe.toString + " again")
      pipe_map(pipe)
    } else {
      val prevs = pipe.getPrevious
      if(prevs.length == 0) {
        // Head pipe.
        println("head pipe with name: " + pipe.getName)
        // Reuse the same head pipe, but add a stage which makes the bloom filter..
        val n = RichPipe(pipe).map(Fields.ALL -> field){ x : TupleEntry => Map[String,BF](pipe.getName -> bfm.create(x.getTuple.toString)) }
        head ::= pipe
        pipe_map += (pipe -> n)
        n
      } else {
        val pnew : Pipe = pipe match {
          case p : Each => {
            if(p.isFunction) {
              if(p.getOutputSelector == Fields.SWAP) {
                new Each(recurseUp(prevs.head), p.getArgumentSelector, p.getFunction, p.getOutputSelector)
              } else if(p.getFunction.isInstanceOf[Identity]) {
                val f = p.getArgumentSelector.append(field)
                new Each(recurseUp(prevs.head), f, new Identity(f))
              } else {
                // Just ensure tracing field is preserved.
                val outs = p.getOutputSelector
                val outn = if(outs == Fields.RESULTS || (outs != Fields.ALL && outs != Fields.REPLACE)) p.getFunction.getFieldDeclaration.append(field) else outs
                new Each(recurseUp(prevs.head), p.getArgumentSelector, p.getFunction, outn)
              }
            } else if(p.isFilter) {
              new Each(recurseUp(prevs.head), p.getArgumentSelector, p.getFilter)
            } else {
              throw new java.lang.Exception("unknown pipe: " + p.toString)
            }
          }
          case p : Every => {
            // Everys and groupBys that are scheduled by the AggregateBy are handled in that guys clause. 
            var parent = recurseUp(prevs.head)
            if(parent.isInstanceOf[GroupBy]) {
              // Add on a thing to aggregate.
              parent = new Every(parent, field, new MRMAggregator[BM,BM,BM]({bf => bf}, {(a,b) => bmmonoid.plus(a,b)}, {bf => bf}, field, bmconv, bmsetter))
            }
            if(p.isAggregator) {
              new Every(parent, p.getArgumentSelector, p.getAggregator)
            } else if(p.isBuffer) {
              new Every(parent, p.getArgumentSelector, p.getBuffer)
            } else {
              throw new java.lang.Exception("unknown pipe: " + p.toString)
            }
          }
          case p : CoGroup => {
            // TODO: self joins.
            // Rename the tracing field on one side of the input, perform cogroup, then merge fields.
            if(prevs.size == 2) {
              val renamedfield = new Fields("__source_data_2__")
              val left = recurseUp(prevs.head)
              val right = recurseUp(prevs.tail.head).rename(field -> renamedfield)
              val cg = new CoGroup(left, p.getKeySelectors.get(left.getName), right, p.getKeySelectors.get(right.getName), p.getJoiner) 
              cg.map(field.append(renamedfield) -> field){ x : (BM, BM) => if(x._1 == null) x._2 else if(x._2 == null) x._1 else x._1 + x._2 }
            } else {
              throw new java.lang.Exception("not yet implemented: " + p.toString + " with " + prevs.size + " parents")
            }
          }
          // TODO: set mapred.reduce.tasks on the groupBy (and the aggregateBy.getGroupBy)
          case p : GroupBy => {
            val groupfields = p.getKeySelectors.asScala.head._2
            if(p.isSorted) {
              new GroupBy(p.getName, recurseUp(prevs.head), groupfields, p.getSortingSelectors.asScala.head._2, p.isSortReversed)
            } else {
              new GroupBy(p.getName, recurseUp(prevs.head), groupfields)
            }
          }
          case p : AggregateBy => {
            // If a groupBys doing the aggregation we can get on board with that and save time.
            // Skip the groupby and the each that preceeds it.
            val inp = recurseUp(p.getGroupBy.getPrevious.head.getPrevious.head) // Skip the "Each" it makes.
            val groupfields = p.getGroupBy.getKeySelectors.asScala.head._2
            val mrm = new MRMBy[BM,BM,BM](field, '__mid_source_data, field, 
              {bf => bf}, {(a,b) => bmmonoid.plus(a,b)}, {bf => bf}, bmconv, bmsetter, bmconv, bmsetter)
            val thresholdfield = classOf[AggregateBy].getDeclaredField("threshold") // pwn
            thresholdfield.setAccessible(true)
            new AggregateBy(p.getName, inp, groupfields, thresholdfield.getInt(p), p, mrm)
          }
          case p : Pipe => {
            new Pipe(p.getName, recurseUp(prevs.head))
          }
        }
        pipe_map += (pipe -> pnew)
        pnew     
      }
    }
  }

  def print_flow(i : Int, tail : Pipe) {
    if(tail.getPrevious.size == 1) {
      print_flow(i, tail.getPrevious.apply(0))
    } else {
      (0 until tail.getPrevious.size).foreach{ j : Int =>
        print_flow(2*i + j + 1, tail.getPrevious.apply(j))
      }
    }
    println(i + " -- " + tail.toString)
  }
  
  def print_flow(tail : Pipe) {
    print_flow(0, tail)
  }

}
