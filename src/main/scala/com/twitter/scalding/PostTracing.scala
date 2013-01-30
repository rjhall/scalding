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
  
  implicit val bfm = new BloomFilterMonoid(5, 1 << 24, 0)
  var pipe_map : Map[Pipe, Pipe] = Map[Pipe,Pipe]()
  val field : Fields = new Fields("__source_data__")
  var head : List[Pipe] = List[Pipe]()

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
    import Dsl._
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
    import Dsl._
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
    import Dsl._
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
              if(p.getFunction.isInstanceOf[AggregateBy.CompositeFunction]) {
                // These will be replaced with new ones by the AggregateBy.
                recurseUp(prevs.head)
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
            if(p.isAggregator) {
              new Every(recurseUp(prevs.head), p.getArgumentSelector, p.getAggregator)
            } else if(p.isBuffer) {
              new Every(recurseUp(prevs.head), p.getArgumentSelector, p.getBuffer)
            } else {
              throw new java.lang.Exception("unknown pipe: " + p.toString)
            }
          }
          case p : CoGroup => {
            throw new java.lang.Exception("not yet implemented: " + p.toString)
          }
          // TODO: set mapred.reduce.tasks on the groupBy (and the aggregateBy.getGroupBy)
          case p : GroupBy => {
            // This clause should only be reached by a groupBy that does no AggregateBy.
            // Thus we can just aggregate the bloom filters on a reducer.
            val groupfields = p.getKeySelectors.asScala.head._2
            val q = if(p.isSorted) {
              new GroupBy(p.getName, recurseUp(prevs.head), groupfields, p.getSortingSelectors.asScala.head._2, p.isSortReversed)
            } else {
              new GroupBy(p.getName, recurseUp(prevs.head), groupfields)
            }
            // Add on a thing to aggregate. TODO: this groupby might not do anything and this will b0rk it.
            type BM = Map[String,BF]
            val bfsetter = implicitly[TupleSetter[BM]]
            val bfconv = implicitly[TupleConverter[BM]]
            val bfmonoid = implicitly[Monoid[BM]]
            new Every(q, field, new MRMAggregator[BM,BM,BM]({bf => bf}, {(a,b) => bfmonoid.plus(a,b)}, {bf => bf}, field, bfconv, bfsetter))
          }
          case p : AggregateBy => {
            // If a groupBys doing the aggregation we can get on board with that and save time.
            val inp = recurseUp(p.getGroupBy.getPrevious.head)
            val groupfields = p.getGroupBy.getKeySelectors.asScala.head._2
            type BM = Map[String,BF]
            val bfsetter = implicitly[TupleSetter[BM]]
            val bfconv = implicitly[TupleConverter[BM]]
            val bfmonoid = implicitly[Monoid[BM]]
            val mrm = new MRMBy[BM,BM,BM](field, new Fields("__mid_source_data"), field, {bf => bf}, {(a,b) => bfmonoid.plus(a,b)}, {bf => bf}, bfconv, bfsetter, bfconv, bfsetter)
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
  
  def print_flow(tail : Pipe) {
    if(tail.getPrevious.size > 0) {
      print_flow(tail.getPrevious.apply(0))
    }
    println(" -- " + tail.toString)
  }

}
