package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.{Fields,Tuple,TupleEntry}

import com.twitter.algebird.Operators._

object Tracing {
  implicit var tracing : Tracing = new NullTracing()
  
  def init(args : Args) : Unit = {
    if(args.boolean("write_sources"))
      tracing = new InputTracing(args.getOrElse("tracing_field", "__source_data__"))
  }

  def clear : Unit = {
    tracing = new NullTracing()
  }
}

abstract class Tracing {
  // Called after Source.read by TracingFileSource
  def afterRead(src : Source, pipe : Pipe) : Pipe

  // Called by RichPipe.write
  def onWrite(pipe : Pipe) : Pipe

  // Called by JoinAlgorithms
  def beforeJoin(pipe : Pipe, side : Boolean) : Pipe
  def afterJoin(pipe : Pipe) : Pipe

  // Called by RichPipe.groupBy
  def onGroupBy(groupbuilder : GroupBuilder) : GroupBuilder

  // Called by SourceTracingJob.buildFlow
  def onFlowComplete(implicit flowDef : FlowDef, mode : Mode) : Unit

  // The fields which get tracked (so that RichPipe doesnt nuke these fields
  // in  e.g., mapTo and project)
  def tracingFields : Option[Fields]
}

// This class does no tracing.
class NullTracing extends Tracing {
  override def afterRead(src : Source, pipe : Pipe) : Pipe = pipe
  override def onWrite(pipe : Pipe) : Pipe = pipe
  override def beforeJoin(pipe : Pipe, side : Boolean) : Pipe = pipe
  override def afterJoin(pipe : Pipe) : Pipe = pipe
  override def onGroupBy(groupbuilder : GroupBuilder) : GroupBuilder = groupbuilder
  override def onFlowComplete(implicit flowDef : FlowDef, mode : Mode) : Unit = {}
  override def tracingFields : Option[Fields] = None
}

// This class traces input records throughout the computation by placing
// the source file tuple contents into a special field, and tracing this through
// the computation.
class InputTracing(val fieldName : String) extends Tracing {
  import Dsl._

  val field = new Fields(fieldName)

  override def tracingFields : Option[Fields] = Some(field)

  protected var sources = Set[TracingFileSource]()
  protected var tailpipes = Map[String, Pipe]()
  
  def register(src : TracingFileSource) : Unit = {
    sources += src
  }

  override def afterRead(src : Source, pipe : Pipe) : Pipe = {
    src match {
      case tf : TracingFileSource => {
        register(tf)
        val fp = tf.toString
        pipe.map(tf.hdfsScheme.getSourceFields -> field){ te : TupleEntry => Map(fp -> List[Tuple](te.getTuple)) }
      }
      case _ => {
        pipe
      }
    }
  }

  override def onWrite(pipe : Pipe) : Pipe = {
    // Nuke the implicit tracing object to turn off tracing for this step.
    Tracing.tracing = new NullTracing()
    sources.foreach { ts : TracingFileSource =>
      val n = ts.toString
      val p = pipe.flatMapTo(fieldName -> ts.hdfsScheme.getSourceFields){ m : Map[String, List[Tuple]] => m.getOrElse(n, List[Tuple]()) }
      if(tailpipes.contains(n))
        tailpipes += (n -> (RichPipe(p) ++ tailpipes(n)))
      else
        tailpipes += (n -> p)
    }
    // Resume tracing
    Tracing.tracing = this
    pipe
  }

  override def beforeJoin(pipe : Pipe, side : Boolean) : Pipe = {
    if(side)
      pipe.rename(field -> new Fields(fieldName+"_"))
    else
      pipe
  }

  override def afterJoin(pipe : Pipe) : Pipe = {
    pipe.map((fieldName, fieldName+"_") -> fieldName){ m : (Map[String,List[Tuple]], Map[String,List[Tuple]]) => m._1 + m._2}
  }

  override def onGroupBy(groupbuilder : GroupBuilder) : GroupBuilder = {
    groupbuilder.plus[Map[String,List[Tuple]]](field -> field)
  }

  override def onFlowComplete(implicit flowDef : FlowDef, mode : Mode) : Unit = {
    // Nuke the implicit tracing object to turn off tracing for this step.
    Tracing.tracing = new NullTracing()
    sources.foreach { ts : TracingFileSource => 
      val n = ts.toString
      if(tailpipes.contains(n)) {
        ts.subset.writeFrom(RichPipe(tailpipes(n)).unique(ts.hdfsScheme.getSourceFields))(flowDef, mode)
      }
    }
  }
}


