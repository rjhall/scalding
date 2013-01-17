package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.{Fields, Tuple, TupleEntry}

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

class TracingFileSource(val original : FileSource, subsetfp : String, args : Args) extends FileSource {

  val use_sources = args.boolean("use_sources")

  val subset = new SequenceFile(subsetfp)

  // To allow for use in a map, as in the testing code.
  override def equals(a : Any) = {
    a.isInstanceOf[TracingFileSource] && 
      a.asInstanceOf[TracingFileSource].original == original &&
      a.asInstanceOf[TracingFileSource].subset == subset
  }
  
  override def hashCode = original.hashCode

  override def localScheme : LocalScheme = {
    if(use_sources) {
      val v = subset.localScheme
      v.setSourceFields(original.localScheme.getSourceFields)
      v
    } else {
      original.localScheme
    }
  }

  override def hdfsScheme : Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_] = {
    if(use_sources) {
      val v = subset.hdfsScheme
      v.setSourceFields(original.hdfsScheme.getSourceFields)
      v
    } else {
      original.hdfsScheme
    }
  }

  override def hdfsPaths : Iterable[String] = {
    if(use_sources) {
      subset.hdfsPaths
    } else {
      original.hdfsPaths
    }
  }

  override def localPath : String = {
    if(use_sources) {
      subset.localPath
    } else {
      original.localPath
    }
  }

  override def writeFrom(pipe : Pipe)(implicit flowDef : FlowDef, mode : Mode) = {
    throw new RuntimeException("Source: (" + toString + ") doesn't support writing")
    pipe
  }

  override def read(implicit flowDef : FlowDef, mode : Mode, tracing : Tracing) : Pipe = {
    tracing.afterRead(this, super.read)
  }

  override def toString : String = {
    "ORIGINAL: " + original.toString + " SUBSET: " + subset.toString
  }
}

