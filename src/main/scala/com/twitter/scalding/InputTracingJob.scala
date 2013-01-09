package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe

class InputTracingJob(args : Args) extends Job(args) {

  // Initialize input tracing.
  Tracing.init(args)

  // Allow the job to use the optionaSubsample method on a pipe.
  val use_subsample = args.boolean("use_sources")
  
  class SubsamplingPipe(pipe : Pipe) extends RichPipe(pipe) {
    def optionalSubsample(p : Double) : Pipe = {
      if(use_subsample)
        pipe
      else
        subsample(p)
    }
  }

  implicit def p2ssp(pipe : Pipe) : SubsamplingPipe = new SubsamplingPipe(pipe)
  implicit def ssp2p(ssp : SubsamplingPipe) : Pipe = ssp.pipe
  // In case the first operation is subsampling.
  implicit def s2ssp(src : Source) : SubsamplingPipe = new SubsamplingPipe(src.read)

  // Convenience method to constructed a traced source.
  def TracingFileSource(o : FileSource, s : String) : TracingFileSource = {
    new TracingFileSource(o, s, args)
  }
  
  // Finish the flow by calling onFlowcomplete.
  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    Tracing.tracing.onFlowComplete.foreach{ x : (Source, Pipe) => x._1.writeFrom(x._2)(flowDef, mode) }
    // Sources are good, now connect the flow:
    mode.newFlowConnector(config).connect(flowDef)
  }
}
