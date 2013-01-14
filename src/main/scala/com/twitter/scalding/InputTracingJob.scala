package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe

class InputTracingJob(args : Args) extends Job(args) {

  // Initialize input tracing.
  Tracing.init(args)

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
