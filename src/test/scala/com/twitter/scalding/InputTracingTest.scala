package com.twitter.scalding

import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import org.specs._
import java.lang.{Integer => JInt}

class InputTracingMapJob(args : Args) extends InputTracingJob(args) {
  TracingFileSource(Tsv("input", ('x,'y)), "subsample")
    .mapTo(('x, 'y) -> 'z){ x : (Int, Int) => x._1 + x._2 }
    .write(Tsv("output"))
}

class InputTracingMapTest extends Specification with TupleConversions {
  import Dsl._
  "Running with --write_sources" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.InputTracingMapJob")
        .arg("write_sources", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "subsample", Args("asdf")), List(("0","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((1)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("subsample")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((0,1)) must be_==(true)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
  "Running with --write_sources -bf" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.InputTracingMapJob")
        .arg("write_sources", "true")
        .arg("bf", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "subsample", Args("asdf")), List(("0","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((1)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("subsample")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_>=(3)
          unordered((0,1)) must be_==(true)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}

class BloomFilterJob(args : Args) extends InputTracingJob(args) {
  TracingFileSource(Tsv("input", ('x, 'y, 'z)), "subsample")
    .filter('x){ x : Int => x == 1 }
    .write(Tsv("output"))
}

class BloomFilterTest extends Specification with TupleConversions {
  import Dsl._
  "Running with --write_sources" should {
    //Set up the job:
    "correctly track sources" in {
      var inp : Seq[(Int, Int, Int)] = for(i <- 0 until 10000) yield (i%10, (i / 10) % 100, i / 1000)
      JobTest("com.twitter.scalding.BloomFilterJob")
        .arg("write_sources", "true")
        .arg("bf", "true")
        .source(new TracingFileSource(Tsv("input", ('x, 'y, 'z)), "subsample", Args("asdf")), inp)
        .sink[(Int,Int,Int)](Tsv("output")) { outBuf => 
          val unordered = outBuf.toSet
          println("output gave: " + unordered.size)
        }
        .sink[(Int,Int,Int)](SequenceFile("subsample")) { outBuf => 
          val unordered = outBuf.toSet
          println("bf gave: " + unordered.size)
          unordered.size must be_>=(1000)
        }
        .runHadoop
        .finish
    }
  }
}

class UseInputTracingTest extends Specification with TupleConversions {
  import Dsl._
  "Running with --use_sources" should {
    //Set up the job:
    "correctly use provided sources" in {
      JobTest("com.twitter.scalding.InputTracingMapJob")
        .arg("use_sources", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "subsample", Args("asdf")), List(("1","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((2)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class InputTracingJoinJob(args : Args) extends InputTracingJob(args) {
  TracingFileSource(Tsv("input", ('x,'y)), "sample/input")
    .joinWithSmaller('x -> 'x, TracingFileSource(Tsv("input2", ('x, 'z)), "sample/input2").read)
    .project('x, 'y, 'z)
    .write(Tsv("output"))
}

class InputTracingJoinTest extends Specification with TupleConversions {
  import Dsl._
  "Source tracing join" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.InputTracingJoinJob")
        .arg("write_sources", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "sample/input", Args("asdf")), List(("0","1"), ("1","3"), ("2","9"), ("10", "0")))
        .source(new TracingFileSource(Tsv("input2", ('x, 'z)), "sample/input2", Args("asdf")), List(("5","1"), ("1","4"), ("2","7")))
        .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3,4)) must be_==(true)
          unordered((2,9,7)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("sample/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("sample/input2")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,4)) must be_==(true)
          unordered((2,7)) must be_==(true)
        }
        .runHadoop
        .finish
    }
    "correctly track sources with --bf" in {
      JobTest("com.twitter.scalding.InputTracingJoinJob")
        .arg("bf", "true")
        .arg("write_sources", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "sample/input", Args("asdf")), List(("0","1"), ("1","3"), ("2","9"), ("10", "0")))
        .source(new TracingFileSource(Tsv("input2", ('x, 'z)), "sample/input2", Args("asdf")), List(("5","1"), ("1","4"), ("2","7")))
        .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3,4)) must be_==(true)
          unordered((2,9,7)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("sample/input")) { outBuf => 
          val unordered = outBuf.toSet
          println("bf output " + unordered.size + " with 2 needed")
          unordered.size must be_>=(2)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("sample/input2")) { outBuf => 
          val unordered = outBuf.toSet
          println("bf output " + unordered.size + " with 2 needed")
          unordered.size must be_>=(2)
          unordered((1,4)) must be_==(true)
          unordered((2,7)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class InputTracingGroupByJob(args : Args) extends InputTracingJob(args) {
  TracingFileSource(Tsv("input", ('x,'y)), "foo/input").groupBy('x){ _.sum('y -> 'y) }
    .filter('x) { x : Int => x < 2 }
    .map('y -> 'y){ y : Double => y.toInt }
    .write(Tsv("output"))
}

class InputTracingGroupByTest extends Specification with TupleConversions {
  import Dsl._
  "Source tracing groupby" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.InputTracingGroupByJob")
        .arg("write_sources", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "foo/input", Args("asdf")), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((0,4)) must be_==(true)
          unordered((1,10)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
    "correctly track sources with --bf" in {
      JobTest("com.twitter.scalding.InputTracingGroupByJob")
        .arg("write_sources", "true")
        .arg("bf", "true")
        .source(new TracingFileSource(Tsv("input", ('x,'y)), "foo/input", Args("asdf")), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((0,4)) must be_==(true)
          unordered((1,10)) must be_==(true)
        }
        .sink[(Int,Int)](SequenceFile("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          println("bf output " + unordered.size + " with 4 needed")
          unordered.size must be_>=(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class PostInputTracingMapJob(args : Args) extends Job(args) {
  val inp = Tsv("input", ('x,'y))
  val sm = Map(inp.asInstanceOf[Source] -> Tsv("subsample").asInstanceOf[Source])
  inp.mapTo(('x, 'y) -> 'z){ x : (Int, Int) => x._1 + x._2 }
     .write(Tsv("output"))
  
  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = PostTracing(flowDef, sm)
    mode.newFlowConnector(config).connect(fd)
  }
}

class PostInputTracingMapTest extends Specification with TupleConversions {
  import Dsl._
  "This fucking thing" should {
    //Set up the job:
    "not fuck up" in {
      JobTest("com.twitter.scalding.PostInputTracingMapJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((1)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("subsample")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((0,1)) must be_==(true)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class PostInputTracingGroupByJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  source.groupBy('x){ _.sum('y -> 'y) }
    .filter('x) { x : Int => x < 2 }
    .map('y -> 'y){ y : Double => y.toInt }
    .project('x, 'y)
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = PostTracing(flowDef, Map[Source,Source](source -> Tsv("foo/input")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class PostInputTracingGroupByTest extends Specification with TupleConversions {
  import Dsl._
  "Source tracing groupby" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.PostInputTracingGroupByJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((0,4)) must be_==(true)
          unordered((1,10)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class PostInputTracingGroupByNopJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  source.groupBy('x){ _.reducers(1) }
    .filter('x) { x : Int => x < 2 }
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = PostTracing(flowDef, Map[Source,Source](source -> Tsv("foo/input")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class PostInputTracingGroupByNopTest extends Specification with TupleConversions {
  import Dsl._
  "Source tracing groupby" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.PostInputTracingGroupByNopJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}

class PostInputTracingGroupByFoldJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  source.groupBy('x){ _.foldLeft[Double,Int]('y -> 'y)(0.0){ (a : Double, b : Int) => a + b } }
    .filter('x) { x : Int => x < 2 }
    .map('y -> 'y){ y : Double => y.toInt }
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = PostTracing(flowDef, Map[Source,Source](source -> Tsv("foo/input")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class PostInputTracingGroupByFoldTest extends Specification with TupleConversions {
  import Dsl._
  "Source tracing groupby" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.PostInputTracingGroupByFoldJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((0,4)) must be_==(true)
          unordered((1,10)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}

class PostInputTracingJoinJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  val source2 = Tsv("input2", ('x, 'z))
  source.joinWithSmaller('x -> 'x, source2.read)
    .project('x, 'y, 'z)
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = PostTracing(flowDef, Map[Source,Source](source -> Tsv("foo/input"), 
                                                     source2 -> Tsv("bar/input2")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class PostInputTracingJoinTest extends Specification with TupleConversions {
  import Dsl._
  "Source tracing join" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.PostInputTracingJoinJob")
        .arg("write_sources", "true")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("1","3"), ("2","9"), ("10", "0")))
        .source(Tsv("input2", ('x, 'z)), List(("5","1"), ("1","4"), ("2","7")))
        .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3,4)) must be_==(true)
          unordered((2,9,7)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("bar/input2")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,4)) must be_==(true)
          unordered((2,7)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}
