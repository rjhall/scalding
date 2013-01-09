Input Tracing
=====

We implement a form of tracing such that at all times we know which input rows gave rise to each row in the pipe.
This is achived by appending a field to the pipe (after its first read from a source) which contains a representation of
the entire source row.  Subsequent flow steps (such as joins, groupBy etc) maintain this field, then when the pipe is written,
this field is used to reconstruct the subsets of the input data which gave rise to the output data.  These subsets are then
writen to disk.

Usage
-----

To use the input tracing in your job is straightforward, all that is required is to extend InputTracingJob, and to wrap
whichever source files you want to be traced.  A canonical example is e.g.,

    package com.etsy.scalding.jobs

    import com.twitter.scalding._
    import com.etsy.scalding._

    class ExampleJob(args : Args) extends InputTracingJob(args) {
      TracingFileSource(Tsv("example_input1"), "subsample/input1")
        .optionalSubsample(0.0001)
        .joinWithSmaller('key1 -> 'key2, TracingFileSource(SequenceFile("example_input2", "sample/input2"))
        .write(SequenceFile("foo.seq"))
    }

The intent of this job is to construct subsamples of `"example_input1"` and `"example_input2"` so that the elements 
of the former will join to the elements of the latter.  An alternative means of constructing these subsets 
(independently subsampling both sources) may fail, since the result of the join may be empty.

Commandline Flags
-----

In order to use the input tracing behavior there are two commandline flags which can be used.  The first is 
`--write_sources` which causes the subsets of the inputs to actually get written (by default they are not and the job
just behaves as normal).  The second option is `--use_sources` which should be used after the traced inputs have been written.
This causes the subsets to be read, in place of the original source.

In order to handle jobs which involve many joins or groupBys, there is an option to trace the input tuples using 
a bloom filter rather than by a list of raw tuples.  To invoke this use the commandline flag `--bf`, the width of the bloom
filter in bits is controlled by `--bfwidth` and the number of hashes by `--bfhashes`, although the default values should be
sufficient for most cases (524288 bits and 5 hashes).


Explanation
------

The method TracingFileSource constructs a kind of file source which is traced in this way.  It takes two arguments,
the first is a FileSource (e.g., a Tsv, SequenceFile etc.), and the second is a filename where the subset will 
be written (and subsequently read from).

InputTracingJob simply provides the convenience methods for the wrapping of sources, and also sets up the flow to
write the subsets on completion of the job.

Finally the method `optionalSubsample` can be used from within an InputTracingJob.  What this method does depends on the 
commandline flags.  In the case that no option is sprcified, or `--write_sources` is used, then this method keeps a fraction
of the rows of the pipe that is approximately equal to the parameter, and discards the rest.  In the case that `--use_sources` is
used, then this method does nothing (since the subsampling would have already happened during the generation of the subsampled data).
The subsampling is implemented by hashing the pipe contents rather than by random number generation, therefore repeated runs of the same job
will always generate the same subsample.

Details
-----

The function TracingFileSource constructs an instance of a TracingFileSource which is a kind of FileSource.  Depending
on the command line flags, this source either reads from the FileSource which was passed to it, or from a sequence file 
with the specified filename for the subsample.

When this kind of source is read (to construct a head pipe), behind the scenes, the input rows are crammed into a single field called
`__source_data__` which is appended to each row.  This field contains a map of input source to a list of input tuples, so at the end of the flow
it is easy to determine which source tuples came from each traced source.  Note that the name of this field can be changed by using the commandline
flag `--tracing_field [name]`.

RichPipe is modified to transparently maintain this row.  Thus if your job performs e.g., a `project` or `mapTo`, this field is 
not dropped.  What's more when you write the pipe out, this field is discarded from your pipe so does not pollute your output.

When your job performs a join, in the case that both pipes have input tracing, the `__source_data__` fields are merged for the joined rows, likewise
when performing a groupBy on a single pipe.


Caveats
-----

 - When tracing the inputs and performing a groupBy, the input-tracing field for an output row will contain all the 
input rows of the group.  Thus when performing a groupAll, it will contain the entire file.  This should not be problematic
on its own, however when this result is then joined to a second pipe it could lead to a massive blowup in the size of the
intermediate files.  Thus, if you know a source file is going to be involved in a groupAll, do not use tracing for that source.

 - Since the subsampling operation is performed via hashing, it may behave in a way which is unintuitive to the user.  For example
saying `pipe.optionalSubsample(0.1).optionalSubsample(0.1)` will be the same as `pipe.optionalSubsample(0.1)`, since whatever elements
are retained in the first call are also retained in the second call.  This is not necessarily true when the two calls take different
parameter values though.
