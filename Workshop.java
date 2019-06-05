package com.mozilla.secops.workshop;

import com.mozilla.secops.CompositeInput;
import com.mozilla.secops.InputOptions;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Getting started with Beam workshop pipeline.
 *
 * <p>Taken from https://github.com/mozilla-services/foxsec-pipeline
 *
 * <p>This class has been adapted for my personal on-boarding to the Apache Beam framework, taking
 * advantage of ameihm0912's workshop
 */
public class Workshop implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * An output transform that simply prints a string
   *
   * <p>Does not need to be modified for workshop.
   */
  public static class PrintOutput extends PTransform<PCollection<String>, PDone> {
    private static final long serialVersionUID = 1L;

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(
          ParDo.of(
              new DoFn<String, Void>() {
                private static final long serialVersionUID = 1L;

                @ProcessElement
                public void processElement(ProcessContext c) {
                  System.out.println(c.element());
                }
              }));
      return PDone.in(input.getPipeline());
    }
  }

  /** DoFn to perform extraction of words from each line of input. */
  public static class ExtractWords extends DoFn<String, String> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      // clean up special characters, make lowercase,
      // and trim whitespace off front and back
      String cleaned =
          c.element()
              .replaceAll("\\.", "")
              .replaceAll(",", "")
              .replaceAll(":", "")
              .replaceAll(";", "")
              .replaceAll("\\(", "")
              .replaceAll("\\)", "")
              .replaceAll("-", "")
              .replaceAll("\\?", "")
              .replaceAll("!", "")
              .toLowerCase()
              .trim();
      // split cleaned up element into lines
      for (String line : cleaned.split("\n")) {
        // split lines into words
        for (String word : line.trim().split(" ")) {
          if (word.equals("")) {
            continue;
          }
          c.output(word);
        }
      }
    }
  }

  /* DoFn with no purpose other than to explore the capabilities of subclasses of DoFn */
  public static class DescribePipeline extends DoFn<String, String> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      // simply pass the element to output
      c.output(c.element());
      // we always have a timestamp associated with every element in a PCollection
      org.joda.time.Instant timestamp = c.timestamp();
      System.out.println("timestamp: " + timestamp.toDate().toString());
      // we always have pipeline options in the ProcessContext
      PipelineOptions opts = c.getPipelineOptions();
      System.out.println("full opts: " + opts.toString());
    }
  }

  /** DoFn to perform extraction of word occurrences from a KV of word-to-count */
  public static class Occurrences extends DoFn<KV<String, Long>, Long> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Long> kv = c.element();
      c.output(kv.getValue());
    }
  }

  /** DoFn to convert pipeline elements from Double to String */
  public static class Stringify extends DoFn<Double, String> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Double.toString(c.element()));
    }
  }

  /** DoFn to wrap the mean value with the format: "mean value: <VALUE>" */
  public static class FormatMeanStr extends DoFn<String, String> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output("mean value: <" + c.element() + ">");
    }
  }

  /** Runtime options for {@link Workshop} pipeline. */
  public interface WorkshopOptions extends InputOptions, PipelineOptions {}

  private static void runWorkshop(WorkshopOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    // read input items (which may be from a file or a stream, as as specified in the options object
    PCollection<String> input = p.apply("read input", new CompositeInput(options));

    // parallelize extraction of words from input data
    PCollection<String> words = input.apply("get words", ParDo.of(new ExtractWords()));

    // count occurrences of words using Beam's Count
    PCollection<KV<String, Long>> wordCounts =
        words.apply("use beam's count transform", Count.perElement());

    // format-wrap each KV of word counts onto a single string with format "<WORD> <OCCURRENCES>"
    PCollection<String> wordCountsFormatted =
        wordCounts.apply(
            "format-wrap word count",
            MapElements.via(
                new SimpleFunction<KV<String, Long>, String>() {
                  private static final long serialVersionUID = 1L;

                  public String apply(KV<String, Long> words) {
                    return "<" + words.getValue() + "> <" + words.getKey() + ">";
                  }
                }));

    // get an occurrences only PCollection in order to easily calculate Mean using Beam's Mean
    // function
    PCollection<Long> occurrences =
        wordCounts.apply("count occurrences", ParDo.of(new Occurrences()));

    // use Beam's mean to calculate the mean
    PCollection<Double> mean = occurrences.apply("compute mean", Mean.<Long>globally());

    // convert Mean's returned Double - to String
    PCollection<String> meanStr = mean.apply("convert mean to string", ParDo.of(new Stringify()));

    // no op to print pipeline details
    meanStr.apply("describe pipeline", ParDo.of(new DescribePipeline()));

    // format-wrap the mean onto a nice output format: "mean value: <VALUE>"
    PCollection<String> meanPretty =
        meanStr.apply("add format wrapper to mean", ParDo.of(new FormatMeanStr()));

    // print out things of interest
    // here instead we could be alerting - sending an HTTP request to a service, etc.
    wordCountsFormatted.apply(new PrintOutput());
    meanPretty.apply(new PrintOutput());

    // block from exiting
    p.run().waitUntilFinish();
  }

  /**
   * Entry point for Beam pipeline.
   *
   * @param args Runtime arguments.
   */
  public static void main(String[] args) throws IOException {
    PipelineOptionsFactory.register(WorkshopOptions.class);
    WorkshopOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WorkshopOptions.class);
    runWorkshop(options);
  }
}
