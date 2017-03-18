/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.tutorials.game.solutions;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fifth in a series of coding exercises in a gaming domain.
 *
 * <p>This exercise introduces side inputs.
 *
 * <p>See README.md for details.
 */
public class Exercise5 {

  private static final Logger LOG = LoggerFactory.getLogger(Exercise5.class);

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

  /**
   * Filter out all but those users with a high clickrate, which we will consider as 'spammy' users.
   * We do this by finding the mean total score per user, then using that information as a side
   * input to filter out all but those user scores that are > (mean * SCORE_WEIGHT)
   */
  public static class CalculateSpammyUsers
      extends PTransform<PCollection<KV<String, Integer>>, PCollection<KV<String, Integer>>> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSpammyUsers.class);
    private static final double SCORE_WEIGHT = 2.5;

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<KV<String, Integer>> userScores) {

      // Get the sum of scores for each user.
      PCollection<KV<String, Integer>> sumScores =
          userScores.apply("UserSum", Sum.<String>integersPerKey());

      // Extract the score from each element, and use it to find the global mean.
      final PCollectionView<Double> globalMeanScore =
          sumScores
              .apply(Values.<Integer>create())
              .apply(Mean.<Integer>globally().asSingletonView());

      // Filter the user sums using the global mean.
      PCollection<KV<String, Integer>> filtered =
          sumScores.apply(
              ParDo.named("ProcessAndFilter")
                  // use the derived mean total score as a side input
                  .withSideInputs(globalMeanScore)
                  .of(
                      new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                        private final Aggregator<Long, Long> numSpammerUsers =
                            createAggregator("SpammerUsers", new Sum.SumLongFn());

                        @Override
                        public void processElement(ProcessContext c) {
                          Integer score = c.element().getValue();
                          Double gmc = c.sideInput(globalMeanScore);
                          if (score > (gmc * SCORE_WEIGHT)) {
                            LOG.info(
                                "user "
                                    + c.element().getKey()
                                    + " spammer score "
                                    + score
                                    + " with mean "
                                    + gmc);
                            numSpammerUsers.addValue(1L);
                            c.output(c.element());
                          }
                        }
                      }));
      return filtered;
    }
  }

  /** Calculate and output an element's session duration. */
  private static class UserSessionInfoFn extends DoFn<KV<String, Integer>, Integer>
      implements RequiresWindowAccess {

    @Override
    public void processElement(ProcessContext c) {
      IntervalWindow w = (IntervalWindow) c.window();
      int duration = new Duration(w.start(), w.end()).toPeriod().toStandardMinutes().getMinutes();
      c.output(duration);
    }
  }

  /** Options supported by {@link GameStats}. */
  interface Exercise5Options extends Options, StreamingOptions {
    @Description("Numeric value of fixed window duration for user analysis, in minutes")
    @Default.Integer(5)
    Integer getFixedWindowDuration();

    void setFixedWindowDuration(Integer value);
  }

  public static void main(String[] args) throws Exception {

    Exercise5Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise5Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    // Allow the pipeline to be cancelled automatically.
    options.setRunner(DataflowPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference teamTable = new TableReference();
    teamTable.setDatasetId(options.getOutputDataset());
    teamTable.setProjectId(options.getProject());
    teamTable.setTableId(options.getOutputTableName());

    PCollection<GameEvent> rawEvents = pipeline.apply(new Exercise3.ReadGameEvents(options));

    // Extract username/score pairs from the event stream
    PCollection<KV<String, Integer>> userEvents =
        rawEvents.apply(
            "ExtractUserScore",
            MapElements.via((GameEvent gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore()))
                .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}));

    // Calculate the total score per user over fixed windows, and
    // cumulative updates for late data.
    final PCollectionView<Map<String, Integer>> spammersView =
        userEvents
            .apply(
                Window.named("FixedWindowsUser")
                    .<KV<String, Integer>>into(
                        FixedWindows.of(
                            Duration.standardMinutes(options.getFixedWindowDuration()))))

            // Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
            // These might be robots/spammers.
            .apply("CalculateSpammyUsers", new CalculateSpammyUsers())
            // Derive a view from the collection of spammer users. It will be used as a side input
            // in calculating the team score sums, below.
            .apply("CreateSpammersView", View.<String, Integer>asMap());

    // Calculate the total score per team over fixed windows,
    // and emit cumulative updates for late data. Uses the side input derived above-- the set of
    // suspected robots-- to filter out scores from those users from the sum.
    // Write the results to BigQuery.
    rawEvents
        .apply(
            Window.named("WindowIntoFixedWindows")
                .<GameEvent>into(
                    FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration()))))
        // Filter out the detected spammer users, using the side input derived above.
        .apply(
            ParDo.named("FilterOutSpammers")
                .withSideInputs(spammersView)
                .of(
                    new DoFn<GameEvent, GameEvent>() {
                      @Override
                      public void processElement(ProcessContext c) {
                        // If the user is not in the spammers Map, output the data element.
                        if (c.sideInput(spammersView).get(c.element().getUser().trim()) == null) {
                          c.output(c.element());
                        }
                      }
                    }))
        // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new Exercise1.ExtractAndSumScore("team"))
        // Write the result to BigQuery
        .apply(ParDo.named("FormatTeamWindows").of(new FormatTeamWindowFn()))
        .apply(
            BigQueryIO.Write.to(teamTable)
                .withSchema(FormatTeamWindowFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
  }

  /** Format a KV of team and associated properties to a BigQuery TableRow. */
  protected static class FormatTeamWindowFn extends DoFn<KV<String, Integer>, TableRow>
      implements RequiresWindowAccess {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", fmt.print(((IntervalWindow) c.window()).start()))
              .set("processing_time", fmt.print(Instant.now()));
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("STRING"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("STRING"));
      return new TableSchema().setFields(fields);
    }
  }
}
