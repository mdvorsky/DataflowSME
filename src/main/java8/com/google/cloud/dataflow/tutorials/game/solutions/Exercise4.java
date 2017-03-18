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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Fourth in a series of coding exercises in a gaming domain.
 *
 * <p>This streaming pipeline calculates user and team scores for a window of time and writes them
 * to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise4 {

  static final Duration TEN_SECONDS = Duration.standardSeconds(10);
  static final Duration THIRTY_SECONDS = Duration.standardSeconds(30);

  /** Exercise4Options supported by {@link Exercise4}. */
  interface Exercise4Options extends Options, StreamingOptions {
    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(1)
    Integer getTeamWindowDuration();

    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(2)
    Integer getAllowedLateness();

    void setAllowedLateness(Integer value);
  }

  /**
   * Extract user/score pairs from the event stream using processing time, via global windowing. Get
   * periodic updates on all users' running scores.
   */
  @VisibleForTesting
  static class CalculateUserScores
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {
    private final Duration allowedLateness;

    CalculateUserScores(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GameEvent> input) {
      return input
          .apply(
              "LeaderboardUserGlobalWindow",
              Window.<GameEvent>into(new GlobalWindows())
                  // Get periodic results every 30 seconds.
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane().plusDelayOf(THIRTY_SECONDS)))
                  .accumulatingFiredPanes()
                  .withAllowedLateness(allowedLateness))
          // Extract and sum username/score pairs from the event data.
          .apply("ExtractUserScore", new Exercise1.ExtractAndSumScore("user"));
    }
  }

  /** Calculates scores for each team within the configured window duration. */
  // Extract team/score pairs from the event stream, using hour-long windows by default.
  @VisibleForTesting
  static class CalculateTeamScores
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {
    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GameEvent> infos) {
      return infos
          .apply(
              "LeaderboardTeamFixedWindows",
              Window.<GameEvent>into(FixedWindows.of(teamWindowDuration))
                  // We will get early (speculative) results as well as cumulative
                  // processing of late data.
                  .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withEarlyFirings(
                              AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_SECONDS))
                          .withLateFirings(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(THIRTY_SECONDS)))
                  .withAllowedLateness(allowedLateness)
                  .accumulatingFiredPanes())
          // Extract and sum teamname/score pairs from the event data.
          .apply("ExtractTeamScore", new Exercise1.ExtractAndSumScore("team"));
    }
  }

  public static void main(String[] args) throws Exception {
    Exercise4Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise4Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    // For example purposes, allow the pipeline to be easily cancelled instead of running
    // continuously.
    options.setRunner(DataflowPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference teamTable = new TableReference();
    teamTable.setDatasetId(options.getOutputDataset());
    teamTable.setProjectId(options.getProject());
    teamTable.setTableId(options.getOutputTableName() + "_team");

    TableReference userTable = new TableReference();
    userTable.setDatasetId(options.getOutputDataset());
    userTable.setProjectId(options.getProject());
    userTable.setTableId(options.getOutputTableName() + "_user");

    PCollection<GameEvent> gameEvents = pipeline.apply(new Exercise3.ReadGameEvents(options));

    gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatTeamScores").of(new FormatTeamScoreFn()))
        .apply(
            BigQueryIO.Write.to(teamTable)
                .withSchema(FormatTeamScoreFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    gameEvents
        .apply(
            "CalculateUserScores",
            new CalculateUserScores(Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatUserScores").of(new FormatUserScoreFn()))
        .apply(
            BigQueryIO.Write.to(userTable)
                .withSchema(FormatUserScoreFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
  }

  /** Format a KV of team and associated properties to a BigQuery TableRow. */
  protected static class FormatTeamScoreFn extends DoFn<KV<String, Integer>, TableRow>
      implements RequiresWindowAccess {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", ((IntervalWindow) c.window()).start().getMillis() / 1000)
              .set("processing_time", Instant.now().getMillis() / 1000)
              .set("timing", c.pane().getTiming().toString());
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
      return new TableSchema().setFields(fields);
    }
  }

  /** Format a KV of user and associated properties to a BigQuery TableRow. */
  static class FormatUserScoreFn extends DoFn<KV<String, Integer>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("user", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("processing_time", Instant.now().getMillis() / 1000);
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }
}
