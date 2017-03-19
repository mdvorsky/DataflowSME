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
package com.google.cloud.dataflow.tutorials.game;

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
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.Mean;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.cloud.dataflow.tutorials.game.solutions.Exercise3;
import com.google.cloud.dataflow.tutorials.game.utils.ChangeMe;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sixth in a series of coding exercises in a gaming domain.
 *
 * <p>This exercise introduces session windows.
 *
 * <p>See README.md for details.
 */
public class Exercise6 {

  private static final Logger LOG = LoggerFactory.getLogger(Exercise6.class);

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
  interface Exercise6Options extends Options, StreamingOptions {
    @Description("Numeric value of gap between user sessions, in minutes")
    @Default.Integer(1)
    Integer getSessionGap();

    void setSessionGap(Integer value);

    @Description(
        "Numeric value of fixed window for finding mean of user session duration, " + "in minutes")
    @Default.Integer(5)
    Integer getUserActivityWindowDuration();

    void setUserActivityWindowDuration(Integer value);
  }

  public static void main(String[] args) throws Exception {

    Exercise6Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise6Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    // Allow the pipeline to be cancelled automatically.
    options.setRunner(DataflowPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference sessionsTable = new TableReference();
    sessionsTable.setDatasetId(options.getOutputDataset());
    sessionsTable.setProjectId(options.getProject());
    sessionsTable.setTableId(options.getOutputTableName());

    PCollection<GameEvent> rawEvents = pipeline.apply(new Exercise3.ReadGameEvents(options));

    // Extract username/score pairs from the event stream
    PCollection<KV<String, Integer>> userEvents =
        rawEvents.apply(
            "ExtractUserScore",
            MapElements.via((GameEvent gInfo) -> KV.of(gInfo.getUser(), gInfo.getScore()))
                .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}));

    // [START EXERCISE 6]:
    // Detect user sessions-- that is, a burst of activity separated by a gap from further
    // activity. Find and record the mean session lengths.
    // This information could help the game designers track the changing user engagement
    // as their set of games changes.
    userEvents
        // Window the user events into sessions with gap options.getSessionGap() minutes. Make sure
        // to use an outputTimeFn that sets the output timestamp to the end of the window. This will
        // allow us to compute means on sessions based on their end times, rather than their start
        // times.
        .apply(
            /* TODO: YOUR CODE GOES HERE */
            new ChangeMe<PCollection<KV<String, Integer>>, KV<String, Integer>>())
        // For this use, we care only about the existence of the session, not any particular
        // information aggregated over it, so the following is an efficient way to do that.
        .apply(Combine.perKey(x -> 0))
        // Get the duration per session.
        .apply("UserSessionActivity", ParDo.of(new UserSessionInfoFn()))
        // Re-window to process groups of session sums according to when the sessions complete.
        // In streaming we don't just ask "what is the mean value" we must ask "what is the mean
        // value for some window of time". To compute periodic means of session durations, we
        // re-window the session durations.
        .apply(
            /* TODO: YOUR CODE GOES HERE */
            new ChangeMe<PCollection<Integer>, Integer>())
        // Find the mean session duration in each window.
        .apply(Mean.<Integer>globally().withoutDefaults())
        // Write this info to a BigQuery table.
        .apply(ParDo.named("FormatSessions").of(new FormatSessionWindowFn()))
        .apply(
            BigQueryIO.Write.to(sessionsTable)
                .withSchema(FormatSessionWindowFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    // [END EXERCISE 6]:

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
  }

  /** Format a KV of session and associated properties to a BigQuery TableRow. */
  static class FormatSessionWindowFn extends DoFn<Double, TableRow>
      implements RequiresWindowAccess {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("window_start", ((IntervalWindow) c.window()).start().getMillis() / 1000)
              .set("mean_duration", c.element());
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("mean_duration").setType("FLOAT"));
      return new TableSchema().setFields(fields);
    }
  }
}
