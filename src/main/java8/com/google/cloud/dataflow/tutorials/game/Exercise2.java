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
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.tutorials.game.utils.ChangeMe;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import com.google.cloud.dataflow.tutorials.game.utils.ParseEventFn;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Second in a series of coding exercises in a gaming domain.
 *
 * <p>This batch pipeline calculates the sum of scores per team per hour, over an entire batch of
 * gaming data and writes the per-team sums to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise2 {
  /** A transform to compute the WindowedTeamScore. */
  public static class WindowedTeamScore
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {

    private Duration duration;

    public WindowedTeamScore(Duration duration) {
      this.duration = duration;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GameEvent> input) {
      // [START EXERCISE 2]:
      // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
      // Developer Docs: https://cloud.google.com/dataflow/model/windowing
      //
      return input
          // Window.into() takes a WindowFn and returns a PTransform that
          // applies windowing to the PCollection. FixedWindows.of() returns a
          // WindowFn that assigns elements to windows of a fixed size. Use
          // these methods to apply fixed windows of size
          // this.duration to the PCollection.
          .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */)
          // Remember the ExtractAndSumScore PTransform from Exercise 1? We
          // parameterized it over the key field. Use it here to compute the "team"
          // scores.
          .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */);
      // [END EXERCISE 2]
    }
  }

  /** Format a KV of team and their score to a BigQuery TableRow. */
  static class FormatTeamScoreSumsFn extends DoFn<KV<String, Integer>, TableRow>
      implements RequiresWindowAccess {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", ((IntervalWindow) c.window()).start().getMillis() / 1000);
      c.output(row);
    }

    /** Defines the BigQuery schema. */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }

  /** Run a batch pipeline. */
  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(options.as(Options.class).getOutputDataset());
    tableRef.setProjectId(options.as(GcpOptions.class).getProject());
    tableRef.setTableId(options.getOutputTableName());

    // Read events from a CSV file and parse them.
    pipeline
        .apply(TextIO.Read.from(options.getInput()))
        .apply(ParDo.named("ParseGameEvent").of(new ParseEventFn()))
        .apply(
            "AddEventTimestamps", WithTimestamps.of((GameEvent i) -> new Instant(i.getTimestamp())))
        .apply("WindowedTeamScore", new WindowedTeamScore(Duration.standardMinutes(60)))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatTeamScoreSums").of(new FormatTeamScoreSumsFn()))
        .apply(
            BigQueryIO.Write.to(tableRef)
                .withSchema(FormatTeamScoreSumsFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
