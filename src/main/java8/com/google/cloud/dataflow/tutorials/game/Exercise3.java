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

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.tutorials.game.solutions.Exercise2;
import com.google.cloud.dataflow.tutorials.game.utils.ChangeMe;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import org.joda.time.Duration;

/**
 * Third in a series of coding exercises in a gaming domain.
 *
 * <p>This is the same pipeline as in Exercise 2, but can run in either batch or streaming mode.
 *
 * <p>See README.md for details.
 */
public class Exercise3 {
  /** A transform to read the game events from either text files or Pub/Sub topic. */
  public static class ReadGameEvents extends PTransform<PBegin, PCollection<GameEvent>> {
    private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

    private Options options;

    public ReadGameEvents(Options options) {
      this.options = options;
    }

    @Override
    public PCollection<GameEvent> apply(PBegin begin) {
      // [START EXERCISE 3]:
      // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
      // Developer Docs: https://cloud.google.com/dataflow/model/pubsub-io
      //
      // Determine whether to use files or topic based on options.
      // (For example, check that options.getInput() is not null and is non-empty.)
      if (true /* TODO: YOUR CODE GOES HERE */) {
        return begin
            .getPipeline()
            // Read game events from files. See main() in Exercise2. Don't forget to include
            // WithTimestamps transform to assign timestamps to events.
            .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */)
            .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */)
            .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */);
      } else {
        return begin
            .getPipeline()
            // Read game events from Pub/Sub topic options.getTopic() using custom timestamps, which
            // are extracted from the pubsub label TIMESTAMP_ATTRIBUTE.
            // Use PubsubIO.Read with timestampLabel() and topic().
            .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */)
            // Parse the messages the same way as when they come from the text file. Note that we no
            // longer have to run WithTimestamps transform, as the timestamps are already set by
            // PubsubIO. (In streaming, changing timestamps must be done carefully to avoid
            // guarantees necessary for watermarks.)
            .apply(new ChangeMe<>() /* TODO: YOUR CODE GOES HERE */);
      }
      // [END EXERCISE 3]
    }
  }

  /** Run a batch or streaming pipeline. */
  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(options.as(Options.class).getOutputDataset());
    tableRef.setProjectId(options.as(GcpOptions.class).getProject());
    tableRef.setTableId(options.getOutputTableName());

    // Read events from either a CSV file or PubSub stream.
    pipeline
        .apply(new ReadGameEvents(options))
        .apply("WindowedTeamScore", new Exercise2.WindowedTeamScore(Duration.standardMinutes(60)))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatTeamScoreSums").of(new Exercise2.FormatTeamScoreSumsFn()))
        .apply(
            BigQueryIO.Write.to(tableRef)
                .withSchema(Exercise2.FormatTeamScoreSumsFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
