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
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import com.google.cloud.dataflow.tutorials.game.utils.ParseEventFn;
import java.util.ArrayList;
import java.util.List;

/**
 * First in a series of coding exercises in a gaming domain.
 *
 * <p>This batch pipeline calculates the sum of scores per user, over an entire batch of gaming data
 * and writes the sums to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise1 {
  /**
   * A transform to extract key/score information from GameEvent, and sum
   * the scores. The constructor arg determines whether 'team' or 'user' info is
   * extracted.
   */
  public static class ExtractAndSumScore
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {

    private final String field;

    public ExtractAndSumScore(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GameEvent> gameEvents) {
      return gameEvents
          .apply(
              MapElements.via((GameEvent event) -> KV.of(event.getKey(field), event.getScore()))
                  .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}))
          .apply(Sum.<String>integersPerKey());
    }
  }

  /**
   * Format a KV of user and their score to a BigQuery TableRow.
   */
  static class FormatUserScoreSumsFn extends DoFn<KV<String, Integer>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("user", c.element().getKey())
          .set("total_score", c.element().getValue());
      c.output(row);
    }

    /** Defines the BigQuery schema. */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      return new TableSchema().setFields(fields);
    }
  }

  /**
   * Run a batch pipeline.
   */
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
        // Extract and sum username/score pairs from the event data.
        .apply("ExtractUserScore", new ExtractAndSumScore("user"))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatUserScoreSums").of(new FormatUserScoreSumsFn()))
        .apply(
            BigQueryIO.Write.to(tableRef)
                .withSchema(FormatUserScoreSumsFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
