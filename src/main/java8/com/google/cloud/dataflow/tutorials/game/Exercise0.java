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
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import java.util.ArrayList;
import java.util.List;

/**
 * Zeroth (no code changes necessary) in a series of exercises in a gaming domain.
 *
 * <p>This batch pipeline imports game events from CSV to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise0 {
  /**
   * Format a GameEvent to a BigQuery TableRow.
   */
  static class FormatGameEventFn extends DoFn<GameEvent, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      GameEvent event = c.element();
      TableRow row = new TableRow()
          .set("user", event.getUser())
          .set("team", event.getTeam())
          .set("score", event.getScore())
          .set("timestamp", event.getTimestamp() / 1000);
      c.output(row);
    }

    /** Defines the BigQuery schema. */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
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

    // Read events from a CSV file, parse them and write (import) them to BigQuery.
    pipeline
        .apply(TextIO.Read.from(options.getInput()))
        .apply(ParDo.named("ParseGameEvent").of(new ParseEventFn()))
        .apply(ParDo.named("FormatGameEvent").of(new FormatGameEventFn()))
        .apply(
            BigQueryIO.Write.to(tableRef)
                .withSchema(FormatGameEventFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
