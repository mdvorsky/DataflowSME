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
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.ApproximateQuantiles;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import com.google.cloud.dataflow.tutorials.game.utils.ParseEventFn;
import com.google.cloud.dataflow.tutorials.game.utils.ParsePlayEventFn;
import com.google.cloud.dataflow.tutorials.game.utils.PlayEvent;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seventh in a series of coding exercises in a gaming domain.
 *
 * <p>This exercise introduces concepts of pubsub message ids, session windows, side inputs, joins,
 * and global combine.
 *
 * <p>See README.md for details.
 */
public class Exercise7 {
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";
  private static final String MESSAGE_ID_ATTRIBUTE = "unique_id";
  private static final int GLOBAL_LATENCY_QUANTILES = 21;
  private static final int GLOBAL_AGGREGATE_FANOUT = 16;
  private static final int GLOBAL_AGGREGATE_TRIGGER_SEC = 30;
  private static final Logger LOG = LoggerFactory.getLogger(Exercise7.class);

  private static final TupleTag<PlayEvent> playTag = new TupleTag<PlayEvent>();
  private static final TupleTag<GameEvent> eventTag = new TupleTag<GameEvent>();

  /** Options supported by {@link GameStats}. */
  interface Exercise7Options extends Options, StreamingOptions {
    @Description("Pub/Sub topic to read from")
    @Validation.Required
    String getTopic();

    void setTopic(String value);

    @Description("Pub/Sub play events topic to read from")
    @Validation.Required
    String getPlayEventsTopic();

    void setPlayEventsTopic(String value);

    @Description("Numeric value of gap between user sessions, in minutes")
    @Default.Integer(5)
    Integer getSessionGap();

    void setSessionGap(Integer value);
  }

  public static class ComputeLatencyFn extends DoFn<KV<String, CoGbkResult>, KV<String, Long>> {
    private final Aggregator<Long, Long> numDroppedSessionsNoEvent =
        createAggregator("DroppedSessionsNoEvent", new Sum.SumLongFn());
    private final Aggregator<Long, Long> numDroppedSessionsTooManyEvents =
        createAggregator("DroppedSessionsTooManyEvents", new Sum.SumLongFn());
    private final Aggregator<Long, Long> numDroppedSessionsNoPlayEvents =
        createAggregator("DroppedSessionsNoPlayEvents", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      Iterable<PlayEvent> plays = c.element().getValue().getAll(playTag);
      Iterable<GameEvent> events = c.element().getValue().getAll(eventTag);
      int playCount = 0;
      Long maxPlayEventTs = 0L;
      for (PlayEvent play : plays) {
        playCount++;
        maxPlayEventTs = Math.max(maxPlayEventTs, play.getTimestamp());
      }
      int eventCount = 0;
      GameEvent event = null;
      for (GameEvent currentEvent : events) {
        eventCount++;
        event = currentEvent;
      }
      String id = c.element().getKey();
      if (eventCount == 0) {
        numDroppedSessionsNoEvent.addValue(1L);
      } else if (eventCount > 1) {
        numDroppedSessionsTooManyEvents.addValue(1L);
      } else if (playCount == 0) {
        numDroppedSessionsNoPlayEvents.addValue(1L);
      } else {
        Long minLatency = event.getTimestamp() - maxPlayEventTs;
        c.output(KV.of(event.getUser(), minLatency));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Exercise7Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise7Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    // Allow the pipeline to be cancelled automatically.
    options.setRunner(DataflowPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference badUserTable = new TableReference();
    badUserTable.setDatasetId(options.getOutputDataset());
    badUserTable.setProjectId(options.getProject());
    badUserTable.setTableId(options.getOutputTableName() + "_bad_users");

    // Read Events from Pub/Sub using custom timestamps and custom message id label.
    PCollection<KV<String, GameEvent>> sessionedEvents =
        pipeline
            .apply(
                "ReadGameScoreEvents",
                PubsubIO.Read.timestampLabel(TIMESTAMP_ATTRIBUTE)
                    .idLabel(MESSAGE_ID_ATTRIBUTE)
                    .topic(options.getTopic()))
            .apply("ParseGameScoreEvents", ParDo.of(new ParseEventFn()))
            .apply(
                "KeyGameScoreByEventId",
                WithKeys.of((GameEvent event) -> event.getEventId())
                    .withKeyType(TypeDescriptor.of(String.class)))
            .apply(
                "SessionizeGameScoreEvents",
                Window.<KV<String, GameEvent>>into(
                        Sessions.withGapDuration(Duration.standardMinutes(options.getSessionGap())))
                    .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow()));

    // Read PlayEvents from Pub/Sub using custom timestamps and custom message id label.
    PCollection<KV<String, PlayEvent>> sessionedPlayEvents =
        pipeline
            .apply(
                "ReadGamePlayEvents",
                PubsubIO.Read.timestampLabel(TIMESTAMP_ATTRIBUTE)
                    .idLabel(MESSAGE_ID_ATTRIBUTE)
                    .topic(options.getPlayEventsTopic()))
            .apply("ParseGamePlayEvents", ParDo.of(new ParsePlayEventFn()))
            .apply(
                "KeyGamePlayByEventId",
                WithKeys.of((PlayEvent play) -> play.getEventId())
                    .withKeyType(TypeDescriptor.of(String.class)))
            .apply(
                "SessionizeGamePlayEvents",
                Window.<KV<String, PlayEvent>>into(
                        Sessions.withGapDuration(Duration.standardMinutes(options.getSessionGap())))
                    .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow()));

    // Compute per-user latency.
    PCollection<KV<String, Long>> userLatency =
        KeyedPCollectionTuple.of(playTag, sessionedPlayEvents)
            .and(eventTag, sessionedEvents)
            .apply("JoinScorePlayEvents", CoGroupByKey.create())
            .apply("ComputeLatency", ParDo.of(new ComputeLatencyFn()));

    // Create a view onto quantiles of the global latency distribution.
    PCollectionView<List<Long>> globalQuantiles =
        userLatency
            .apply("GetLatencies", Values.create())
            // Re-window session results into a global window, and trigger periodically making sure
            // to use the full accumulated window contents.
            .apply(
                "GlobalWindowRetrigger",
                Window.<Long>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(
                                    Duration.standardSeconds(GLOBAL_AGGREGATE_TRIGGER_SEC))))
                    .accumulatingFiredPanes())
            .apply(
                ((Combine.Globally<Long, List<Long>>)
                        ApproximateQuantiles.<Long>globally(GLOBAL_LATENCY_QUANTILES))
                    .withFanout(GLOBAL_AGGREGATE_FANOUT)
                    .asSingletonView());

    userLatency
        // Use the computed latency distribution as a side-input to filter out likely bad users.
        .apply(
            "DetectBadUsers",
            ParDo.withSideInputs(globalQuantiles)
                .of(
                    new DoFn<KV<String, Long>, String>() {
                      public void processElement(ProcessContext c) {
                        String user = c.element().getKey();
                        Long latency = c.element().getValue();
                        List<Long> quantiles = c.sideInput(globalQuantiles);
                        // Users in the first quantile are considered spammers, since their
                        // score to play event latency is too low, suggesting a robot.
                        if (latency < quantiles.get(1)) {
                          c.output(user);
                        }
                      }
                    }))
        // We want to only emilt a single BigQuery row for every bad user. To do this, we
        // re-key by user, then window globally and trigger on the first element for each key.
        .apply(
            "KeyByUser",
            WithKeys.of((String user) -> user).withKeyType(TypeDescriptor.of(String.class)))
        .apply(
            "GlobalWindowsTriggerOnFirst",
            Window.<KV<String, String>>into(new GlobalWindows())
                .triggering(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(10)))
                .accumulatingFiredPanes())
        .apply("GroupByUser", GroupByKey.<String, String>create())
        .apply("FormatBadUsers", ParDo.of(new FormatBadUserFn()))
        .apply(
            "WriteBadUsers",
            BigQueryIO.Write.to(badUserTable)
                .withSchema(FormatBadUserFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
  }

  /** Format a KV of user and associated properties to a BigQuery TableRow. */
  static class FormatBadUserFn extends DoFn<KV<String, Iterable<String>>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("bad_user", c.element().getKey())
              .set("time", Instant.now().getMillis() / 1000);
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("bad_user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("time").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }
}
