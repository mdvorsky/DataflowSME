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

package com.google.cloud.dataflow.tutorials.game.utils;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;

/**
 * Options supported by the exercise pipelines.
 */
public interface Options extends PipelineOptions {

  @Description("Path to the data file(s) containing game data.")
  String getInput();
  void setInput(String value);

  @Description("BigQuery Dataset to write tables to. Must already exist.")
  @Validation.Required
  String getOutputDataset();
  void setOutputDataset(String value);

  @Description("The BigQuery table name. Should not already exist.")
  @Validation.Required
  String getOutputTableName();
  void setOutputTableName(String value);
}
