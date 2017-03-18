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

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

/**
 * Class to hold info about a game event.
 */
@DefaultCoder(AvroCoder.class)
public class GameEvent {
  @Nullable String user;
  @Nullable String team;
  @Nullable Integer score;
  @Nullable Long timestamp;
  @Nullable String eventId;

  public GameEvent() {}

  public GameEvent(String user, String team, Integer score, Long timestamp, String eventId) {
    this.user = user;
    this.team = team;
    this.score = score;
    this.timestamp = timestamp;
    this.eventId = eventId;
  }

  public String getUser() {
    return this.user;
  }
  public String getTeam() {
    return this.team;
  }
  public Integer getScore() {
    return this.score;
  }
  public String getKey(String keyname) {
    if (keyname.equals("team")) {
      return this.team;
    } else {  // return username as default
      return this.user;
    }
  }
  public Long getTimestamp() {
    return this.timestamp;
  }
  public String getEventId() {
    return this.eventId;
  }
}
