/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

package io.cdap.plugin.mainframe.common;

import com.google.common.io.CharSource;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * A {@link CharSource} for {@link Location}.
 */
public class LocationCharSource extends CharSource {

  private final Location location;
  private final Charset charset;

  public LocationCharSource(Location location, Charset charset) {
    this.location = location;
    this.charset = charset;
  }

  @Override
  public Reader openStream() throws IOException {
    return new InputStreamReader(location.getInputStream(), charset);
  }
}
