/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.plugin.mainframe;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit tests for {@link Config}
 */
public class ConfigTest {

  private static final String COPYBOOK_CONTENTS = "" +
    "000100*                                                                         \n" +
    "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
    "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
    "000400*                                                                         \n" +
    "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
    "000600*                                                                         \n" +
    "000700*   RECORD LENGTH IS 27.                                                  \n" +
    "000800*                                                                         \n" +
    "000900        03  DTAR020-KCODE-STORE-KEY.                                      \n" +
    "001000            05 DTAR020-KEYCODE-NO      PIC X(08).                         \n" +
    "001100            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               \n" +
    "001200        03  DTAR020-DATE               PIC S9(07)   COMP-3.               \n" +
    "001300        03  DTAR020-DEPT-NO            PIC S9(03)   COMP-3.               \n" +
    "001400        03  DTAR020-QTY-SOLD           PIC S9(9)    COMP-3.               \n" +
    "001500        03  DTAR020-SALE-PRICE         PIC S9(9)V99 COMP-3.";

  // After replacing :PREFIX: and "COMP: with DTAR020 and COMP-3,
  // this copybook should yield the same outputs as COPYBOOK_CONTENTS
  private static final String COPYBOOK_WITH_REPLACEMENTS = "" +
    "000100*                                                                         \n" +
    "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
    "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
    "000400*                                                                         \n" +
    "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
    "000600*                                                                         \n" +
    "000700*   RECORD LENGTH IS 27.                                                  \n" +
    "000800*                                                                         \n" +
    "000900        03  :PREFIX:-KCODE-STORE-KEY.                                      \n" +
    "001000            05 :PREFIX:-KEYCODE-NO      PIC X(08).                         \n" +
    "001100            05 :PREFIX:-STORE-NO        PIC S9(03)   :COMP:.               \n" +
    "001200        03  :PREFIX:-DATE               PIC S9(07)   :COMP:.               \n" +
    "001300        03  :PREFIX:-DEPT-NO            PIC S9(03)   :COMP:.               \n" +
    "001400        03  :PREFIX:-QTY-SOLD           PIC S9(9)    :COMP:.               \n" +
    "001500        03  :PREFIX:-SALE-PRICE         PIC S9(9)V99 :COMP:.";

  private static final String MOCK_STAGE = "mockStage";
  private static final Config VALID_CONFIG = new Config(
    "referenceName",
    "/tmp/binary/file/path",
    COPYBOOK_CONTENTS,
    null,
    null,
    null,
    null,
    null,
    null);


  @Test
  public void testValidateCopybookContents() {
    Config config = Config.builder(VALID_CONFIG)
      .setCopybookContents(COPYBOOK_WITH_REPLACEMENTS)
      .build();
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    List<List<String>> paramName = Arrays.asList(
      Arrays.asList(Config.COPYBOOK_CONTENTS, Config.REPLACEMENTS),
      Arrays.asList(Config.COPYBOOK_CONTENTS, Config.REPLACEMENTS),
      Arrays.asList(Config.COPYBOOK_CONTENTS, Config.REPLACEMENTS),
      Arrays.asList(Config.COPYBOOK_CONTENTS, Config.REPLACEMENTS),
      Arrays.asList(Config.COPYBOOK_CONTENTS, Config.REPLACEMENTS),
      Arrays.asList(Config.COPYBOOK_CONTENTS, Config.REPLACEMENTS),
      Collections.singletonList(Config.COPYBOOK_CONTENTS)
    );
    assertValidationFailed(failureCollector, paramName);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }

}
