/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import net.sf.JRecord.Common.IFieldDetail;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.JRecordInterface1;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This class <code>CopybookToSchema</code> transforms COBOL copybook to Avro compatible schema.
 */
public final class CopybookToSchema {
  private String copybook;
  private String font;

  public CopybookToSchema(String copybook, String font) {
    this.copybook = copybook;
    this.font = font;
  }

  public Schema getSchema(boolean allStringType) throws IOException {
    LayoutDetail layout = JRecordInterface1.COBOL
      .newIOBuilder(IOUtils.toInputStream(copybook, "UTF-8"), "")
      .setFont("cp037")
      .getLayout();

    Map<String, IFieldDetail> recordFieldNameMap = layout.getRecordFieldNameMap();
    List<Schema.Field> schemaFields = Lists.newArrayList();
    for (Map.Entry<String, IFieldDetail> entry : recordFieldNameMap.entrySet()) {
      String name = Datum.canonicalize(entry.getKey());
      Schema.Type schemaType = Schema.Type.STRING;
      if (!allStringType) {
        schemaType = getFieldSchemaType(entry.getValue().getType());
      }
      schemaFields.add(Schema.Field.of(name, Schema.nullableOf(Schema.of(schemaType))));
    }
    return Schema.recordOf(layout.getLayoutName(), schemaFields);
  }

  /**
   * Get the field Schema.Type from the copybook data types
   *
   * @param type AbstractFiledValue type to be converted to CDAP Schema.Type
   * @return CDAP Schema.Type objects
   */
  public static Schema.Type getFieldSchemaType(int type) {
    switch (type) {
      case 0:
        return Schema.Type.STRING;
      case 17:
        return Schema.Type.FLOAT;
      case 18:
      case 22:
      case 31:
      case 32:
      case 33:
        return Schema.Type.DOUBLE;
      case 25:
        return Schema.Type.INT;
      case 35:
      case 36:
      case 39:
        return Schema.Type.LONG;
      default:
        return Schema.Type.STRING;
    }
  }
}
