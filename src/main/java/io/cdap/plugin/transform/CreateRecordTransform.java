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

package io.cdap.plugin.transform;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transforms flat records into hierarchical records.
 */
@Plugin(type = "transform")
@Name("CreateRecord")
@Description("Create Record plugin transforms flat structures into hierarchical structures.")
public class CreateRecordTransform extends Transform<StructuredRecord, StructuredRecord> {

  /**
   * Create hierarchy config
   */
  public static class CreateRecordTransformConfig extends PluginConfig {
    public static final String FIELD_MAPPING = "fieldMapping";
    public static final String INCLUDE_NON_MAPPED_FIELDS = "includeNonMappedFields";

    @Macro
    @Description("Specifies the mapping for generating the hierarchy.")
    @Name(FIELD_MAPPING)
    String fieldMapping;

    @Macro
    @Description("Specifies whether the fields in the input schema that are not part of the mapping, " +
      "should be carried over as-is.")
    @Name(INCLUDE_NON_MAPPED_FIELDS)
    String includeNonMappedFields;

    public CreateRecordTransformConfig(String fieldMapping, String includeNonMappedFields) {
      this.fieldMapping = fieldMapping;
      this.includeNonMappedFields = includeNonMappedFields;
    }

    public String getFieldMapping() {
      return fieldMapping;
    }

    public JsonElement getFieldMappingJson() {
      return GSON.fromJson(fieldMapping, JsonElement.class);
    }

    public String getIncludeNonMappedFields() {
      return includeNonMappedFields;
    }

    public void validate(FailureCollector collector) {
      if (!containsMacro(FIELD_MAPPING)) {
        try {
          getFieldMappingJson();
        } catch (Exception e) {
          collector.addFailure("Invalid field mapping provided.",
                               "Please provide valid field mapping.").withConfigProperty(FIELD_MAPPING);
        }
      }
      if (!containsMacro(INCLUDE_NON_MAPPED_FIELDS) && Strings.isNullOrEmpty(INCLUDE_NON_MAPPED_FIELDS)) {
        collector.addFailure("Include missing fields property missing.",
                             "Please provide include missing fields value.")
          .withConfigProperty(INCLUDE_NON_MAPPED_FIELDS);
      }
      collector.getOrThrowException();
    }

    public boolean hasFieldsWithMacro() {
      return containsMacro(FIELD_MAPPING) || containsMacro(INCLUDE_NON_MAPPED_FIELDS);
    }
  }

  private final CreateRecordTransformConfig createRecordTransformConfig;
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(CreateRecordTransform.class);
  private JsonElement fieldMappingJson = null;
  private Schema outputSchema = null;

  public CreateRecordTransform(CreateRecordTransformConfig createRecordTransformConfig) {
    this.createRecordTransformConfig = createRecordTransformConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    createRecordTransformConfig.validate(failureCollector);
    if (pipelineConfigurer.getStageConfigurer().getInputSchema() != null &&
      !createRecordTransformConfig.hasFieldsWithMacro()) {
      //validate the input schema and get the output schema for it
      final Schema outputSchema = getOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema(),
                                                  failureCollector);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector failureCollector = context.getFailureCollector();
    createRecordTransformConfig.validate(failureCollector);
    recordLineage(context, context.getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    FailureCollector failureCollector = context.getFailureCollector();
    createRecordTransformConfig.validate(failureCollector);
    outputSchema = context.getOutputSchema();
    if (outputSchema == null && context.getInputSchema() != null) {
      outputSchema = getOutputSchema(context.getInputSchema(), context.getFailureCollector());
      fieldMappingJson = getFinalFieldMappingJson(context.getInputSchema());
    }
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    if (outputSchema == null) {
      outputSchema = getOutputSchema(structuredRecord.getSchema(), getContext().getFailureCollector());
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    if (fieldMappingJson == null) {
      fieldMappingJson = getFinalFieldMappingJson(structuredRecord.getSchema());
    }
    mapFields(builder, structuredRecord, fieldMappingJson, outputSchema);
    emitter.emit(builder.build());
  }

  /**
   * Map input data fields to output data fields
   *
   * @param builder          {@link StructuredRecord.Builder} builder set the data for
   * @param oldRecord        {@link StructuredRecord} existing record to read from
   * @param fieldMappingJson {@link JsonElement} path for the field to read from
   */
  private void mapFields(StructuredRecord.Builder builder, StructuredRecord oldRecord, JsonElement fieldMappingJson,
                         Schema outputSchema) {
    for (Map.Entry<String, JsonElement> treeNode : fieldMappingJson.getAsJsonObject().entrySet()) {
      if (treeNode.getValue().isJsonArray()) {
        builder.set(treeNode.getKey(), getField(oldRecord, treeNode.getValue().getAsJsonArray()));
      }
      if (treeNode.getValue().isJsonObject()) {
        final Schema schema = outputSchema.getField(treeNode.getKey()).getSchema();
        StructuredRecord.Builder builder1 = StructuredRecord.builder(schema);
        mapFields(builder1, oldRecord, treeNode.getValue(), schema);
        builder.set(treeNode.getKey(), builder1.build());
      }
    }
  }

  /**
   * Generate field for output schema from input schema and mapping field data
   *
   * @param inputSchema      {@link Schema}
   * @param collector        {@link FailureCollector}
   * @param recordName       name for the field of type record
   * @param fieldMappingJson {@link JsonElement} path for the field in input record
   * @return returns {@link Schema} generated from mapping
   */
  private Schema generateFields(Schema inputSchema, FailureCollector collector, String recordName,
                                JsonElement fieldMappingJson) {
    List<Schema.Field> fieldList = new ArrayList<>();
    for (Map.Entry<String, JsonElement> treeNode : fieldMappingJson.getAsJsonObject().entrySet()) {
      if (treeNode.getValue().isJsonArray()) {
        final Schema.Field field = getField(inputSchema, treeNode.getValue().getAsJsonArray());
        if (field == null) {
          collector.addFailure(String.format("Field with name %s not found in input schema.",
                                             treeNode.getValue()),
                               "Please make sure mapped fields are available in input schema.")
            .withConfigProperty(CreateRecordTransformConfig.FIELD_MAPPING);
          collector.getOrThrowException();
        }
        fieldList.add(Schema.Field.of(treeNode.getKey(), field.getSchema()));
      }
      if (treeNode.getValue().isJsonObject()) {
        final Schema result = generateFields(inputSchema, collector, treeNode.getKey(),
                                             treeNode.getValue().getAsJsonObject());
        fieldList.add(Schema.Field.of(treeNode.getKey(), result));
      }
    }
    return Schema.recordOf(recordName, fieldList);
  }

  private List<List<String>> generateFieldMapForInputSchema(List<String> path, Schema inputSchema) {
    List<List<String>> map = new ArrayList<>();
    for (Schema.Field field : inputSchema.getFields()) {
      List<String> fieldPath = new ArrayList<>(path);
      fieldPath.add(field.getName());
      if (field.getSchema().getType().equals(Schema.Type.RECORD)) {
        map.addAll(generateFieldMapForInputSchema(fieldPath, field.getSchema()));
      } else {
        map.add(fieldPath);
      }
    }
    return map;
  }

  /**
   * Converts field map to array list
   *
   * @param fieldMap field map from config
   * @return List of field maps
   */
  private List<List<String>> fieldMapToList(JsonElement fieldMap) {
    List<List<String>> map = new ArrayList<>();
    final Type listType = new TypeToken<List<String>>() {
    }.getType();

    for (Map.Entry<String, JsonElement> mapEntry : fieldMap.getAsJsonObject().entrySet()) {
      if (mapEntry.getValue().isJsonObject()) {
        map.addAll(fieldMapToList(mapEntry.getValue()));
      } else {
        map.add(GSON.fromJson(mapEntry.getValue(), listType));
      }
    }
    return map;
  }

  /**
   * Calculates difference between two field mapping lists
   *
   * @param source source list to compare from
   * @param target target list to compare with
   * @return difference between two lists
   */
  private List<List<String>> differenceBetweenMaps(List<List<String>> source, List<List<String>> target) {
    List<List<String>> difference = new ArrayList<>();
    for (List<String> item : source) {
      if (!target.contains(item)) {
        difference.add(item);
      }
    }
    return difference;
  }

  /**
   * Add non mapped fields to field map
   *
   * @param filedMappings  existing field mapping
   * @param differenceList list of non mapped fields
   * @return updated field mapping
   */
  private JsonElement includeMissingFields(JsonElement filedMappings, List<List<String>> differenceList) {
    for (List<String> field : differenceList) {
      addField(field, field, filedMappings);
    }
    return filedMappings;
  }

  /**
   * Add field to json element
   *
   * @param rootPath    absolute path of field
   * @param path        relative path of the field
   * @param jsonElement current json element
   * @return updated json element
   */
  private JsonElement addField(List<String> rootPath, List<String> path, JsonElement jsonElement) {
    if (jsonElement.isJsonObject()) {
      if (!jsonElement.getAsJsonObject().has(path.get(0))) {
        if (path.size() > 1) {
          jsonElement.getAsJsonObject().add(path.get(0), addField(rootPath, path.subList(1, path.size()),
                                                                  GSON.fromJson("{}", JsonElement.class)));
        } else {
          jsonElement.getAsJsonObject().add(path.get(0), GSON.toJsonTree(rootPath));
        }
      }
    }
    return jsonElement;
  }

  /**
   * Return field mapping with/without unmapped fields based on config
   *
   * @param inputSchema {@link Schema} current input schema
   * @return {@link JsonElement} field mapping
   */
  private JsonElement getFinalFieldMappingJson(Schema inputSchema) {
    boolean includeNonMappedFields = createRecordTransformConfig.getIncludeNonMappedFields().equals("on");
    final JsonElement fieldMappingJson = createRecordTransformConfig.getFieldMappingJson();
    if (includeNonMappedFields) {
      final List<List<String>> inputSchemaMap = generateFieldMapForInputSchema(new ArrayList<>(), inputSchema);
      final List<List<String>> fieldMapToList = fieldMapToList(fieldMappingJson);
      final List<List<String>> difference = differenceBetweenMaps(inputSchemaMap, fieldMapToList);
      return includeMissingFields(fieldMappingJson, difference);
    }
    return fieldMappingJson;
  }

  /**
   * Generate output schema
   */
  private Schema getOutputSchema(Schema inputSchema, FailureCollector collector) {
    final JsonElement fieldMappingJson = getFinalFieldMappingJson(inputSchema);
    if (fieldMappingJson.isJsonNull()) {
      collector.addFailure("Empty mapping field.", "Please provide valid mapping field.")
        .withConfigProperty(CreateRecordTransformConfig.FIELD_MAPPING);
    }
    collector.getOrThrowException();
    final Schema schema = generateFields(inputSchema, collector, "record",
                                         fieldMappingJson.getAsJsonObject());
    return schema;
  }

  /**
   * Get field from structured record
   *
   * @param structuredRecord {@link StructuredRecord} record to read the data from
   * @param pathMap          {@link JsonArray} path of the field to read from
   * @return {@link StructuredRecord} property containing the value
   */
  private Object getField(StructuredRecord structuredRecord, JsonArray pathMap) {
    Object value = null;
    boolean isFirstValue = true;
    final Iterator<JsonElement> iterator = pathMap.iterator();
    while (iterator.hasNext()) {
      final JsonElement next = iterator.next();
      if (isFirstValue) {
        value = structuredRecord.get(next.getAsString());
        isFirstValue = false;
      } else {
        value = ((StructuredRecord) value).get(next.getAsString());
      }
      // return null if one of the fields in path is not found
      if (value == null) {
        return null;
      }
    }
    return value;
  }

  /**
   * Get field from schema
   *
   * @param inputSchema {@link Schema} input schema
   * @param pathMap     {@link JsonArray} path of the field to read
   * @return return field found in given path
   */
  private Schema.Field getField(Schema inputSchema, JsonArray pathMap) {
    Schema.Field value = null;
    boolean isFirstValue = true;
    final Iterator<JsonElement> iterator = pathMap.iterator();
    while (iterator.hasNext()) {
      final JsonElement next = iterator.next();
      if (isFirstValue) {
        value = inputSchema.getField(next.getAsString());
        isFirstValue = false;
      } else {
        value = value.getSchema().getField(next.getAsString());
      }
      // return null if one of the fields in path is not found
      if (value == null) {
        return null;
      }
    }
    return value;
  }

  /**
   * Emit lineage
   *
   * @param context {@link StageSubmitterContext}
   * @param schema  input schema
   */
  private void recordLineage(StageSubmitterContext context, Schema schema) {
    if (schema == null) {
      LOG.debug("The input schema is null. Field level lineage will not be recorded");
      return;
    }
    JsonElement fieldMappingData = getFinalFieldMappingJson(schema);
    LinkedList<FieldOperation> lineageData = generateLineageRecord("record", fieldMappingData.getAsJsonObject());
    context.record(lineageData);
  }

  /**
   * Generates lineage for each record in mapping json
   *
   * @param record
   */
  private LinkedList<FieldOperation> generateLineageRecord(String recordName, JsonElement record) {
    LinkedList<FieldOperation> fieldOperations = new LinkedList<>();
    List<String> inputFields = new ArrayList<>();
    List<String> outputFields = new ArrayList<>();
    for (Map.Entry<String, JsonElement> recordNode : record.getAsJsonObject().entrySet()) {
      JsonElement value = recordNode.getValue();
      if (value.isJsonArray()) {
        // the field is not nested field - input is mapped to output directly
        outputFields.add(recordNode.getKey());
        inputFields.add(value.getAsJsonArray().get(0).getAsString());
      }
      if (value.isJsonObject()) {
        // the field is nested object - make recursive call to map nested object fields
        fieldOperations.addAll(generateLineageRecord(recordNode.getKey(), recordNode.getValue()));
        outputFields.add(recordNode.getKey());
      }
    }
    String description = String.format("Fields “%s” combined to form a record “%s”",
                                       inputFields.stream().collect(Collectors.joining(", ")),
                                       String.format("%s{%s}", recordName,
                                                     outputFields.stream().collect(Collectors.joining(", ")))
    );
    fieldOperations.add(new FieldTransformOperation(recordName, description, inputFields, outputFields));
    return fieldOperations;
  }
}
