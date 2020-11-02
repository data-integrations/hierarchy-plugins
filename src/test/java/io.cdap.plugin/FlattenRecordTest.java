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
 package io.cdap.plugin;

 import io.cdap.cdap.api.data.format.StructuredRecord;
 import io.cdap.cdap.api.data.schema.Schema;
 import io.cdap.cdap.etl.api.Transform;
 import io.cdap.cdap.etl.api.validation.ValidationException;
 import io.cdap.cdap.etl.mock.common.MockEmitter;
 import io.cdap.cdap.etl.mock.transform.MockTransformContext;
 import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
 import org.junit.Assert;
 import org.junit.Test;

 import java.lang.reflect.Field;

 /**
  * Tests {@link FlattenRecord}.
  */
 public class FlattenRecordTest {

   private static final Schema INPUT_SCHEMA_RECORD_A =
     Schema.recordOf("recordA",
                     Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b", Schema.of(Schema.Type.STRING))
     );

   private static final Schema INPUT_SCHEMA_RECORD_B =
     Schema.recordOf("recordB",
                     Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b", INPUT_SCHEMA_RECORD_A));

   private static final Schema INPUT_SCHEMA_RECORD_B_DUPLICATE =
     Schema.recordOf("recordBDuplicate",
                     Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b", INPUT_SCHEMA_RECORD_A),
                     Schema.Field.of("b_a", Schema.of(Schema.Type.STRING)),
                     Schema.Field.of("b_b", Schema.of(Schema.Type.STRING)));


   private static final Schema INPUT = Schema.recordOf("input",
                                                       Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("c", INPUT_SCHEMA_RECORD_A),
                                                       Schema.Field.of("d", INPUT_SCHEMA_RECORD_B));

   @Test
   public void testWhenNoFieldIsSelected() {
     FlattenRecord.Config config = new FlattenRecord.Config(null, "10");
     Transform<StructuredRecord, StructuredRecord> transform = new FlattenRecord(config);
     MockTransformContext context = new MockTransformContext();
     try {
       transform.initialize(context);
       Assert.assertEquals(1, context.getFailureCollector().getValidationFailures().size());
     } catch (Exception e) {
       Assert.assertEquals(e.getClass(), ValidationException.class);
       Assert.assertEquals(1, ((ValidationException) e).getFailures().size());
     }
   }

   @Test
   public void testSchemaGeneration() throws Exception {
     String fieldsToFlatten = "a,b,c,d";
     FlattenRecord.Config config = new FlattenRecord.Config(fieldsToFlatten, "10");
     Transform<StructuredRecord, StructuredRecord> transform = new FlattenRecord(config);
     MockTransformContext context = new MockTransformContext();
     Field field = FlattenRecord.class.getDeclaredField("failureCollector");
     field.setAccessible(true);
     field.set(transform, context.getFailureCollector());
     MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
     transform.transform(StructuredRecord.builder(INPUT)
                           .set("a", "1")
                           .set("b", "2")
                           .set("c",
                                StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                  .set("a", "a1")
                                  .set("b", "b1").build()
                           )
                           .set("d", StructuredRecord.builder(INPUT_SCHEMA_RECORD_B)
                             .set("a", "a2")
                             .set("b",
                                  StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                    .set("a", "a1")
                                    .set("b", "b1").build()
                             ).build()
                           ).build(), emitter);
     Assert.assertEquals(1, emitter.getEmitted().size());
     Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
     Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
     Assert.assertEquals("a1", emitter.getEmitted().get(0).get("c_a"));
     Assert.assertEquals("a1", emitter.getEmitted().get(0).get("d_b_a"));
     Assert.assertEquals("b1", emitter.getEmitted().get(0).get("c_b"));
     Assert.assertEquals("a2", emitter.getEmitted().get(0).get("d_a"));
     Assert.assertEquals("b1", emitter.getEmitted().get(0).get("d_b_b"));
   }

   @Test
   public void testDuplicateFieldNames() throws Exception {
     Schema schemaB = Schema.recordOf("schemaC", Schema.Field.of("c", Schema.of(Schema.Type.STRING)));
     Schema schemaA = Schema.recordOf("schemaA",
                                      Schema.Field.of("b_c", Schema.of(Schema.Type.STRING)),
                                      Schema.Field.of("b", schemaB));
     Schema inputSchema = Schema.recordOf("input",
                                          Schema.Field.of("a", schemaA),
                                          Schema.Field.of("a_b_c", Schema.of(Schema.Type.STRING)));

     StructuredRecord structuredRecord = StructuredRecord.builder(inputSchema)
       .set("a", StructuredRecord.builder(schemaA)
         .set("b_c", "duplicate1")
         .set("b", StructuredRecord.builder(schemaB)
           .set("c", "duplicate2")
           .build())
         .build())
       .set("a_b_c", "duplicate3")
       .build();
     testInvalidDuplicates("a", structuredRecord);
   }

   @Test
   public void testNestedFieldLimitsReached() throws Exception {
     String fieldsToFlatten = "a,b,c,d";
     FlattenRecord.Config config = new FlattenRecord.Config(fieldsToFlatten, null);
     Transform<StructuredRecord, StructuredRecord> transform = new FlattenRecord(config);
     MockTransformContext context = new MockTransformContext();
     Field field = FlattenRecord.class.getDeclaredField("failureCollector");
     field.setAccessible(true);
     field.set(transform, context.getFailureCollector());
     MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
     transform.transform(StructuredRecord.builder(INPUT)
                           .set("a", "1")
                           .set("b", "2")
                           .set("c",
                                StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                  .set("a", "a1")
                                  .set("b", "b1").build()
                           )
                           .set("d", StructuredRecord.builder(INPUT_SCHEMA_RECORD_B)
                             .set("a", "a2")
                             .set("b",
                                  StructuredRecord.builder(INPUT_SCHEMA_RECORD_A)
                                    .set("a", "a1")
                                    .set("b", "b1").build()
                             ).build()
                           ).build(), emitter);
     Assert.assertEquals(1, emitter.getEmitted().size());
     Assert.assertEquals(6, emitter.getEmitted().get(0).getSchema().getFields().size());
   }

   private void testInvalidDuplicates(String fieldsToFlatten, StructuredRecord input) throws Exception {
     FlattenRecord.Config config = new FlattenRecord.Config(fieldsToFlatten, "10");
     Transform<StructuredRecord, StructuredRecord> transform = new FlattenRecord(config);
     MockTransformContext context = new MockTransformContext();
     Field field = FlattenRecord.class.getDeclaredField("failureCollector");
     field.setAccessible(true);
     field.set(transform, context.getFailureCollector());
     MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
     try {
       transform.transform(input, emitter);
       Assert.fail();
     } catch (Exception e) {
       MockFailureCollector collector = (MockFailureCollector) context.getFailureCollector();
       Assert.assertEquals(1, collector.getValidationFailures().size());
       Assert.assertEquals("Duplicate fields name.", collector.getValidationFailures().get(0).getMessage());
     }
   }
 }
