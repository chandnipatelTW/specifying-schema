package schema

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class SpecifyingSchemaTest  extends QueryTest with SharedSQLContext{
  test("should return no schema") {
    val noSchema = "StructType(StructField(prev_id,StringType,true), StructField(curr_id,StringType,true), StructField(n,StringType,true), StructField(prev_title,StringType,true), StructField(curr_title,StringType,true), StructField(type,StringType,true))"
    assert(SpecifyingSchema.setNoSchema() === noSchema)
  }

  test("should return inferred schema") {
    val inferSchema = "StructType(StructField(prev_id,IntegerType,true), StructField(curr_id,IntegerType,true), StructField(n,IntegerType,true), StructField(prev_title,StringType,true), StructField(curr_title,StringType,true), StructField(type,StringType,true))"
    assert(SpecifyingSchema.setInferSchema() === inferSchema)
  }

  test("should return explicit full schema") {
    val explicitSchema = "StructType(StructField(previous_id,IntegerType,true), StructField(curr_id,IntegerType,true), StructField(n,IntegerType,true), StructField(prev_title,StringType,true), StructField(curr_title,StringType,true), StructField(type,StringType,true))"
    assert(SpecifyingSchema.setExplicitSchema() === explicitSchema)
  }

  test("should return missing field schema") {
    val missingFieldSchema = "StructType(StructField(prev_id,IntegerType,true), StructField(n,IntegerType,true), StructField(prev_title,StringType,true), StructField(curr_title,StringType,true), StructField(type,StringType,true))"
    assert(SpecifyingSchema.setMissingFieldSchema() === missingFieldSchema)
  }

  test("should return schema with errors") {
    val errorSchema = "StructType(StructField(prev_id,IntegerType,true), StructField(curr_id,IntegerType,true), StructField(n,IntegerType,true), StructField(prev_title,StringType,true), StructField(curr_title,StringType,true), StructField(type,IntegerType,true))"
    assert(SpecifyingSchema.setErrorSchema() === errorSchema)
  }
}
