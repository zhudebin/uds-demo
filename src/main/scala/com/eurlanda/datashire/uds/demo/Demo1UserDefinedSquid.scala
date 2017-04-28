package com.eurlanda.datashire.uds.demo

import com.eurlanda.datashire.engine.ud.UserDefinedSquid
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by zhudebin on 2017/4/19.
  */
class Demo1UserDefinedSquid() extends UserDefinedSquid {
  /**
    * 输入的dataset的schema
    *
    * @return
    */
  override val inputSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("depId", IntegerType)

  /**
    * 输出dataset的schema
    *
    * @return
    */
  override val outSchema: StructType = new StructType()
    .add("id", IntegerType)
    .add("depId", IntegerType)
    .add("name", StringType)

  override def process(dataset: Dataset[Row]): Dataset[Row] = {
    val isvalid = dataset.schema.equals(inputSchema)
    println("------------" + isvalid)
//    if(!isvalid) {
//      throw new RuntimeException("schema不匹配")
//    }
    dataset.schema.printTreeString()
    println("---------------------")
    val schema = dataset.schema
    dataset.map(r => {
      val id = r.getInt(schema.fieldIndex("id"))
      val depId = r.getInt(schema.fieldIndex("depId"))

      Row(id, depId, "name_" + id + params.get("split") + depId)
//      new GenericRowWithSchema(Array[Any](id, depId, "name_" + id + params.get("split") + depId), outSchema).asInstanceOf[Row]
    })(RowEncoder(outSchema))
  }

}
