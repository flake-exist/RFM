import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

//args[0] - input filepath
//args[1] - output folder
//args[2] - type report

import RFM_CONSTANTS._
import CONSTANTS.{TRANSIT,CONCAT_SYMBOL}

object RFM_chain {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Reform Chain").getOrCreate()
    import spark.implicits._

        val reformChain_udf = spark.udf.register("reformChain_udf",reformChain)
        val type_report:String = args(2) match {
          case a@"detail" => a
          case b@"agg"    => b
          case e@_        => throw new Exception(s"$e mode does not exist")
        }

        val data = spark.read.
          format("csv").
          option("inferSchema","false").
          option("header","true").
          option("mergeSchema","true").
          load(args(0))

        data.show(20)

        val data_0 = data.
          withColumn("user_pathR", reformChain_udf(col(USER_PATH), lit(TRANSIT), lit(CONCAT_SYMBOL), lit(CLICK)))

        type_report match {
          case "detail" => {
            data_0.write.format("csv").option("header", "true").mode("overwrite").save(args(1))
          }
          case "agg"   => {
            data_0.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(args(1))
          }
          case e@_     => throw new Exception(s"$e report type  does not exist")
        }

  }
}