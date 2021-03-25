import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws

//args[0] - input filepath
//args[1] - output folder
//args[2] - type report

import RFM_CONSTANTS._
import CONSTANTS.{TRANSIT,CONCAT_SYMBOL}

object RFM_chain {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Reform Chain").getOrCreate()
    import spark.implicits._

    val reformChain_udf      = spark.udf.register("reformChain_udf",reformChain)
    val getChannelR_udf      = spark.udf.register("getChannelR_udf",getChannelR)
    val getTimelineR_udf     = spark.udf.register("getTimelineR_udf",getTimelineR)

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

    val data_seq = data.
      withColumn("channels",split($"user_path",TRANSIT)).
      withColumn("date_touch",split($"timeline",TRANSIT)).
      withColumn("touch_data",arrays_zip($"channels",$"date_touch"))

    val data_reform = data_seq.
      withColumn("touch_dataR", reformChain_udf($"user_path",lit(CONCAT_SYMBOL),lit(CLICK), lit(SESSION)))



    val data_seqR = data_reform.
      withColumn("user_pathR_seq",getChannelR_udf($"touch_dataR")).
      withColumn("timelineR_seq",getTimelineR_udf($"touch_dataR"))

    val data_result = data_seqR.
      withColumn("user_pathR",concat_ws(TRANSIT,$"user_pathR_seq")).
      withColumn("timelineR",concat_ws(TRANSIT,$"timelineR_seq")).
      select(
        $"ClientID",
        $"user_pathR",
        $"timelineR"
      )

    type_report match {
      case "detail" => {
        data_result.write.format("csv").option("header", "true").mode("overwrite").save(args(1))
      }
      case "agg"   => {
        data_result.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(args(1))
      }
      case e@_     => throw new Exception(s"$e report type  does not exist")
    }

  }
}

