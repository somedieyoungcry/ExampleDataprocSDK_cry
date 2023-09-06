package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ClubPlayersTag, InputTag, NationalitiesTag, PlayersTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Player
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseInt.Zero
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

class CourseSparkProcess extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("Hi, estoy desde mi nuevo modulo de dataproc sdk")
    val config: Config = runtimeContext.getConfig

    val dfMap: Map[String, DataFrame] = config.readInputs

    dfMap(PlayersTag).show()

    val df: Dataset[Row] = dfMap(PlayersTag)
      .select(
        col("skill_ball_control")
      )

    //df.show()

    /*val rddOne: RDD[String] = df
          .rdd
          .map(_.getString(0).replace(" ", ""))
          .flatMap(record => record.split(","))

        val rddTwo: RDD[(String, Int)] = rddOne
          .map(record => {
            (record, 1)
          }).reduceByKey((a, b) => {
          a + b
        })

        rddTwo.foreach(
          record => println(record)
        )*/

    // AREA PARA HACER MIS EJERCICIOS

    val player: Player = dfMap(PlayersTag).rdd
      .map(row => Player(row.getString(2), row.getInt(4)))
      .reduce((a, b) => {
        if (a.overall > b.overall) a else b
      })

    print(player)




    /*val rdd: RDD[String] = dfMap(PlayersTag)
      .rdd
      .map(row => row.getString(22))
      .filter(_ != null)

    val traitCounts = rdd
      .map(traitName => (traitName, 1))
      .reduceByKey(_ + _)

    val minTrait = traitCounts
      .reduce((trait1, trait2) => if (trait1._2 < trait2._2) trait1 else trait2)

    println(s"El trait con el menor número de jugadores es '${minTrait._1}' con ${minTrait._2} jugadores.")

    val nationalityCounts = df.groupBy("nationality_id").count()
    val mexicanPlayerCount = nationalityCounts.filter(col("nationality_id") === 83).select("count").first().getLong(0)
    println(s"El número de jugadores mexicanos es: $mexicanPlayerCount")
    nationalityCounts.show()*/

    Zero
  }
  override def getProcessId: String = "CourseSparkProcess"

}
