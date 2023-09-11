package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ClubPlayersTag, ClubTeamsTag, InputTag, NationalitiesTag, PlayersTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseInt.Zero
import com.bbva.datioamproduct.fdevdatio.common.fields.{CatHeight}
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.slf4j.{Logger, LoggerFactory}
import com.bbva.datioamproduct.fdevdatio.transformations._
import org.apache.spark.sql.functions.{avg, col}

class CourseSparkProcess extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("Hi, estoy desde mi nuevo modulo de dataproc sdk")
    val config: Config = runtimeContext.getConfig
    // val params: Params = config.getParams

    val dfMap: Map[String, DataFrame] = config.readInputs

    /*dfMap(ClubPlayersTag)
      .filterMinContractYear
      .show()*/

    /*dfMap(ClubPlayersTag)
      .magicMethod
      .show(100)*/

    """val aggVal: Double = dfMap(PlayersTag)
      .withColumn(CatHeight.name, CatHeight())
      .filter(col(CatHeight.name) === "D").agg(avg("height_cm")).collect()(0).getDouble(0)

    print(aggVal)"""





   """ val playersDF: DataFrame = dfMap(PlayersTag)
    val filteredDF: DataFrame = playersDF.filter(col(CatHeight.name) === "D")

    val averageHeight: Double = filteredDF.agg(avg("height_cm")).collect()(0).getDouble(0)

    logger.info(s"Promedio de estatura para jugadores con cat_height=D: $averageHeight")"""


    /*dfMap(ClubTeamsTag)
      .filterMaxLeagueLevel
      .show()*/


    """val cutOffDate: String = config.getString("courseJob.params.cutoffDate")
    val dfOne: DataFrame = dfMap(PlayersTag)
      .filter(col("dob") === cutOffDate)

    val dfTwo: DataFrame = dfOne
      .select(col("dob"))

    dfTwo.show()"""

    // Esto corresponde al vid #6

   """val nationalityID: String = config.getString("courseJob.params.nationalityID")
    val dfOne: DataFrame = dfMap(PlayersTag)
      .filter(col("nationality_id") === nationalityID)

    val dfTwo: DataFrame = dfOne
      .select(col("nationality_id"))

    dfTwo.show()"""

    Zero
  }
  override def getProcessId: String = "CourseSparkProcess"

}
