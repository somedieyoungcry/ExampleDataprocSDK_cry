package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ClubPlayersTag, ClubTeamsTag, InputTag, NationalPlayersTag, NationalTeamsTag, NationalitiesTag, PlayersTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseInt.Zero
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes.{InnerJoin, LeftAnti, LeftJoin, LeftOuter, LeftSemi, OuterJoin}
import com.bbva.datioamproduct.fdevdatio.common.fields.{CatAge, CatHeight, ClubTeamId, LongName, NationTeamId, NationalityId, Overall, Potential, ShortName, SofifaId}
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.slf4j.{Logger, LoggerFactory}
import com.bbva.datioamproduct.fdevdatio.transformations._
import org.apache.spark.sql.functions.{avg, col, count}

class CourseSparkProcess extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("Hi, estoy desde mi nuevo modulo de dataproc sdk")
    val config: Config = runtimeContext.getConfig
    // val params: Params = config.getParams

    val dfMap: Map[String, DataFrame] = config.readInputs

   """ dfMap(ClubPlayersTag)
      .filterMinContractYear
      .show()"""

    """dfMap(ClubPlayersTag)
      .magicMethod
      .show(100)

    val aggVal: Double = dfMap(PlayersTag)
      .withColumn(CatHeight.name, CatHeight())
      .filter(col(CatHeight.name) === "D").agg(avg("height_cm")).collect()(0).getDouble(0)

    print(aggVal)"""

    // SECCION DE JOINS
    """val dfA: DataFrame = dfMap(PlayersTag)
    val dfB: DataFrame = dfMap(ClubPlayersTag)
    val dfC: DataFrame = dfMap(NationalPlayersTag)
    val dfD: DataFrame = dfMap(ClubTeamsTag)
    val dfE: DataFrame = dfMap(NationalitiesTag)
    val dfF: DataFrame = dfMap(NationalTeamsTag)


    val resultDF: DataFrame = dfA.join(dfB, Seq(SofifaId.name, ClubTeamId.name), LeftJoin)
    // resultDF.show()

    val resultDF2: DataFrame = dfA.join(dfB, dfA(SofifaId.name) === dfB(SofifaId.name) &&
      dfA(ClubTeamId.name) === dfB(ClubTeamId.name), LeftJoin)
    // resultDF2.show()

    val resultA: DataFrame = dfMap(ClubPlayersTag)
      .join(dfMap(NationalPlayersTag), Seq(SofifaId.name), OuterJoin)

    val resultB: DataFrame = dfB.join(dfC, Seq(SofifaId.name), InnerJoin)
    //  print(">>> ", resultB.count())

    val resultC: DataFrame = dfC.join(dfB, Seq(SofifaId.name), LeftAnti)

    val resultD: DataFrame = dfC.join(dfB, Seq(SofifaId.name), OuterJoin)
      .where(SofifaId.column isin(211592, 198479, 223074, 239952, 243009))


    val result: DataFrame = dfA
      .join(dfB, Seq(SofifaId.name), LeftAnti)
      .join(dfC, Seq(SofifaId.name), LeftSemi)
    result.show()"""

    val AllDF: DataFrame = dfMap.getAllDF
    AllDF
      .aggPlayersPositions
      .show


    Zero
  }
  override def getProcessId: String = "CourseSparkProcess"

}
