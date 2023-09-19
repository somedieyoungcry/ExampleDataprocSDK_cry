package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ClubPlayersTag, ClubTeamsTag, InputTag, NationalPlayersTag, NationalTeamsTag, NationalitiesTag, PlayersTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseInt.Zero
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.FilterValues.{ClubNecaxa, Mexico, SofifaFilterValue}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes.{InnerJoin, LeftAnti, LeftJoin, LeftOuter, LeftSemi, OuterJoin}
import com.bbva.datioamproduct.fdevdatio.common.fields.{Age, CatAgeOverall, CatHeight, ClubName, ClubTeamId, ExplodePlayerPosition, LongName, MeanOverall, NationTeamId, NationalityId, NationalityName, Overall, PlayerPositions, Potential, ShortName, SofifaId, StdDevOverall, SumOverall, ZScore}
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


    val AllDF: DataFrame = dfMap.getAllDF.replaceColumn(PlayerPositions())

    AllDF
      .select(ShortName.column,
        NationalityName.column,
        Age.column,
        Overall.column)
      .addColumn(MeanOverall())
      .addColumn(StdDevOverall())
      .addColumn(CatAgeOverall())
      .addColumn(ZScore())
      .where(NationalityName.column === Mexico)
      .show

    val fDF: DataFrame = dfMap
      .getAllDF
      .replaceColumn(PlayerPositions())
      .addColumn(CatAgeOverall())
      .addColumn(SumOverall())

    fDF
      .where(ClubName.column === ClubNecaxa)
      .select(ShortName.column, ClubName.column, CatAgeOverall.column, Overall.column, SumOverall.column)
      .show(false)

    Zero
  }
  override def getProcessId: String = "CourseSparkProcess"

}
