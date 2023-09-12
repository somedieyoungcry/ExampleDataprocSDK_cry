package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ClubPlayersTag, InputTag, NationalPlayersTag, NationalTeamsTag, NationalitiesTag, PlayersTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes.{LeftAnti, LeftJoin}
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit}
import com.bbva.datioamproduct.fdevdatio.common.fields.{CatHeight, ClubTeamId, LongName, NationTeamId, NationalityId, Overall, PlayerPositions, Potential, ShortName, SofifaId}

import scala.collection.convert.ImplicitConversions.`set asScala`

package object transformations {
  implicit class TransformationDF(df: DataFrame) {
    def getMinContractYear(): Int = {
      df.rdd
        .map(row => row.getInt(6))
        .reduce((a, b) => if (a > b) b else a)
    }

    def filterMinContractYear: DataFrame = {
      val maxContractYear = df.getMinContractYear
      df.filter(col("club_contract_valid_until") === maxContractYear)
    }
  }

  implicit class MaxDF(df: DataFrame) {
    def getMaxLeagueLevel(): Int = {
      df.rdd
        .map(row => row.getInt(3))
        .reduce((a, b) => if (a > b) a else b)
    }

    def filterMaxLeagueLevel: DataFrame = {
      val maxLevel = df.getMaxLeagueLevel
      df.filter(col("league_level") === maxLevel)
    }
  }

  implicit class CustomTransformations(df: DataFrame){
    def magicMethod: DataFrame = {
      df.select(
        df.columns.map{
          case name: String if name == "club_jersey_number" => lit(3).alias(name)
          case _@name => col(name)
        }:_*
      )
    }
  }

  implicit class MapToDataFrame(dfMap: Map[String, DataFrame]){
    def getFullDF: DataFrame = {
      dfMap(PlayersTag)
        .join(dfMap(ClubPlayersTag), Seq(SofifaId.name, ClubTeamId.name), LeftAnti)
        .join(dfMap(NationalPlayersTag), Seq(NationTeamId.name, SofifaId.name), LeftJoin)
        .join(dfMap(NationalTeamsTag), Seq(NationTeamId.name), LeftJoin)
    }
  }
}
