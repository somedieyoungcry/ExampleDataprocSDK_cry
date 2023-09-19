package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ClubPlayersTag, ClubTeamsTag, InputTag, NationalPlayersTag, NationalTeamsTag, NationalitiesTag, PlayersTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.FilterValues.{EnglishPremierLeague, MexicanLigueMX, ST}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes.{LeftAnti, LeftJoin}
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit}
import com.bbva.datioamproduct.fdevdatio.common.fields.{CatHeight, ClubName, ClubTeamId, CountByPlayersPositions, ExplodePlayerPosition, LeagueName, LeagueNameOverall, LongName, NationTeamId, NationalityId, NationalityName, Overall, OverallByNationality, PlayerByNationality, Potential, ShortName, SofifaId}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.convert.ImplicitConversions.`set asScala`

package object transformations {
  case class ReplaceColumnException(message: String, columnName: String, columns: Array[String]) extends Exception(message)
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit class TransformationDF(df: DataFrame) {
    def getMinContractYear: Int = {
      df.rdd
        .map(row => row.getInt(6))
        .reduce((a, b) => if (a > b) b else a)
    }

    def filterMinContractYear: DataFrame = {
      val maxContractYear = df.getMinContractYear
      df.filter(col("club_contract_valid_until") === maxContractYear)
    }

    def aggregateExample: DataFrame = {
      df
        .groupBy(NationalityName.column)
        .agg(OverallByNationality(), PlayerByNationality())
        .orderBy(PlayerByNationality.column.desc)
    }

    @throws[Exception]
    def addColumn(newColumn: Column): DataFrame = {
      try {
        val columns: Array[Column] = df.columns.map(col) :+ newColumn
        df.select(columns: _*)
      } catch {
        case exception: Exception => throw exception
      }
    }


    @throws[ReplaceColumnException]
    def replaceColumn(field: Column): DataFrame = {
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name

      if (df.columns.contains(columnName)) {
        val columns: Array[Column] = df.columns.map {

          case column: String if column == columnName => field
          case _@column => col(column)

        }.toArray

        df.select(columns: _*)
      } else {
        val message: String = s"La columna $columnName, no puede ser remplazada}."

        throw new ReplaceColumnException(message, columnName, df.columns)

      }
    }

    def agregatePlayerPositions: DataFrame = {
      df.groupBy(ExplodePlayerPosition.column)
        .agg(CountByPlayersPositions())
    }

    def aggregateAverageLeague: DataFrame = {
      df.groupBy(ExplodePlayerPosition.column, LeagueName.column)
        .agg(LeagueNameOverall())
        .where(LeagueName.column === EnglishPremierLeague && ExplodePlayerPosition.column === ST)
    }

    def aggregateBestAverageMXLeague: DataFrame = {
      df.groupBy(ClubName.column, ExplodePlayerPosition.column, LeagueName.column)
        .agg(LeagueNameOverall())
        .where(LeagueName.column === MexicanLigueMX)
        .orderBy(LeagueNameOverall.column.desc)
    }
  }

  implicit class MaxDF(df: DataFrame) {
    def getMaxLeagueLevel: Int = {
      df.rdd
        .map(row => row.getInt(3))
        .reduce((a, b) => if (a > b) a else b)
    }

    def filterMaxLeagueLevel: DataFrame = {
      val maxLevel = df.getMaxLeagueLevel
      df.filter(col("league_level") === maxLevel)
    }
  }


  def replaceColumn(field: Column, df: DataFrame): DataFrame = {
    val columnName: String = field.expr.asInstanceOf[NamedExpression].name

    if(df.columns.contains(columnName)) {
      val columns: Array[Column] = df.columns.map {
        case name: String if name == columnName => field
        case _@name => col(name)
      }
      df.select(columns: _*)
    } else {
      logger.error("Error")
    }
    throw new Exception("La columna no puede ser sustituida")
  }

  implicit class MapToDataFrame(dfMap: Map[String, DataFrame]) {
    case class JoinException(expectedKeys: Array[String],
                             columns: Array[String],
                             location: String = "com.bbva.datioamproduct.fdevdatio.MapException.getFullDF",
                             message: String)
      extends Exception(message)

    @throws[JoinException]
    def getFullDF: DataFrame = {
      val playersKeys: Set[String] = Set(SofifaId.name, ClubTeamId.name, NationTeamId.name)
      val clubTeamsKeys: Set[String] = Set(SofifaId.name, ClubTeamId.name)
      val nationalPlayersKeys: Set[String] = Set(SofifaId.name, NationTeamId.name)
      val nationalTeamsKeys: Set[String] = Set(NationTeamId.name)

      if (!playersKeys.subsetOf(dfMap(PlayersTag).columns.toSet)) {
        val message = s"No se encontraron todas las llaves esperadas en el DataFrame $PlayersTag."
        throw JoinException(playersKeys.toArray, dfMap(PlayersTag).columns, location = "getFullDF", message)

      } else if (!clubTeamsKeys.subsetOf(dfMap(ClubTeamsTag).columns.toSet)) {
        val message = s"No se encontraron todas las llaves esperadas en el DataFrame $ClubTeamsTag."
        throw JoinException(clubTeamsKeys.toArray, dfMap(ClubTeamsTag).columns, location = "getFullDF", message)

      } else if (!nationalPlayersKeys.subsetOf(dfMap(NationalPlayersTag).columns.toSet)) {
        val message = s"No se encontraron todas las llaves esperadas en el DataFrame $NationalPlayersTag."
        throw JoinException(nationalPlayersKeys.toArray, dfMap(NationalPlayersTag).columns, location = "getFullDF", message)

      } else if (!nationalTeamsKeys.subsetOf(dfMap(NationalTeamsTag).columns.toSet)) {
        val message = s"No se encontraron todas las llaves esperadas en el DataFrame $NationalTeamsTag."
        throw JoinException(nationalTeamsKeys.toArray, dfMap(NationalTeamsTag).columns, location = "getFullDF", message)

      } else {
        dfMap(PlayersTag)
          .join(dfMap(ClubPlayersTag), Seq(SofifaId.name, ClubTeamId.name), LeftJoin)
          .join(dfMap(ClubTeamsTag), Seq(ClubTeamId.name), LeftJoin)
          .join(dfMap(NationalPlayersTag), Seq(NationTeamId.name, SofifaId.name), LeftJoin)
          .join(dfMap(NationalitiesTag), Seq(NationTeamId.name), LeftJoin)
      }
    }
  }

  implicit class MapDF(dfMap: Map[String, DataFrame]) {
    def getAllDF: DataFrame = {
      dfMap(PlayersTag)
        .join(dfMap(ClubPlayersTag), Seq(SofifaId.name, ClubTeamId.name), LeftJoin)
        .join(dfMap(ClubTeamsTag), Seq(ClubTeamId.name), LeftJoin)
        .join(dfMap(NationalPlayersTag), Seq(NationTeamId.name, SofifaId.name), LeftJoin)
        .join(dfMap(NationalitiesTag), Seq(NationalityId.name), LeftJoin)
    }
  }
}



