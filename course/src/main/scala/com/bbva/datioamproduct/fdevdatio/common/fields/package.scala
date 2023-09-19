package com.bbva.datioamproduct.fdevdatio.common

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.AlphabeticConstants.{AChar, BChar, CChar, DChar, EChar}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseInt.{OneHEF, OneHSF, OneHSixF, TwoH}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.SeparateValues.Comma
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{avg, collect_list, count, explode, mean, regexp_replace, split, stddev, trim, when}

package object fields {

  case object Age extends Field {
    override val name: String = "age"
  }

  case object HeightCM extends Field {
    override val name: String = "height_cm"
  }

  case object SofifaId extends Field {
    override val name: String = "sofifa_id"
  }

  case object ClubTeamId extends Field {
    override val name: String = "club_team_id"
  }

  case object ClubName extends Field {
    override val name: String = "club_name"
  }

  case object NationTeamId extends Field {
    override val name: String = "nation_team_id"
  }

  case object ShortName extends Field {
    override val name: String = "short_name"
  }

  case object LongName extends Field {
    override val name: String = "long_name"
  }

  case object Overall extends Field {
    override val name: String = "overall"
  }

  case object Potential extends Field {
    override val name: String = "potential"
  }

  case object LeagueName extends Field {
    override val name: String = "league_name"
  }

  case object NationalityId extends Field {
    override val name: String = "nationality_id"
  }

  case object NationalityName extends Field {
    override val name: String = "nationality_name"
  }

  case object PlayerByNationality extends Field {
    override val name: String = "players_by_nationality"
    def apply(): Column = count(Overall.column) alias name
  }

  case object OverallByNationality extends Field {
    override val name: String = "overall_by_nationality"

    def apply(): Column = avg(Overall.column) alias name
  }

  case object StdDevOverall extends Field{
    override val name: String = "std_dev_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column)

      stddev(Overall.column).over(w).alias(name)
    }
  }

  case object MeanOverall extends Field {
    override val name: String = "mean_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column)

      mean(Overall.column).over(w).alias(name)
    }
  }
  case object ZScore extends Field{
    override val name: String = "z_score"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(NationalityName.column, CatAgeOverall.column)
      ((Overall.column - MeanOverall.column) / StdDevOverall.column) alias name
    }
  }

  case object SumOverall extends Field {
    override val name: String = "sum_overall"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(ClubName.column, CatAgeOverall.column)
      collect_list(Overall.column) over w alias name
    }
  }


  case object PlayerPositions extends Field {
    override val name: String = "player_positions"

    def apply(): Column = {
      split(regexp_replace(column, " ", ""), Comma) alias name
    }
  }

  case object ExplodePlayerPosition extends Field {
    override val name: String = "explode_player_positions"

    def apply(): Column = explode(PlayerPositions.column) alias name

  }

  case object CountByPlayersPositions extends Field {
    override val name: String = "count_by_position"

    def apply(): Column = count(ExplodePlayerPosition.column) alias name
  }

  case object LeagueNameOverall extends Field {
    override val name: String = "avg_overall_league_name"
    def apply(): Column = avg(Overall.column) alias name

  }

    case object CatAgeOverall extends Field{
    override val name: String = "cat_age"
    def apply(): Column = {
      when(Age.column <= 20 || Overall.column > 80, AChar)
        .when(Age.column <= 23 || Overall.column > 70, BChar)
        .when(Age.column <= 30, CChar)
        .otherwise(DChar) alias name
    }
  }

  case object CatHeight extends Field {
    override val name: String = "cat_height"

    def apply(): Column = {
      when(HeightCM.column > TwoH, AChar)
        .when(HeightCM.column >= OneHEF, BChar)
        .when(HeightCM.column > OneHSF, CChar)
        .when(HeightCM.column >= OneHSixF, DChar)
        .otherwise(EChar) alias name
    }
  }

  """case object ShortName extends Field {
    override val name: String = "short_name"
    def apply(): Column = lit("NewColumnBABY") alias name
  }

  object YourUtilityClass {
    def replaceColumn(df: DataFrame, field: Field): DataFrame = {
      val columnName: String = field.name
      val newColumn: Column = field()

      val columnsToSelect: Seq[Column] = df.columns.map { colName =>
        if (colName == columnName) newColumn else col(colName)
      }

      df.select(columnsToSelect: _*)
    }
  }"""
}




