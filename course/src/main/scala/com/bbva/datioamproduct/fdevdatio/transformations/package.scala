package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{InputTag, NationalitiesTag}
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions.{col, lit}

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
}
