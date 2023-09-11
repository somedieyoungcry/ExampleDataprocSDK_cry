package com.bbva.datioamproduct.fdevdatio.common

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.AlphabeticConstants.{AChar, BChar, CChar, DChar, EChar}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseInt.{OneHEF, OneHSF, OneHSixF, TwoH}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, when}

package object fields {

  case object HeightCM extends Field {
    override val name: String = "height_cm"
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

  case object ShortName extends Field {
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
  }
}




