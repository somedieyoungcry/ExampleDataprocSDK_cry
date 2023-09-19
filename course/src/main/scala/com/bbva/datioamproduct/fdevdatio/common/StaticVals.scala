package com.bbva.datioamproduct.fdevdatio.common

object StaticVals {
  case object CourseConfigConstants{
    val RootTag: String = "courseJob"
    val InputTag: String = s"$RootTag.input"

    val ClubPlayersTag: String = "fdevClubPlayers"
    val ClubTeamsTag: String = "fdevClubTeams"
    val NationalPlayersTag: String = "fdevNationalPlayers"
    val NationalTeamsTag: String = "fdevNationalTeams"
    val NationalitiesTag: String = "fdevNationalities"
    val PlayersTag: String = "fdevPlayers"

    val CutOffDateTag: String = "cutoffDate"

  }

  case class Player(name: String, overall: Int) extends Serializable

  case object CourseInt{

    val Zero:Int = 0
    val TwoH: Int = 200
    val OneHEF: Int = 185
    val OneHSF: Int = 175
    val OneHSixF: Int = 165

  }

  case object AlphabeticConstants {

    val AChar: Char = 'A'
    val BChar: Char = 'B'
    val CChar: Char = 'C'
    val DChar: Char = 'D'
    val EChar: Char = 'E'

  }

  case object JoinTypes {
    val LeftJoin: String = "left"
    val OuterJoin: String = "outer"
    val InnerJoin: String = "inner"
    val LeftOuter: String = "left_outer"
    val LeftAnti: String = "left_anti"
    val LeftSemi: String = "left_semi"

  }

  case object SeparateValues {
    val Comma: String = ","
  }

  case object FilterValues {
    val EnglishPremierLeague: String = "English Premier League"
    val ST: String = "ST"
    val MexicanLigueMX: String = "Mexican Liga MX"
    val SofifaFilterValue: Int = 189596
    val Mexico: String = "Mexico"
    val ClubNecaxa: String = "Club Necaxa"
  }

}
