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

  }

  case class Player(name: String, overall: Int) extends Serializable

  case object CourseInt{

    val Zero:Int = 0
  }
}
