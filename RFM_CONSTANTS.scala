object RFM_CONSTANTS {

  val reformChain = (ch_hts:Seq[Seq[String]],
                     utm_sep:String,
                     alert_symbol:String,
                     session_symbol:String) => {

    val alert_seq = alert_symbol + utm_sep
    val arr = ch_hts.foldLeft(List.empty[Map[String,String]]) {
      case (acc,i) if acc.isEmpty                                                               => List(Map(i(0) -> i(1)))
      case (acc,i) if acc.last.keys.head.startsWith("click_>>_") & i(0).startsWith("session")   => acc
      case (acc,i) if acc.last.keys.head.startsWith("click_>>_") & i(0).startsWith("click_>>_") => acc
      case (acc,i)                                                                              => acc :+ Map(i(0) -> i(1))
    }
//    val reform_chain:String = arr.mkString(ch_sep)
    arr
  }

  val getChannelR = (ch_hts:Seq[Map[String,String]]) => {
    val channel_seq:Seq[String] = ch_hts.map(_.keys.head)
    channel_seq

  }

  val getTimelineR = (ch_hts:Seq[Map[String,String]]) => {
    val timeline:Seq[String]     = ch_hts.map(_.values.head)
    timeline
  }



  val CLICK:String     = "click"
  val VIEW:String      = "view"
  val SESSION:String   = "session"
  val USER_PATH:String = "user_path"
}