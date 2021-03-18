object RFM_CONSTANTS {
  val reformChain = (chain:String,ch_sep:String,utm_sep:String,sp_symbol:String) => {
    val delimiter = sp_symbol + utm_sep
    val ch_list   = chain.split(ch_sep)
    val arr       = ch_list.foldLeft(List.empty[String]) {
      case (acc,i) if acc.isEmpty                                               => List(i)
      case (acc,i) if acc.last.startsWith(delimiter) & !i.startsWith(delimiter) => acc
      case (acc,i)                                                              => acc :+ i
    }
    val reform_chain:String = arr.mkString(ch_sep)
    reform_chain
  }

  val CLICK:String     = "click"
  val VIEW:String      = "view"
  val USER_PATH:String = "user_path"
}