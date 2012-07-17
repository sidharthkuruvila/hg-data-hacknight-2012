package util

object Util{
  def tryO[T](f: =>T): Option[T] = {
    try{
      Some(f)
    } catch {
      case _ => None
    }
  }
}