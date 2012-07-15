package util

import collection.generic.CanBuildFrom

object Unfoldable{
  implicit def anyToUnfoldable[T](t:T) = new Unfoldable(t)
}
class Unfoldable[T](x: T){
  def unfoldLeft[B, That](stop:T)(f:T=>(B, T))
  (implicit bf: CanBuildFrom[Nothing, B, That]): That = {
    val b = bf()
    def unfold(v:T){
        if(v != stop) {
          val (nx, r) = f(v)
          unfold(r)
          b+=nx
        }
    }
    unfold(x)
    b.result
  }
}