import sbt._
import com.twitter.sbt.StandardProject

class NankeenProject(info: ProjectInfo) extends StandardProject(info) {
  val twitterRepo = "Twitter Repo" at "http://binaries.local.twitter.com/maven/"
  // dependencies
  val jackhammer = "com.twitter" % "jackhammer_2.7.7" % "1.0"
  val smile = "net.lag" % "smile" % "0.8.12"
  val grabby = "com.twitter" % "grabbyhands" % "1.0"

  override def mainClass = Some("com.twitter.nankeen.Nankeen")
}
