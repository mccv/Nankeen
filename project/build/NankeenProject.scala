import sbt._
import com.twitter.sbt._

class NankeenProject(info: ProjectInfo) extends StandardProject(info) {
  val twitterRepo = "Twitter Repo" at "http://maven.twttr.com/"
  // dependencies
  val jackhammer = "com.twitter" % "jackhammer_2.7.7" % "1.0"
  val smile = "net.lag" % "smile" % "0.8.12"
  val grabby = "com.twitter" % "grabbyhands" % "1.1"
  override def ivyXML =
    <dependencies>
      <exclude org="apache-log4j"/>
    </dependencies>

  override def mainClass = Some("com.twitter.nankeen.Nankeen")
}
