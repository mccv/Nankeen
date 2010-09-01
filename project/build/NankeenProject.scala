import sbt._
import com.twitter.sbt._

class NankeenProject(info: ProjectInfo) extends StandardProject(info) {
  // dependencies
  val jackhammer = "com.twitter" % "jackhammer_2.7.7" % "1.0"
  val smile = "net.lag" % "smile" % "0.8.12"
  override def ivyXML =
    <dependencies>
      <exclude org="apache-log4j"/>
    </dependencies>

  override def mainClass = Some("com.twitter.nankeen.Nankeen")
}
