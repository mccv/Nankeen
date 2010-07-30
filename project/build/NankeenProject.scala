import sbt._
import Process._


class NankeenProject(info: ProjectInfo) extends DefaultProject(info) {
  // Maven repositories
  val scalaToolsTesting = "testing.scala-tools.org" at "http://scala-tools.org/repo-releases/"
  val powerMock = "powermock-api" at "http://powermock.googlecode.com/svn/repo/"
  val mavenDotOrg = "repo1" at "http://repo1.maven.org/maven2/"
  val scalaToolsReleases = "scala-tools.org" at "http://scala-tools.org/repo-releases/"
  val reucon = "reucon" at "http://maven.reucon.com/public/"
  val lagDotNet = "lag.net" at "http://www.lag.net/repo/"
  val oauthDotNet = "oauth.net" at "http://oauth.googlecode.com/svn/code/maven"
  val javaDotNet = "download.java.net" at "http://download.java.net/maven/2/"
  val jBoss = "jboss-repo" at "http://repository.jboss.org/maven2/"
  val nest = "nest" at "http://www.lag.net/nest/"

  // dependencies
  val jackhammer = "com.twitter" % "jackhammer_2.7.7" % "1.0"
  val smile = "net.lag" % "smile" % "0.8.12"

  lazy val runLoad = task { args =>
    if (args.length < 5) {
      task { Some("usage: <queue name> <num queues> <readers> <writers> <messages>")}
    } else {
      runTask(Some("com.twitter.nankeen.Nankeen"), testClasspath, args)
    }
  } 
}
