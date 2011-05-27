

import sbt._
import de.element34.sbteclipsify._

class LiftProject(info: ProjectInfo) extends DefaultWebProject(info) with Eclipsify with AkkaProject
{
  val liftVersion = "2.4-SNAPSHOT"
  
  /**
   * Application dependencies
   */
  val webkit    = "net.liftweb" %% "lift-webkit" % liftVersion % "compile->default" withSources ()
  val logback   = "ch.qos.logback" % "logback-classic" % "0.9.26" % "compile->default" withSources ()
  
  val servlet   = "javax.servlet" % "servlet-api" % "2.5" % "provided->default"
  val jetty7 = "org.eclipse.jetty" % "jetty-webapp" % "8.0.0.M2" % "test"
 
  val junit     = "junit" % "junit" % "4.5" % "test->default" withSources ()
  val specs     = "org.scala-tools.testing" %% "specs" % "1.6.8" % "test->default" withSources ()
  
  
  
  /**
   * Maven repositories
   */
  lazy val scalatoolsSnapshots = ScalaToolsSnapshots
  
  override val jettyPort = 8088
  
  //val nexusRepo = "nexus" at "https://nexus.griddynamics.net/nexus/content/groups/public"
}