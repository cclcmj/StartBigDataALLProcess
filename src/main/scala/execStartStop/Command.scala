package execStartStop

import scala.sys.process._
import cats.effect.IO
import cats.effect.Timer
import scala.annotation.tailrec
import scala.concurrent.duration._

case class Command(
    action: String,
    serviceProcessName: String,
    p: ProcessBuilder
) {
  import Command._
  import main.Exec._
  def execAndResult: IO[String] = exec.flatMap(_ => polledverify)
  def exec: IO[Unit] = IO { p.run() }
  def verifyExec: IO[Boolean] =
    IO { "jps".!! }.map(str => verifyjudge(str, action))
  def verifyjudge(str: String, action: String): Boolean =
    ((
      serviceProcessName
        .split(" ")
        .toList
        .map(n => str.contains(n))
        .reduce(_ && _),
      action
    ): @unchecked) match {
      case (true, "start") | (false, "stop") => true
      case (true, "stop") | (false, "start") => false
    }
  def polledverify(implicit timer: Timer[IO]): IO[String] = {
    val startTimeIO = timer.clock.monotonic(MILLISECONDS)
    def essAcc(action: String): IO[String] = {
      for {
        _ <- timer.sleep(5.seconds)
        b <- verifyExec
        start <- startTimeIO
        stop <- timer.clock.monotonic(MILLISECONDS)
        result <-
          if (b)
            IO.pure(serviceProcessName + " " + action + s"${stop - start} s")
          else if ((stop - start) > 7)
            IO.raiseError[String](
              new Exception(s"$serviceProcessName 执行超时" + s"${stop - start} s")
            )
          else essAcc(action)
      } yield result
    }
    essAcc(action)
  }
}
object Command {
  def parse(args: List[String]): List[Command] = {
    val cmds = services
      .filter(s =>
        args(1) match {
          case "all"       => true
          case serviceName => s.name == serviceName
        }
      )
      .flatMap(ser => ser.cmd(args(0)))
    (args(0): @unchecked) match {
      case "start" => cmds
      case "stop"  => cmds.reverse
    }
  }
  val services = List(
    Service(
      "hadoop",
      "sbin",
      List(
        ServiceProcess(
          "action-dfs.sh",
          "",
          "NameNode SecondaryNameNode DataNode"
        )
      )
    ),
    Service(
      "hive",
      "metastore-derby",
      List(
        ServiceProcess(
          "actionNetworkServer",
          "-h hive",
          "NetworkServerControl"
        ),
        ServiceProcess("hive", "--service metastore", "RunJar")
      )
    ),
    Service(
      "spark",
      "sbin",
      List(
        ServiceProcess("action-master.sh", "", "Master"),
        ServiceProcess("action-slave.sh", "spark://hive:7077", "Worker"),
        ServiceProcess("action-history-server.sh", "", "HistoryServer")
      )
    ),
    Service(
      "kafka",
      "bin",
      List(
        ServiceProcess(
          "zookeeper-server-action.sh",
          "../config/zookeeper.properties",
          "QuorumPeerMain"
        ),
        ServiceProcess(
          "kafka-server-action.sh",
          "../config/server.properties",
          "Kafka"
        )
      )
    )
  )
}