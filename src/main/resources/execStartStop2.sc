import $ivy.{`org.typelevel::cats-effect:2.2.0`}

import cats.effect.{ExitCode, IO, Concurrent, IOApp, Sync, Resource, Timer}
import java.io.{PrintStream, File}
import scala.collection.immutable.Nil
import scala.sys.process._
import scala.annotation.tailrec
import scala.concurrent.duration._

@main
def main(s1: String, s2: String, path: os.Path = os.pwd) = {
  Exec.main(Array(s1, s2))
}
object Exec extends IOApp {
  implicit val imtimer = super.timer
  override def run(args: List[String]): IO[ExitCode] = {
    for {
      _ <- init(args)
      result <- exec(args)
      _ <- outputStream.use { case out => IO(out.println(result)) }
    } yield ExitCode.Success
  }
  //提示开始并验证参数
  def init(args: List[String]): IO[Unit] = {
    val reg1 = "(start|stop)".r
    val reg2 = "(hadoop|hive|spark|kafka|all)".r
    args match {
      case reg1(_) :: reg2(_) :: Nil => IO.unit
      case _                         => IO.raiseError[Unit](new Exception(s"输入不合法"))
    }
  }
  //Resource
  def outputStream: Resource[IO, PrintStream] =
    Resource.make { IO(Console.out) } { outStream =>
      IO(outStream.close()).handleErrorWith(_ => IO.unit)
    }
  //解析参数为命令,执行命令获得结果,验证结果
  def exec(args: List[String]): IO[String] =
    Command
      .parse(args)
      .map(_.execAndResult)
      .foldLeft(IO.pure(""))((io1, io2) =>
        io1.flatMap(r1 => io2.map(r2 => r1 + "\n" + r2))
      )
}
case class Command(
    action: String,
    serviceProcessName: String,
    p: ProcessBuilder
) {
  import Command._
  import Exec._
  def execAndResult: IO[String] = exec.flatMap(_ => polledverify)
  def exec: IO[Unit] = IO { p.run() }
  def verifyExec: IO[Boolean] =
    IO { "jps".!! }.map(str => verifyjudge(str, action))
  def verifyjudge(str: String, action: String): Boolean = {
    val result = serviceProcessName
      .split(" ")
      .toList
      .map(n => str.contains(n))
    (action: @unchecked) match {
      case "start" => result.reduce(_ && _)
      case "stop"  => !result.reduce(_ || _)
    }
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
final case class Service(
    name: String,
    startSubPath: String,
    sh: List[ServiceProcess]
) {
  def cmd(action: String): List[Command] = {
    sh.map {
      case ServiceProcess(shName, args, processName) =>
        Command(
          action,
          processName,
          Process(
            s"setsid ${if (shName.contains(".sh")) s"./$shName" else shName} $args"
              .replace("action", action),
            new File(s"${sys.env(name.toUpperCase + "_HOME")}/$startSubPath")
          )
        )
    }.filter(c => !(c.action == "stop" && c.serviceProcessName == "RunJar"))
  }
}
final case class ServiceProcess(
    shName: String,
    args: String,
    processName: String
)
