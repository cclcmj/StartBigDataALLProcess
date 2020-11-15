
package ammonite
package $file.src.main.resources
import _root_.ammonite.interp.api.InterpBridge.{
  value => interp
}
import _root_.ammonite.interp.api.InterpBridge.value.{
  exit
}
import _root_.ammonite.interp.api.IvyConstructor.{
  ArtifactIdExt,
  GroupIdExt
}
import _root_.ammonite.runtime.tools.{
  browse,
  grep,
  time,
  tail
}
import _root_.ammonite.repl.tools.{
  desugar,
  source
}
import _root_.ammonite.main.Router.{
  doc,
  main
}
import _root_.ammonite.repl.tools.Util.{
  pathScoptRead
}


object execStartStop2{
/*<script>*/import $ivy.$                                   

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
  /**
    * 执行并查询执行结果
    *
    * @return
    */
  def execAndResult: IO[String] = exec.flatMap(_ => polledverify)
  /**
    * 执行processbuilder
    *
    * @return
    */
  private def exec: IO[Unit] = IO { p.run() }
  /**
    * 获取进程信息，并验证动作是否被执行
    *
    * @return
    */
  private def verifyExec: IO[Boolean] =
    IO { "jps".!! }.map(str => verifyjudge(str, action))
  /**
    * 验证动作是否被执行
    *
    * @param str
    * @param action
    * @return
    */
  private def verifyjudge(str: String, action: String): Boolean = {
    val result = serviceProcessName
      .split(" ")
      .toList
      .map(n => str.contains(n))
    (action: @unchecked) match {
      case "start" => result.reduce(_ && _)
      case "stop"  => !result.reduce(_ || _)
    }
  }
  /**
    * 每5秒获取进程信息，如果执行时间超过7秒返回执行超时Exception
    *
    * @param timer
    */
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
  /**
    * 将输入参数解析为List[Commmand]
    *
    * @param args
    * @return
    */
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
  /**
    * List包括进程信息，脚本位置，脚本名称，服务名
    */
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
  /**
    * 将服务信息转换为Command
    *
    * @param action
    * @return
    */
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
/*</script>*/ /*<generated>*/
def $main() = { scala.Iterator[String]() }
  override def toString = "execStartStop2"
  /*</generated>*/
}
