import $ivy.{`org.typelevel::cats-effect:2.2.0`}
import cats.Monad
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ExitCode, IO, IOApp, Resource, Sync}
import cats.syntax.all._
import scala.reflect.ClassTag
import scala.util.{Try, Failure, Success}
import java.io.{PrintStream, BufferedReader}
import scala.sys.process._
import scala.reflect.runtime.{universe => ru}
import scala.concurrent.ExecutionContext
import java.io.File

@main
def main(s1: String, s2: String, path: os.Path = os.pwd) = {
  Main.main(Array(s1, s2))
}
object Main extends IOApp {
  import ExecAll._
  override def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- IO(println("执行开始"))
      _ <- if(!verifyArgs(args))   IO.raiseError(new Exception(s"${args.mkString(" ")} 输入不合法"))
           else IO.unit
      _ <- start[IO](args)
    } yield ExitCode.Success
}
object ExecAll {
  val resultName = List(
    "Kafka",
    "NetworkServerControl",
    "SecondaryNameNode",
    "NameNode",
    "Master",
    "Worker",
    "HistoryServer",
    "DataNode",
    "QuorumPeerMain",
    "RunJar",
    "Jps"
  )
  val models = Map(
    "hadoop" -> Model("hadoop", "sbin", ("start-dfs.sh", "") :: Nil),
    "hive" -> Model(
      "hive",
      "metastore-derby",
      List(
        ("startNetworkServer", "-h hive"),
        ("hive", "--service metastore")
      )
    ),
    "spark" -> Model(
      "spark",
      "sbin",
      List(
        ("start-master.sh", ""),
        ("start-slave.sh", "spark://hive:7077"),
        ("start-history-server.sh", "")
      )
    ),
    "kafka" -> Model(
      "kafka",
      "bin",
      List(
        ("zookeeper-server-start.sh", "../config/zookeeper.properties"),
        ("kafka-server-start.sh", "../config/server.properties")
      )
    )
  )
  def outputStream[F[_]: Sync](guard: Semaphore[F]): Resource[F, PrintStream] =
    Resource.make { Sync[F].delay(Console.out) } { outStream =>
      guard.withPermit {
        Sync[F].delay(outStream.close()).handleErrorWith(_ => Sync[F].unit)
      }
    }
  def start[F[_]: Concurrent](args: List[String]) =
    for {
      guard <- Semaphore[F](1)
      _ <- outputStream(guard).use {
        case out => guard.withPermit(transfer(args, out))
      }
    } yield ()

  def transfer[F[_]: Sync](args: List[String], out: PrintStream) =
    for {
      _ <- Sync[F].delay(out.println(exec(args)))
    } yield ()
  def exec(args: List[String]): String = {
    parse(args).foreach(cmd => {
      cmd.run()
      Thread.sleep(3000)
      println(cmd)
    })
    args(0) match {
      case "stop" => {
        "jps".!!.trim().split("\n")
          .filter(e =>{!(e.contains("Jps") || e.contains("Main"))})
          .foreach(str => {
            println(str)
            s"kill -9 ${str.split(" ")(0)}".!
          })
      }
      case _ => 
    }
    check
  }
  def verifyArgs(args: List[String]): Boolean = {
    val reg = "(start|stop) (hadoop|hive|spark|kafka|all)".r
    args.mkString(" ") match {
      case reg(action,model) => true
      case _   => false
    }
  }
  def parse(args: List[String]): List[ProcessBuilder] = {
    (args(0), args(1)) match {
      case (action, "all") =>
        models.values.toList.flatMap(m => execmethod(m, action).get)
      case (action, model) => execmethod(models(model), action).get
    }
  }
  def check: String = {
    val process = "jps".!!
    resultName
      .map(n => (n, process.contains(n)))
      .map {
        case (n, true)  => s"$n 正在运行"
        case (n, false) => s"$n 未运行"
      }
      .mkString("\n")
  }
  def execmethod(m: Model, action: String): Try[List[ProcessBuilder]] = {
    val en = ru.runtimeMirror(getClass.getClassLoader)
    val command =
      ru.typeOf[Model].decl(ru.TermName(s"${action}Command")).asMethod
    val method = en.reflect(m).reflectMethod(command)
    method() match {
      case cmds: List[ProcessBuilder] => Success(cmds)
      case _                          => Failure(new Exception("运行时反射失败"))
    }
  }
}
case class Model(name: String, subPath: String, sh: List[(String, String)]) {
  def startCommand: List[ProcessBuilder] =
    sh.map {
      case (n, args) =>
        Process(
          s"setsid ${if (n.contains(".sh")) s"./$n" else n} $args",
          new File(s"${sys.env(name.toUpperCase + "_HOME")}/$subPath")
        )
    }
  def stopCommand: List[ProcessBuilder] =
    sh.filter(_._1.contains("start")).reverse.map {
      case (n, args) =>
        Process(
          s"setsid ${if (n.contains(".sh")) s"./$n" else n} $args"
            .replace("start", "stop"),
          new File(s"${sys.env(name.toUpperCase + "_HOME")}/$subPath")
        )
    }
}

