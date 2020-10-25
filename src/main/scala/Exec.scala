package main

import cats.effect.IOApp
import cats.effect.{ExitCode, IO}
import cats.effect.Sync
import cats.effect.Resource
import cats.effect.Concurrent
import java.io.PrintStream
import scala.collection.immutable.Nil
import execStartStop.Command
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
