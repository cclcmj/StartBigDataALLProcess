import scala.util.Success
import scala.util.Try
import scala.util.Failure
import java.io.File
import sys.process._
import java.io.File

@main
def main(machNum: String, port: String, path: os.Path = os.pwd) = {
  StartV2ray.startV2ray(machNum.toUpperCase(), port)
}
object StartV2ray {
  val portMap = (50515 to 50517).toList.zip((16823 to 16825).toList).toMap
  val v2rayPath = System.getenv("V2RAY_HOME")

  /**
    * 解析输入参数为配置文件名列表
    *
    * @param machNum
    * @param port
    * @return
    */
  def configList(machNum: String, port: String): List[String] =
    port match {
      case "all|ALL" =>
        portMap.keys.toList.map(portNum => s"port${machNum + portNum}")
      case portNum => List(s"port${machNum + portNum}")
    }

  /**
    * 验证输入参数是否复合要求
    *
    * @param machNum
    * @param port
    * @return
    */
  private def verifyStr(machNum: String, port: String): Boolean = {
    val pat = "(A|B)".r
    val portpat = "(all|50515|50516|50517)".r
    (machNum, port) match {
      case (pat(_), portpat(_)) => true
      case _                    => false
    }
  }

  /**
    * 验证端口是否启动成功
    *
    * @param machNum
    */
  private def verifyDown(machNum: String) = {
    portMap.toList
      .map {
        case (p, n) =>
          (
            p,
            s"netstat${machNum match {
              case "A" => ".exe -ano"
              case "B" => " -nltp"
            }}" #| s"grep $n"
          )
      }
      .foreach {
        case (port, process) =>
          println(
            Try(process.!!) match {
              case Success(v) =>
                port + "启动成功"
              case Failure(e) =>
                port + "未能完成启动"
            }
          )
      }
  }

  /**
    * 由配置名转为Processbuilder
    *
    * @param configName
    * @return
    */
  private def cmdBuilder(configName: String): ProcessBuilder = {
    val cmd = s"setsid ./v2ray -config=./${configName}.json"
    Process(cmd, new File(v2rayPath)) #>> new File(
      v2rayPath + s"/out/$configName.out"
    )
  }
  def startV2ray(machNum: String, port: String) = {
    require(
      verifyStr(machNum, port),
      s"$machNum $port 不是有效的参数，请输入a或者b（不区分大小写）和 all(不区分大小写)或者自定义端口号"
    )
    println("start")
    configList(machNum, port).map(cmdBuilder(_)).foreach { p =>
      p.run(); Thread.sleep(2000)
    }
    verifyDown(machNum)
    println("down")
  }
}
