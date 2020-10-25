package execStartStop

import scala.sys.process.Process
import java.io.File

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
