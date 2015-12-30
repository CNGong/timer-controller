package com.tcl.bigdata.yhb.timer

import java.util.Date
import com.tcl.bigdata.yhb.timer.bean.TimerDependBean
import scala.collection.mutable
import scala.xml.XML
//import sys.process._

object TimerDependence {

  val waitOutputs = new mutable.HashSet[String]
  val finishOutputs = new mutable.HashSet[String]
  val failedOutputs = new mutable.HashSet[String]
  val commands = new mutable.HashSet[TimerDependBean]

  private def getFirstNodeText(nodeSeq:scala.xml.NodeSeq) = {
    nodeSeq.headOption match {
      case Some(node) => node.text.trim
      case None => ""
    }
  }

  private def loadConf(confPath:String){
    val commandsXML = XML.loadFile(confPath)
    for(commandXML <- commandsXML \ "timer-command"){
      val outputs = getFirstNodeText(commandXML \ "outputs")
      val dependence = getFirstNodeText(commandXML \ "dependence")
      val command_content = getFirstNodeText(commandXML \ "command-content")
      //命令放在commands数组
      commands += new TimerDependBean(outputs.split(",").toSet,dependence.split(",").toSet,command_content)
      //这个集合主要用来判断当前的命令可不可以执行
      waitOutputs ++= outputs.split(",").toSet
    }
  }

  private def executeCommands(){
    var executed:mutable.HashSet[TimerDependBean] = null
    do{
      executed = new mutable.HashSet[TimerDependBean]
      for(command <- commands){
        if(command.dependence == Set("") || command.dependence.subsetOf(finishOutputs)){
          val process = Runtime.getRuntime.exec(command.command)
//          val isSuccess = command.command !
          //判断是否执行成功
          if(process.waitFor() == 0){
//          if(isSuccess == 0){
            println(new Date + " execute \"" + command.command + "\" success")
            waitOutputs --= command.outputs
            executed += command
            finishOutputs ++= command.outputs
          }else{
            println(new Date + " execute \"" + command.command + "\" failed")
            waitOutputs --= command.outputs
            executed += command
            failedOutputs ++= command.outputs
          }
        }else if(!command.dependence.subsetOf(waitOutputs union finishOutputs)){
          println(new Date + " \"" + command.command + "\" can't find dependence " + command.dependence)
          waitOutputs --= command.outputs
          executed += command
          failedOutputs ++= command.outputs
        }
      }
      commands --= executed
    }while(commands.nonEmpty && executed.nonEmpty)
    failedOutputs ++= waitOutputs
  }

  def main(args: Array[String]): Unit = {
    val confPath = args(0)
    loadConf(confPath)
    executeCommands()
    println("failedOutputs:"+failedOutputs.mkString(","))
  }
}
