package com.tcl.bigdata.yhb.timer.bean

/**
 * Created by root on 15-11-18.
 */
private[timer] class TimerDependBean(val outputs:Set[String],
                                     val dependence:Set[String],
                                     val command:String)
