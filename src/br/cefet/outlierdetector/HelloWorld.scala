package br.cefet.outlierdetector

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloWorld {

    def main(args: Array[String]) {
      val logFile = "/home/gabriel/once_upon_a_nightmare.txt" // Should be some file on your system
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("mind")).count()
      val numBs = logData.filter(line => line.toUpperCase().contains("LIFE")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    }

}