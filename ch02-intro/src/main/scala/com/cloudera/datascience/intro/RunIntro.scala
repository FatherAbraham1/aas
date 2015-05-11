/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.intro

import java.lang.Double.isNaN

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

case class MatchData(id1: Int, id2: Int,
  scores: Array[Double], matched: Boolean)
case class Scored(md: MatchData, score: Double)

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }

    val sc = new SparkContext(new SparkConf().setAppName("Intro").setMaster("local"))
    val input = args.length match {
      //sc.textFile("hdfs:///user/ds/linkage")
      case x: Int if x > 1 => sc.textFile(args(1))
      case _ => sc.textFile("./files/2/block_1_1000.csv")
        //sc.parallelize(List("37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE",
       // "39086,47614,1,?,1,?,1,1,1,1,1,TRUE"))
    }
    val rawblocks = input
      //sc.parallelize(Seq("id_1"))
    def isHeader(line: String) = line.contains("id_1")
    
    val noheader = rawblocks.filter(x => !isHeader(x))
    def toDouble(s: String) = {
     if ("?".equals(s)) Double.NaN else s.toDouble
    }

    def parse(line: String) = {
      val pieces = line.split(',')
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed = noheader.map(line => parse(line))
    parsed.cache()

    val matchCounts = parsed.map(md => md.matched).countByValue()
    val matchCountsSeq = matchCounts.toSeq
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)

    val stats = (0 until 9).map(i => {
      parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
    })
    stats.foreach(println)

    val nasRDD = parsed.map(md => {
      md.scores.map(d => NAStatCounter(d))
    })
    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    reduced.foreach(println)

    val statsmRDD:RDD[Array[Double]] = parsed.filter(_.matched).map(_.scores)
    val statsnRDD:RDD[Array[Double]] = parsed.filter(!_.matched).map(_.scores)
    // avoid after filter. rdd become empty, so we should use sample data not head data
    if(!statsmRDD.isEmpty() && !statsnRDD.isEmpty()) {
      val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
      val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))
      statsm.zip(statsn).map { case (m, n) =>
        (m.missing + n.missing, m.stats.mean - n.stats.mean)
      }.foreach(println)
    }
    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    ct.filter(s => s.score >= 4.0).
      map(s => s.md.matched).countByValue().foreach(println)
    ct.filter(s => s.score >= 2.0).
      map(s => s.md.matched).countByValue().foreach(println)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    // check null avoid exception
//    val acc = rdd.sparkContext.accumulator(0)
//    val nullParNUM = rdd.mapPartitions(iter => {
//      if(iter.isEmpty) {
//        acc += 1
//      }
//      iter
//    }).count()
//    val sumPar = rdd.partitions.size
    //val n = sumPar - nullParNUM
    // check null  rdd.coalesce(1).
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
        if(iter.isEmpty){
          println()
        }
        val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
        iter.foreach(arr => {
          nas.zip(arr).foreach { case (n, d) => n.add(d) }
        })
        Iterator(nas)
    })
    // a void n must be positive
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }
}

class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}
