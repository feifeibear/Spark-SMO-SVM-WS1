/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */



import java.io._
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.math._

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import org.apache.spark.mllib.util.MLUtils

import java.lang.System
/*
    print("Congratulations!")
  // Create an RDD of key-value pairs with Long keys.
    val rdd = sc.parallelize((1 to 1000000).map(x => (x.toLong, 0)))
    // Construct an IndexedRDD from the pairs, hash-partitioning and indexing
    // the entries.
    idval indexed = IndexedRDD(rdd).cache()

    // Perform a point update.
    val indexed2 = indexed.put(1234L, 10873).cache()
    // Perform a point lookup. Note that the original IndexedRDD remains
    // unmodified.
    indexed2.get(1234L) // => Some(10873)
    indexed.get(1234L) // => Some(0)

    // Efficiently join derived IndexedRDD with original.
    val indexed3 = indexed.innerJoin(indexed2) { (id, a, b) => b }.filter(_._2 != 0)
    indexed3.collect // => Array((1234L, 10873))

    // Perform insertions and deletions.
    val indexed4 = indexed2.put(-100L, 111).delete(Array(998L, 999L)).cache()
    indexed2.get(-100L) // => None
    indexed4.get(-100L) // => Some(111)
    indexed2.get(999L) // => Some(0)
    indexed4.get(999L) // => None

    print("Congratulations!", indexed2.get(999L), indexed2.get(1234L))
*/
object SVM {

  def main(args: Array[String]){
  	// open file read data
    val conf = new SparkConf().setAppName("SMO-SVM")  
    conf.setMaster("local[4]")  

    val sc = new SparkContext(conf)

    val data =  MLUtils.loadLibSVMFile(sc, args(0))
    //val data =  MLUtils.loadLibSVMFile(sc, "./dataset/heart_scale")

    
    var svm =  new kernelSVM(data)

    val t1 = System.currentTimeMillis
    
    svm.train()

    val t2 = System.currentTimeMillis
    val runtime = (t2 - t1)/1000
    print("time is ", runtime)
  }

}

