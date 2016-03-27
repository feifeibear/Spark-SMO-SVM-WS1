import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import Array._
import scala.math._
import scala.util.control.Breaks._

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors 

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import scala.collection.mutable.HashMap

class kernelSVM(training_data:RDD[LabeledPoint]) extends java.io.Serializable{
 	
  //(data, devF)
  var data = training_data.map{
      case x => {
        var newlabel = 0D;
        if(x.label < 1){
          newlabel = -1D
        }
        else{
          newlabel = x.label
        }
        (newlabel, x.features)
      }
    }.map(x => (x._2, -1 * x._1.toDouble))

  val y = training_data.map{
      case x => {
        var newlabel = 0D;
        if(x.label < 1){
          newlabel = -1D
        }
        else{
          newlabel = x.label
        }
        newlabel
      }
    }.collect
  val broad_y = data.sparkContext.broadcast(y)

  var checkpoint_dir = "checkpoint.out"
  data.sparkContext.setCheckpointDir(checkpoint_dir)

  //(idx, features)
  var indexedData = IndexedRDD(data.zipWithIndex.map(x => (x._2, x._1._1)))
  //idx, devF
  var indexedDevF = IndexedRDD(data.zipWithIndex.map(x => (x._2, x._1._2)))

  val m = indexedData.count.toInt
  val n = indexedDevF.count.toInt

  var alpha = new Array[Double](m)
  alpha = alpha.map(x => 0D)

 	def train(){
    indexedData.cache 
    indexedDevF.cache

    val cost = 1D
    val gamma = 0.1D
    val tolerance : Double = 1e-3D
    val epsilon = 1e-5
    val cEpsilon = cost - epsilon

    var iteration = 0

    var bLow = 1D;
    var bHigh = -1D;
    var iLow = -1;
    var iHigh = -1;

    var i = 0
    while( i < m){
      if (y(i) == -1){
          if (iLow == -1){
              iLow = i
              if (iHigh > -1)
                  i = m
          }
      }
      else{
          if (iHigh == -1){
              iHigh = i
              if (iLow > -1)
                  i = m
          }
      }
      i = i+1
    }

    println("iHigh, iLow ", iHigh, iLow)
    //var dataiHigh = dataindexed.get(iHigh.toLong).get
    //var dataiLow = dataindexed.get(iLow.toLong).get

    var dataiHigh = indexedData.get(iHigh.toLong).get
    //data.zipWithIndex.filter(_._2 == iHigh).map(_._1._1).first
    var dataiLow = indexedData.get(iLow.toLong).get 
    //data.zipWithIndex.filter(_._2 == iLow).map(_._1._1).first

    var eta = 2 - 2*kernel( dataiHigh , dataiLow )

    //println(dataiHigh.toDense)
    //println(dataiLow.toDense)

    println("eta is ", eta)

    var alphaLowOld = alpha(iLow)
    var alphaHighOld = alpha(iHigh)

    var alphaLowNew = 0D


    if ( eta != 0 )
      alphaLowNew = 2.0/eta
    else
      alphaLowNew = 0.0

    if (alphaLowNew > cost)
        alphaLowNew = cost   

    println("alphaLowNew alphaLowOld ", alphaLowNew, alphaLowOld)

    alpha(iLow) = alphaLowNew
    alpha(iHigh) = alphaLowNew
    var alphaLowDiff = alpha(iLow) - alphaLowOld
    var alphaHighDiff = -1.0 * y(iHigh) * y(iLow) * alphaLowDiff
    iteration = 1

    println("iLow iHigh ", iLow, iHigh)
    
    breakable{
      while(bLow > bHigh + 2*tolerance){
        //devF[i] = devF[i] + alphaHighDiff * label[iHigh] * kernel(data[i], data[iHigh], n, gamma) + alphaLowDiff * label[iLow] * kernel(data[i], data[iLow], n, gamma);
        //println("==========================================")
        //println("alphaHighDiff, alphaLowDiff, iHigh, iLow ", alphaHighDiff, alphaLowDiff, iHigh, iLow)

        //println(dataiHigh.toDense)
        //println(dataiLow.toDense)
        
        indexedDevF = indexedData.innerJoin(indexedDevF){(id, a, b) => (a,b)}.mapValues( x => ( x._2 + alphaHighDiff * broad_y.value(iHigh) * kernel(x._1, dataiHigh) + alphaLowDiff * broad_y.value(iLow) * kernel(x._1, dataiLow) ) ).persist

        if (iteration % 100 == 0 ) {
              indexedDevF.checkpoint()
        }
        /*
        data = data.map {
          case x => {
            var tmp = x._2 + alphaHighDiff * broad_y.value(iHigh) * kernel(x._1, dataiHigh) + alphaLowDiff * broad_y.value(iLow) * kernel(x._1, dataiLow)
            (x._1, tmp)
          }
        }
        */
        
        //all reduce move devF to driver
        //(idx, devF)
        //indexedDevF.persist
        val devFMap = indexedDevF.collectAsMap()

/*
        val devFMap = devF.collect.foldLeft(new HashMap[Int, Double]()){
          (map, term) => {
            map += term._1.toInt -> term._2
            map
          }
        }
*/
        //println("=================iter1 devF==============")
        //devF.foreach(println)
        //println("=================iter2 devF==============")

        //break;

        var min_value = 655345D
        var max_value = -min_value
        var min_i = -1
        var max_i = -1

        i = 0
        while( i < m ){

          if(((y(i) > 0) && (alpha(i) < cEpsilon)) || ((y(i) < 0) && (alpha(i) > epsilon))){
              if( devFMap(i) <= min_value){
                  min_value = devFMap(i)
                  min_i = i
              }
          }

          if(((y(i) > 0) && (alpha(i) > epsilon)) || ((y(i) < 0) && (alpha(i) < cEpsilon))){
              if( devFMap(i) >= max_value ){
                  max_value = devFMap(i)
                  max_i = i
              }
          }
          i = i+1
        }

        iHigh = min_i
        iLow = max_i
        bHigh = devFMap(iHigh)
        bLow = devFMap(iLow) 

        //println("[FJR INFO] iHigh, iLow, bHigh, bLow ", iHigh, iLow, bHigh, bLow)

/*
        println("===========old==============================")
        if(iteration == 1){
          indexedData.mapValues(_._2).collect.foreach(println)
        }
*/
        //indexedData.cache //essential makesure 
        dataiHigh = indexedData.get(iHigh.toLong).get
        dataiLow = indexedData.get(iLow.toLong).get 

/*
        println("===========new==============================")
        if(iteration == 1){
          indexedData.mapValues(_._2).collect.foreach(println)
          break
        }
        */
        //dataiHigh = data.zipWithIndex.filter(_._2 == iHigh).map(_._1._1).first
        //dataiLow = data.zipWithIndex.filter(_._2 == iLow).map(_._1._1).first

        //dataiHigh = dataindexed.get(iHigh.toLong).get
        //dataiLow = dataindexed.get(iLow.toLong).get

        eta = 2 - 2 * kernel(dataiHigh, dataiLow)
        //println("1 eta ", eta);

        alphaHighOld = alpha(iHigh)
        alphaLowOld = alpha(iLow)
        var alphaDiff = alphaLowOld - alphaHighOld
        var lowLabel = y(iLow)
        var sign = y(iHigh) * lowLabel

        var alphaLowLowerBound = 0D
        var alphaLowUpperBound = 0D

        if (sign < 0){
            if (alphaDiff < 0){
                alphaLowLowerBound = 0;
                alphaLowUpperBound = cost + alphaDiff;
            }
            else{
                alphaLowLowerBound = alphaDiff;
                alphaLowUpperBound = cost;
            }
        }
        else{
            var alphaSum = alphaLowOld + alphaHighOld;
            if (alphaSum < cost){
                alphaLowUpperBound = alphaSum;
                alphaLowLowerBound = 0;
            }
            else{
                alphaLowLowerBound = alphaSum - cost;
                alphaLowUpperBound = cost;
            }
        }

        //println("2 alphaLowLowerBound alphaLowUpperBound", alphaLowLowerBound, alphaLowUpperBound)

        if (eta > 0){
            alphaLowNew = alphaLowOld + lowLabel*(bHigh - bLow)/eta;
            //println("[in eta ] alphaLowNew ", alphaLowNew, alphaLowOld, lowLabel, bHigh, bLow, eta)
            if (alphaLowNew < alphaLowLowerBound)
                alphaLowNew = alphaLowLowerBound;
            else if (alphaLowNew > alphaLowUpperBound) 
                alphaLowNew = alphaLowUpperBound;
        }
        else{
            var slope = lowLabel * (bHigh - bLow);
            var delta = slope * (alphaLowUpperBound - alphaLowLowerBound);
            if (delta > 0){
                if (slope > 0)  
                    alphaLowNew = alphaLowUpperBound;
                else
                    alphaLowNew = alphaLowLowerBound;
            }
            else
                alphaLowNew = alphaLowOld;
        }
        //println("3 alphaLowNew alphaLowOld ", alphaLowNew, alphaLowOld)

        alphaLowDiff = alphaLowNew - alphaLowOld;
        alphaHighDiff = -sign*(alphaLowDiff);
        alpha(iLow) = alphaLowNew;
        alpha(iHigh) = (alphaHighOld + alphaHighDiff);

        
        //println("[FJR INFO] alpha(iLow), alpha(iHigh) ", alpha(iLow), alpha(iHigh))
        if(iteration % 50 == 0)
          print(".")
        //break
        iteration = iteration + 1;
        //println("==========================================")

/*
        if(iteration == 10)
          break
*/

      }
 	  }

    println("total iteration is ", iteration)
  }//train


  def kernel(x_1: Vector, x_2: Vector): Double = {
        val gamma = 0.1
        //math.exp(-1 * gamma * math.pow(Vectors.sqdist(x_1, x_2),2))
        math.exp(-1 * gamma * Vectors.sqdist(x_1, x_2))
    }

}