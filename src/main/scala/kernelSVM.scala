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

class kernelSVM(training_data:RDD[LabeledPoint]) extends java.io.Serializable{
 	
	var data = training_data.map{x => {
        if(x.label == 0){ 
          val res = LabeledPoint(-1, x.features)
          res
        }
        else
          x
      }
    }
	val m = data.count.toInt

  var checkpoint_dir = "checkpoint.out"
  data.sparkContext.setCheckpointDir(checkpoint_dir)

 	def train(){
    val cost = 1D
    val gamma = 0.1D
    val tolerance = 1e-3
    val eps = 1e-3
    val tau = 1e-12
 		//var A = data.sparkContext.parallelize(new Array[Double](m)).map(x => -1D)
 		//var G = data.sparkContext.parallelize(new Array[Double](m))
    //(data, A, G)
    var model = data.map(x => (x, 0D , -1D))


    var iteration = 0

    breakable{
      while(true){
          //(y, A, G, t)
          var G_max = 0D
          var i = -1
//!!!bug -1000D
          var dummy = (-1000D, (LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(1.0) )), 0D, 0D) , 0L) 
          //((data, A, G), t)
          var res1 = model.zipWithIndex.filter(x => findSetI(x, cost)).map{ 
            case ( v, k ) => (-v._1.label*v._3, v, k)
            }.fold(dummy)((a, b) => {if(a._1 > b._1) a else b})
          //-yt*Gt, model, idx

          G_max = res1._1
          i = res1._3.toInt
          var modeli = res1._2

          println("G_max and i is",G_max, i)

  
          var j = -1
          var obj_min = 0D

          //((data, A, G), t)
          var res2 = model.zipWithIndex.filter{ case x => (x._1._1.label == 1 && x._1._2 > 0 || x._1._1.label == -1 && x._1._2 < cost)}

          //((data, A, G), t) -> value
          var res3 = res2.map{
              x => -x._1._1.label*x._1._3
          }.min
          
          var G_min = res3

          //G_min, modelj, j
          dummy = (1000D, (LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(1.0) )), 0D, 0D) , 0L)  
          //((data, A, G), t) -> value, (data, A, G), t
          var res4 = res2.filter(x => (G_max + x._1._1.label * x._1._3) > 0).map{
              x => {
                  val b = G_max + x._1._1.label * x._1._3
                  var a = kernel(modeli._1.features, modeli._1.features) + kernel(x._1._1.features, x._1._1.features) - 2*kernel(modeli._1.features, x._1._1.features)
                  if(a <= 0)
                    a = tau
                  (-(b*b)/a, x._1, x._2)
                }
              }.fold(dummy)( (a,b) => if(a._1 < b._1) a else b )

          j = res4._3.toInt
          var modelj = res4._2

          println("G_min and j is",G_min, j)
          
          if(G_max - G_min < eps){
            i = -1
            j = -1
          }

          if(j == -1)
            break

          var a = kernel(modeli._1.features, modeli._1.features) + kernel(modelj._1.features, modelj._1.features) - 2*kernel(modeli._1.features, modelj._1.features)

          if( a <= 0 )
            a = tau

          println("a: ", a, kernel(modeli._1.features, modeli._1.features), kernel(modelj._1.features, modelj._1.features),kernel(modeli._1.features, modelj._1.features))

          var b = -modeli._1.label*modeli._3 + modelj._1.label*modelj._3 //-y[i]*G[i]+y[j]*G[j]

          println("b: ", b)

          var oldAi = modeli._2
          var oldAj = modelj._2

          //((data, A, G)
          var Ai = modeli._2
          var Aj = modelj._2

          var yi = modeli._1.label
          var yj = modelj._1.label

          Ai = Ai + yi*b/a
          Aj = Aj - yj*b/a

          var sum = yi*oldAi + yj*oldAj
          println("sum y A is ", sum, yi, yj, Ai, Aj)
          if(Ai > cost)
             Ai = cost
          if(Ai < 0)
             Ai = 0

          Aj = yj*(sum-yi*Ai)

          if (Aj > cost)
              Aj = cost
          if (Aj < 0)
              Aj = 0
          Ai = yi*(sum-yj*Aj)

          println("Ai Aj ", Ai, Aj)

          var deltaAi = Ai - oldAi 
          var deltaAj = Aj - oldAj

          model = model.zipWithIndex.map{
            x=>{
              if(x._2 == i)
                (x._1._1, Ai, x._1._3)
              else if(x._2 == j)
                (x._1._1, Aj, x._1._3)
              else
                x._1
            }
          }.map{
            x=>{
              var Gtnew = x._3 + x._1.label*yi*kernel(x._1.features,modeli._1.features)*deltaAi + x._1.label*yj*kernel(x._1.features,modelj._1.features)*deltaAj
              (x._1, x._2, Gtnew)
            }
          }

          iteration = iteration+1;
          println("iteration ", iteration)
          //fjr temp
          //break
          //b = -datai.label*model._3 + dataj.label*model._3
          if (iteration % 100 == 0 ) {
                model.checkpoint()
          }
      }
 	  }
  }//train


  def findSetI( A: ( (LabeledPoint, Double, Double) , Long), cost : Double): Boolean = {
      if( A._1._1.label == 1 && A._1._2 < cost || A._1._1.label == -1 && A._1._2 > 0)
        return true
      else 
        return false
  }

  def kernel(x_1: Vector, x_2: Vector): Double = {
        val gamma = 0.1
        //math.exp(-1 * gamma * math.pow(Vectors.sqdist(x_1, x_2),2))
        math.exp(-1 * gamma * Vectors.sqdist(x_1, x_2))
    }

}