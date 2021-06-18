package org.siti.bigdata.examples

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LogisticRegressionExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder
            .getOrCreate()

        import spark.implicits._

        // Load training data
        val irisDataSet = spark.read.format("csv")
            .option("header", "true")
            .load(args(0))

        println(Console.GREEN + "Successfully loaded " + irisDataSet.count() + " records" + Console.RESET)

        //Parsing raw data (sql.Dataframe) as LabeledPoint (DataSet[LabledPoint])
        val irisDataSetParsed = irisDataSet.withColumn("class", when(col("class").equalTo("Iris-setosa"), "1")
            .when(col("class").equalTo("Iris-versicolor"), "2")
            .when(col("class").equalTo("Iris-virginica"), "3"))
            .map { data =>
                val sepalLength = data(0).toString.toDouble
                val sepalWidth = data(1).toString.toDouble
                val petalLength = data(2).toString.toDouble
                val petalWidth = data(3).toString.toDouble
                val trueClass = data(4).toString.toDouble
                LabeledPoint(trueClass, Vectors.dense(Array(sepalLength, sepalWidth, petalLength, petalWidth)))
            }
        irisDataSetParsed.show(1,truncate = false)

        println(Console.GREEN + "Successfully parsed " + irisDataSetParsed.count() + " records" + Console.RESET)

        //Splitting parsed data as training set and test test
        val Array(trainingData, testData) = irisDataSetParsed.randomSplit(Array(0.7, 0.3),1L)

        //Declaring Multinomial Logistic Regression options
        val lr = new LogisticRegression()
            .setFamily("multinomial")
            .setMaxIter(50)

        println(Console.GREEN + "Training Multinomial Logistic Regression model" + Console.RESET)

        val lrModel = lr.fit(trainingData)

        println(s"Coefficients: \n${lrModel.coefficientMatrix}\n")
        println(s"Intercepts: \n${lrModel.interceptVector}\n")

        // Compute raw scores on the test set
        //Calculating predictions of test set using trained model
        val predictions = lrModel.transform(testData)

        //Prediction result for test set
        predictions.show(10, truncate = false)

        //Evaluating Multinomial Logistic Regression model
        //Evaluation Method A
        val metricA = new MulticlassClassificationEvaluator().setPredictionCol("prediction").setLabelCol("label")

        val f1Score = metricA.setMetricName("f1").evaluate(predictions)
        val weightedPrecision = metricA.setMetricName("weightedPrecision").evaluate(predictions)
        val weightedRecall = metricA.setMetricName("weightedRecall").evaluate(predictions)
        val accuracy = metricA.setMetricName("accuracy").evaluate(predictions)

        println("f1Score = " + f1Score)
        println("weightedPrecision = " + weightedPrecision)
        println("weightedRecall = " + weightedRecall)
        println("accuracy = " + accuracy)

        //Evaluation method B
        val predictionsAndLables = predictions.map{x=>(x.getDouble(4),x.getDouble(0))}.rdd
        val metricB = new MulticlassMetrics(predictionsAndLables)

        println("WeightedFMeasure = " + metricB.weightedFMeasure)
        println("weightedPrecision = " + metricB.weightedPrecision)
        println("weightedRecall = " + metricB.weightedRecall)
        println("accuracy = " + metricB.accuracy)
        println("weightedTruePositiveRate = " + metricB.weightedTruePositiveRate)
        println("weightedTruePositiveRate = " + metricB.weightedTruePositiveRate)
        println("Confusion Matrix : \n" + metricB.confusionMatrix)

        //Taking comma separated values for features as command line argument
        // to predict using the above trained model
    
        // println(Console.MAGENTA + "Enter new values for sepalLength,sepalWidth,petalLength,petalWidth to predict class" + Console.RESET)
        // val newData = Console.in.readLine()
        // val arr = newData.split(",")
        // if(arr.length==4){
        //     try {
        val l = List(LabeledPoint(0, Vectors.dense(Array(args(1).toDouble, args(2).toDouble, args(3).toDouble, args(4).toDouble))))
        val prediction = lrModel.transform(l.toDF())
        val result = prediction.first().getDouble(4)
        if(result==1.0){
            println("Class : \"Iris-setosa\"")
        } else if(result==2.0){
            println("Class : \"Iris-versicolor\"")
        } else if(result==3.0){
            println("Class : \"Iris-virginica\"")
        }
        //     }catch {
        //         case _: java.lang.NumberFormatException => None
        //     }
        // }else{
        //     println(Console.RED + "ERROR : Enter comma separated values for sepalLength,sepalWidth,petalLength,petalWidth")
        // }

    }

}
