import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, date_format, lag, lead, max, min, rank, sum, to_date, to_timestamp, when}


object PROBLEMSONLEADANDLAG {
  def main(arr:Array[String]): Unit = {
      val conf=new SparkConf()
      conf.set("spark.appname","chaithu")
      conf.set("spark.master","local[*]")
    val spark=SparkSession.builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    //First Problem
//    val data=List((1,"Kitkat",1000,"2021-01-01"),
//          (1,"Kitkat",2000,"2021-01-02"),
//          (1,"Kitkat",1000,"2021-01-03"),
//            (1,"Kitkat",2000,"2021-01-04"),
//            (1,"Kitkat",3000,"2021-01-05"),
//            (1,"Kitkat",1000,"2021-01-06")).toDF("sno","product","revenue","date")
//    val window=Window.orderBy(col("product"))
//    val b=data.withColumn("differnece",
//      col("revenue") - lag("revenue",1).over(window)
//    ).show()
    //2ND Problem
//val data=List(
//  (1,"john",1000,"01/01/2016"),
//  (1,"john",2000,"01/01/2016"),
//  (1,"john",1000,"01/01/2016"),
//  (1,"john",2000,"01/01/2016"),
//  (1,"john",3000,"01/01/2016"),
//  (1,"john",1000,"01/01/2016"))
//  .toDF("sno","name","Salary","Date")
//    val window=Window.orderBy(col("name").desc)
//    val b=data.withColumn("salarydifference",
//      col("salary") - lag(col("salary"),1).over(window)
//    )
//    val c=b.withColumn("increamenrtordrecrement",
//      when(col("salarydifference")>=1000,"Up").otherwise("Down")
//    ).show()

//    val data=List(
//      (1,"karthik",1000),
//      (2,"moahn",2000),
//      (3,"vinay",1500),
//      (4,"Deva",3000)).toDF("sno","name","salary")
    //15thProblems
//    val window=Window.orderBy("sno")
//    val b=data.withColumn("lagg",
//      lag("salary",1).over(window))
//    val c=b.withColumn("lag",
//      when(col("salary")>col("lagg"),lead("salary",1).over(window)).otherwise("none")).withColumn("lead",
//      when(col("salary")>col("lagg"),lag("salary",1).over(window)).otherwise("none")).show()
//    val c=data.withColumn("lead",
//      when(col("salary")>col(
//    val c=b.withColumn("lag",
//      when(col("salary")>=col("salary")))
//13th problem
//    val window=Window.partitionBy("name").orderBy("sno")
//    val b=data.withColumn("avg",
//      avg("salary").over(window))
//    val c=b.withColumn("avgsalary",
//      col("salary")-col("avg")).show()
    //12TH
//    val window=Window.partitionBy("name")
//    val b=data.withColumn("maxsalary",
//      max("salary").over(window)).show()
    //9TH problem
//   val window=Window.orderBy("sno")
//    val b=data.withColumn("SalaryDifference",
//      col("salary") -lag(col("salary"),1).over(window)
//    )
//    val c=b.withColumn("lag",
//      when(col("SalaryDifference")>500,lag(col("salary"),1).over(window)).otherwise("null")).withColumn("lead",
//      when(col("SalaryDifference")>500,lead(col("salary"),1).over(window)).otherwise("null")).show()
//       //8TH problem
//    val window=Window.orderBy("sno")
//    val b=data.withColumn("leadsalary",
//      when(col("salary")>1500,lead(col("salary"),1).over(window))
//    ).withColumn("lagsalary",
//      when(col("salary")>1500,lag(col("salary"),1).over(window))).show()

    //4THproblems
//    val window=Window.orderBy("sno")
//    val b=data.withColumn("lead",
//      lag("salary",1).over(window)
//    )
//    val c=b.withColumn("percentage",
//      (col("salary")-col("lead"))/col("salary")*100
//    ).show()
    //7TH
//    val window=Window.partitionBy("name").orderBy("sno")
//    val a=data.withColumn("leadsalary",
//      lead("salary",1).over(window)
//    ).withColumn("lagsalary",
//      lag("salary",1).over(window)
//    ).show()
    //11 TH problem
//    val window=Window.orderBy("sno")
//    val c=data.withColumn("TotalSalary",
//      sum("salary").over(window)).show()
//    //14TH Problem
//    val window=Window.orderBy(col("salary").desc)
//    val b=data.withColumn("rank",
//      rank().over(window)
//    ).show()
    //5Th problem
//    val window=Window.orderBy("sno").rangeBetween(-2,0)
//    val b=data.withColumn("rollsum",
//      sum("salary").over(window)).show()
//    3rd Problem
//    val window=Window.orderBy("sno")
//    val b=data.withColumn("leadsalary",
//      lead("salary",1).over(window)
//    ).withColumn("lagsalary",
//      lag("salary",1).over(window)
//    ).show()


  }

}
