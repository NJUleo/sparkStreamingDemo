import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SLN {
  def main(args:Array[String]): Unit ={
    //println("kkp")
    //    val wordFile = "file:///home/leo/Desktop/sources.list"
    //    val conf = new SparkConf().setAppName("SLN");
    //    val sc = new SparkContext(conf)
    //    val input = sc.textFile(wordFile, 2).cache()
    //    val lines = input.flatMap(line=>line.split(" "))
    //    val count = lines.map(word => (word,1)).reduceByKey{case (x,y)=>x+y}
    //    val output = count.saveAsTextFile("file:///home/leo/Desktop/cnm")


    val batch = 10
    val conf = new SparkConf().setAppName("storeHotness")
    val ssc = new StreamingContext(conf, Seconds(batch))

    ssc.checkpoint("file:///home/leo/Desktop/cpStore")

    val lines = ssc.textFileStream("file:///home/leo/Desktop/InputFilesStore")

    val record = lines.map(line => {
      line.split("-").toList
    })
//    record.print()

    val count = record.map(word => (word, 1))

    val state = count.updateStateByKey[Int](updateFunction _)

    //rank by total max comments
    val totalSort = state.transform(rdd => rdd.sortBy(_._2, false))
    totalSort.saveAsTextFiles("file:///home/leo/Desktop/hahaStore/hh")



    for(i <- 1 to 12){
      //filter the required month and sort, then save file
      val temp = state.filter(rdd => {
        rdd._1.apply(1) == i.toString
      }).transform(rdd =>{
        rdd.sortBy(_._2, false)
      }).saveAsTextFiles("file:///home/leo/Desktop/hahaStore/month/" + i.toString + "/month_" + i.toString + "_result")
    }

    //每类在每月的热度
    val  classWhiteList : List[String] = "火锅 烧烤 日本菜 小吃快餐 江浙菜 香锅烤鱼 日韩料理 自助餐 小龙虾 中式烧烤/烤串 新疆菜 西餐 北京菜 饮品店".split(" ").toList
    val classHotness = totalSort.map(
      rdd => ((rdd._1.apply(0), rdd._1.apply(1), rdd._1.apply(3)), rdd._2)
    ).reduceByKey(_+_)
      .filter(_._1._1.toInt >= 2017)//2017年以后的数据
      .filter(e => classWhiteList.contains(e._1._3))
      .map(rdd => (rdd._1, 0 - rdd._2))
      .transform(rdd => {
        rdd.sortBy(e => (e._1._1, e._1._2, e._2))
      })
      .map(rdd => (rdd._1, 0 - rdd._2))
      .map(rdd => (rdd._1._1, rdd._1._2, rdd._1._3, rdd._2))
    classHotness.saveAsTextFiles("file:///home/leo/Desktop/hahaStore/class")

//    //获取前十的类别
//    val classSort = state.map(rdd =>{
//      (rdd._1.apply(3),rdd._2)
//    }).reduceByKey((x, y) => x + y).transform(_.sortBy(_._2, false))
//    classSort.saveAsTextFiles("file:///home/leo/Desktop/hahaStore/class/result")



    ssc.start()
    ssc.awaitTermination()
    //      val str = "三天之内杀了你"
    //      val l = new JiebaSegmenter().sentenceProcess(str)
    //    println(l)
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }
}
