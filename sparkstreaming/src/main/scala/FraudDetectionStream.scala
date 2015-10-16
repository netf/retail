import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.joda.time.DateTime
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.SomeColumns


case class Receipts(credit_card_number: Long,
                    store_id: Int,
                     receipt_timestamp: Long)


object FraudDetectionStream {

  def main(args: Array[String]) {

//    Create Spark Context

    val sparkConf = new SparkConf()
      .setAppName("CreditCardFraudDetectionStream")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.shuffle.partitions","4")

    import sqlContext.implicits._

    val ssc = new StreamingContext(sc, Seconds(10))

    val lines = ssc.receiverStream(new JMSReceiver("Receipts","tcp://localhost:61616"))

//          // note that we use def here so it gets evaluated in the map
//          def current_time = new DateTime()
//          val series_name: String = "hotproducts"
//
//          lines.map(line => line.split('|'))       // we have a list of arrays - not too useful
//            .map(arr => (arr(0), arr(1).toInt))    // convert to list of tuples (product, amount)
//            .reduceByKeyAndWindow( (total_sales,current_sale) => total_sales + current_sale , Seconds(10))  // same thing, but 1 row per product
//            .map{ case (product, amount) => Map(product -> amount)}
//            .reduce( (current_map, new_element) => current_map ++ new_element )  // Make a map of {product -> amount, ...}
//            .map( qty_map => { (series_name, current_time, qty_map)}) // fill in the row keys
//            .saveToCassandra("retail","real_time_analytics",SomeColumns("series","timewindow","quantities"))

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val incoming_receipts = lines.map(line => line.split('|'))
      .map( line => Receipts(line(0).toLong,line(1).toInt, formatter.parse(line(2)+ " "+line(3)).getTime))

    incoming_receipts.foreachRDD( r => {

      r.collect() foreach println
      println("Number of incoming receipts : " + r.count())

      val incoming_receipts_df = r.toDF()
      incoming_receipts_df.registerTempTable("incoming_receipts")

      val receipts_df = sqlContext.read.format("org.apache.spark.sql.cassandra").
        options(Map("keyspace"-> "retail", "table" -> "receipts")).
        load()

      receipts_df.registerTempTable("receipts")

      val stores_df = sqlContext.read.format("org.apache.spark.sql.cassandra").
        options(Map("keyspace"-> "retail", "table" -> "stores")).
        load()

      stores_df.registerTempTable("stores")

      sqlContext.udf.register("unix_timestamp", (s:Date) => s.getTime )

//      val incoming_receipts_with_state = sqlContext.sql("select credit" +
//        " from incoming_receipts ir, receipts r " +
//        " where r.credit_card_number = ir.credit_card_number " +
//        " and store")

      val potential_fraud_df = sqlContext.sql("select distinct ir.credit_card_number, ir.receipt_timestamp, s1.state "+
        " from incoming_receipts ir, receipts r, stores s1, stores s2 " +
        " where r.credit_card_number = ir.credit_card_number " +
        " and s1.store_id = ir.store_id " +
        " and s2.store_id = r.store_id " +
        " and s1.state <> s2.state " +
        " and unix_timestamp( r.receipt_timestamp) + 24*60*60*1000 > ir.receipt_timestamp " ).cache()



      potential_fraud_df.show()

      val nb_potential_fraud = potential_fraud_df.count()

      println("number of potential frauds in current set : "+ nb_potential_fraud)

      if (nb_potential_fraud>0) {
        potential_fraud_df.write.format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "retail", "table" -> "potential_fraud"))
          .mode(SaveMode.Append)
          .save()


        potential_fraud_df.registerTempTable("potential_fraud")


        // update potential_fraud_per_state table
        // read the current counters
        val current_potential_fraud_per_state_df = sqlContext.read.format("org.apache.spark.sql.cassandra").
          options(Map("keyspace"-> "retail", "table" -> "potential_fraud_per_state")).
          load()

        current_potential_fraud_per_state_df.registerTempTable("current_potential_fraud_per_state")

        // values to add to current counters
        val potential_fraud_per_state_to_add = sqlContext.sql("select state, count(*) as number from potential_fraud group by state")
        potential_fraud_per_state_to_add.show()
        val nb_potential_fraud_per_state_to_add = potential_fraud_per_state_to_add.count()
        println("number of states impacted : "+ nb_potential_fraud_per_state_to_add)

        potential_fraud_per_state_to_add.registerTempTable("potential_fraud_per_state_to_add")

        // counters with new values
        val potential_fraud_per_state = sqlContext.sql("select c_pf.state, c_pf.number+pf_a.number as number from current_potential_fraud_per_state c_pf join potential_fraud_per_state_to_add pf_a on c_pf.state = pf_a.state")

        potential_fraud_per_state.show()

        potential_fraud_per_state.write.format("org.apache.spark.sql.cassandra").
          options(Map( "table" -> "potential_fraud_per_state", "keyspace" -> "retail")).
          mode(SaveMode.Append).
          save()

      }

      potential_fraud_df.unpersist()

    })





    ssc.start()

    ssc.awaitTermination()

  }
}
