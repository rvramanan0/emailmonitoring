package com.nbc.adsale.monitoring
import java.sql.{Connection, DriverManager}

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame;


object extparam extends App with sparksessimpl  {



    val conf = ConfigFactory.load("application.conf")

    import spark.implicits._

try {



  val driver = conf.getString("oracleParams.oracle.driver")
  val url = conf.getString("oracleParams.oracle.url")
  val username = conf.getString("oracleParams.oracle.username")
  val password = conf.getString("oracleParams.oracle.password")
  val totrows = (tablename: String) => {


    var connection: Connection = null
    Class.forName("oracle.jdbc.driver.OracleDriver")
    connection = DriverManager.getConnection(url)
    var rcData: Long = 0
    //    // create the statement, and run the select query
    val statement = connection.createStatement()
    val result = statement.executeQuery("SELECT count(0) as rccount FROM " + tablename)

    while (result.next()) {
      rcData = result.getLong("rccount")

    }
    result.close()
    rcData
  }


  spark.udf.register("totrows", totrows)



  val dfcount = (cassTbl: String) => {
    var cassCnt: BigInt = 0
    val casstab = cassTbl.trim()
    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> casstab, "keyspace" -> "data_lake")).load().toDF()
    cassCnt = df.count()
    cassCnt

  }

  //val NBC_APPS = "jdbc:oracle:thin:NBC_APPS/nbc_apps_d124@aoadbss00002c0.tfayd.com:15192/d124"
  val exp_param = "monitoring_param"

  val tabname = spark.read.format("jdbc").options(Map("driver" -> "oracle.jdbc.driver.OracleDriver", "url" -> url, "dbtable" -> exp_param)).load()


  tabname.createOrReplaceTempView("rc1")
  tabname.createOrReplaceTempView("rc2")

  val tablenames = spark.sql("select Cassandra_table,key_no,oracle_table,totrows(oracle_table) as ora_count from rc1")




  val cassnames = spark.sql("select Cassandra_table from rc2")

  val cassnames1 =cassnames.select("Cassandra_table").map(r =>r.getAs[String](0)).collect.toList



  val cassnames5 = cassnames1.map {
    x =>
      val cass_table = x
      val casscount = dfcount(x.toString)
      (cass_table, casscount)
  }



  val dfWithColNames: DataFrame = cassnames5.toDF("cass_table", "casscount")


  tablenames.createOrReplaceTempView("table1")
  dfWithColNames.createOrReplaceTempView("table2")


  val res = spark.sql("select t1.key_no,t1.oracle_table,t1.ora_count,t2.cass_table,t2.casscount,(t1.ora_count -t2.casscount) as Diff from table1 t1,table2 t2 where t1.Cassandra_table = t2.cass_table")

  res.createOrReplaceTempView("mailds")

  val mail_zero = spark.sql("select ROW_NUMBER() OVER (order by oracle_table) as Seq_no,oracle_table,ora_count,cass_table,casscount,abs(Diff) as Difference from mailds where Diff <> 0")

  def showHTML(ds: DataFrame, limit: Int = 40, truncate: Int = 20) = {
    import xml.Utility.escape
    val data = ds.take(limit)
    val header = ds.schema.fieldNames.toSeq
    val rows: Seq[Seq[String]] = data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val q = new StringBuilder

    println("insidestringbuilder")

    q.append("<html> <body> <p>Hi team,</p> <p>Please find the details of count mismatch between NBC Apps and Cassandra</p> </body> </html>")
    q.append(System.getProperty("line.separator"))

    println("q before html-" + q)

    q.append(
      s""" <html>
<head>
<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
</style>
</head>
<body>

<table style="width:100%">

                <tr>
                 ${header.map(h => s"<th>${escape(h)}</th>").mkString}
                </tr>
                ${
        rows.map { row =>
          s"<tr>${row.map { c => s"<td>${escape(c)}</td>" }.mkString}</tr>"
        }.mkString
      }
            </table>
        """)
    q.append(System.getProperty("line.separator"))


    println("q after html-" + q)
    q.append("<p>Thanks and Reagrds<br>Track 7 & team</p>")
    q.append(System.getProperty("line.separator"))
    q.append("<p>***********This is an auto generated test POC email by Venkat,Kindly share suggestions if any************</p>")


    println("value final-" + q.toString())
    q.toString()


  }


  if (mail_zero.count > 0) {
    mail.functionExample(showHTML(mail_zero))
  }

}catch {
  case e: Throwable => e.printStackTrace
}finally{
  spark.stop()

}

}
