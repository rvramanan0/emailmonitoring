package com.nbc.adsale.monitoring

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

trait sparksessimpl {


  val  spark = SparkSession
    .builder()
    .appName("EmailMonitoring")
    .master("local[4]")
    .config("spark.cassandra.connection.host", "100.126.50.22")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.auth.username", "ssp")
    .config("spark.cassandra.auth.password", "P36tyybd")
    .config("spark.executor.instances","2")
    .config("spark.executor.cores", "2")
    .config("spark.cassandra.input.split.size_in_mb","67108864")
    .config("spark.cores.max", "4")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.sql.tungsten.enabled", "true")
    .config("spark.io.compression.codec", "snappy")
    .config("spark.rdd.compress", "true")
    .config("spark.executor.memory", "6g")
    .config("spark.sql.shuffle.partitions", "300")
    .config("spark.default.parallelism", "300")
    .getOrCreate();


  def getConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        classOf[Array[org.apache.spark.sql.Row]],
        classOf[Array[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema]],
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("[[B"),
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.ArrayType],
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        classOf[org.apache.spark.util.collection.BitSet],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        Class.forName("java.lang.Class")
      )
    )
  }
}
