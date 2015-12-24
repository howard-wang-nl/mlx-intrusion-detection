/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by howard@lunatech.com for MLX in Dec. 2015.
 */

package nl.ing.mlx

// scalastyle:off println
//import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
 * K-means clustering analysis on KDD Cup 1999 data set for network intrusion detection.
 * {{{
 * sbt assembly &&
 * spark-submit --master 'local[*]' \
 *   --driver-memory 4g \
 *   --executor-memory 4g \
 *   target/scala-2.10/mlx-intrusion-detection-assembly-0.1.0.jar -k 10 data/kddcup.data_10_percent.csv
 * }}}
 */

object ClusterKMeans {

  /* Input data fields
  http://kdd.ics.uci.edu/databases/kddcup99/task.html
  http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html

Sample data line:
0,tcp,http,SF,181,5450,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.00,0.00,0.00,0.00,1.00,0.00,0.00,9,9,1.00,0.00,0.11,0.00,0.00,0.00,0.00,0.00,normal.
184,tcp,telnet,SF,1511,2957,0,0,0,3,0,1,2,1,0,0,1,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,1,3,1.00,0.00,1.00,0.67,0.00,0.00,0.00,0.00,buffer_overflow.
0,tcp,other,REJ,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,1.00,1.00,1.00,0.00,0.00,6,6,1.00,0.00,0.17,0.00,0.00,0.00,1.00,1.00,normal.

10 features without description?

Basic features of individual TCP connections.
|feature name |description  |type |
|---------------|---------------|-----|
|duration  |length (number of seconds) of the connection  |continuous|
|protocol_type  |type of the protocol, e.g. tcp, udp, etc.  |discrete|
|service  |network service on the destination, e.g., http, telnet, etc.  |discrete|
|src_bytes  |number of data bytes from source to destination  |continuous|
|dst_bytes  |number of data bytes from destination to source  |continuous|
|flag  |normal or error status of the connection  |discrete|
|land  |1 if connection is from/to the same host/port; 0 otherwise  |discrete|
|wrong_fragment  |number of ``wrong'' fragments  |continuous|
|urgent  |number of urgent packets  |continuous|

Content features within a connection suggested by domain knowledge.
|---------------|---------------|-----|
|hot  |number of ``hot'' indicators |continuous|
|num_failed_logins  |number of failed login attempts  |continuous|
|logged_in  |1 if successfully logged in; 0 otherwise  |discrete|
|num_compromised  |number of ``compromised'' conditions  |continuous|
|root_shell  |1 if root shell is obtained; 0 otherwise  |discrete|
|su_attempted  |1 if ``su root'' command attempted; 0 otherwise  |discrete|
|num_root  |number of ``root'' accesses  |continuous|
|num_file_creations  |number of file creation operations  |continuous|
|num_shells  |number of shell prompts  |continuous|
|num_access_files  |number of operations on access control files  |continuous|
|num_outbound_cmds |number of outbound commands in an ftp session  |continuous|
|is_hot_login  |1 if the login belongs to the ``hot'' list; 0 otherwise  |discrete|
|is_guest_login  |1 if the login is a ``guest''login; 0 otherwise  |discrete|

Traffic features computed using a two-second time window.
|---------------|---------------|-----|
|count  |number of connections to the same host as the current connection in the past two seconds  |continuous|
||Note: The following  features refer to these same-host connections.|
|serror_rate  |% of connections that have ``SYN'' errors  |continuous|
|rerror_rate  |% of connections that have ``REJ'' errors  |continuous|
|same_srv_rate  |% of connections to the same service  |continuous|
|diff_srv_rate  |% of connections to different services  |continuous|
|srv_count  |number of connections to the same service as the current connection in the past two seconds  |continuous|
||Note: The following features refer to these same-service connections.|
|srv_serror_rate  |% of connections that have ``SYN'' errors  |continuous|
|srv_rerror_rate  |% of connections that have ``REJ'' errors  |continuous|
|srv_diff_host_rate  |% of connections to different hosts  |continuous|
   */

  val NUM_COLS = 42
  val I_PROTOCOL_TYPE = 1
  val I_SERVICE = 2
  val I_FLAG = 3
  val I_LABEL = 41

  val features = Array(
    "duration",
    "protocol_type",
    "service",
    "flag",
    "src_bytes",
    "dst_bytes",
    "land",
    "wrong_fragment",
    "urgent",
    "hot",
    "num_failed_logins",
    "logged_in",
    "num_compromised",
    "root_shell",
    "su_attempted",
    "num_root",
    "num_file_creations",
    "num_shells",
    "num_access_files",
    "num_outbound_cmds",
    "is_hot_login",
    "is_guest_login",
    "count",
    "serror_rate",
    "rerror_rate",
    "same_srv_rate",
    "diff_srv_rate",
    "srv_count",
    "srv_serror_rate",
    "srv_rerror_rate",
    "srv_diff_host_rate",
    "label"
  )

  /*
    Attack Categories:
    DOS: denial-of-service, e.g. syn flood;
    R2L: unauthorized access from a remote machine, e.g. guessing password;
    U2R:  unauthorized access to local superuser (root) privileges, e.g., various ``buffer overflow'' attacks;
    probing: surveillance and other probing, e.g., port scanning.
   */
  val attackCategories = Array (
    "normal", "dos", "r2l", "u2r", "probe"
  )

  val attackTypes = Array(
    ("normal", "normal"),
    ("back", "dos"),
    ("buffer_overflow", "u2r"),
    ("ftp_write", "r2l"),
    ("guess_passwd", "r2l"),
    ("imap", "r2l"),
    ("ipsweep", "probe"),
    ("land", "dos"),
    ("loadmodule", "u2r"),
    ("multihop", "r2l"),
    ("neptune", "dos"),
    ("nmap", "probe"),
    ("perl", "u2r"),
    ("phf", "r2l"),
    ("pod", "dos"),
    ("portsweep", "probe"),
    ("rootkit", "u2r"),
    ("satan", "probe"),
    ("smurf", "dos"),
    ("spy", "r2l"),
    ("teardrop", "dos"),
    ("warezclient", "r2l"),
    ("warezmaster", "r2l")
  )

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
                     input: String = null,
                     k: Int = 2,
                     numIterations: Int = 10,
                     relDifWSSSE: Double = 0.01,
                     initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]


  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ClusterKMeans") {
      head("K-means clustering analysis on KDD Cup 1999 data set for network intrusion detection.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("relDifWSSSE")
        .text(s"relative difference of WSSSE between different run with k, default: ${defaultParams.relDifWSSSE}")
        .action((x, c) => c.copy(relDifWSSSE = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
        s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      arg[String]("<input>")
        .text("input paths of data")
        .required()
        .action((x, c) => c.copy(input = x))
      /* TODO: add more options:
runs is the number of times to run the k-means algorithm (k-means is not guaranteed to find a globally optimal solution, and when run multiple times on a given dataset, the algorithm returns the best clustering result).
initializationSteps determines the number of steps in the k-means|| algorithm.
epsilon determines the distance threshold within which we consider k-means to have converged.
       */
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"hw.ClusterKMeans with $params")
    val sc = new SparkContext(conf)

    println(s"\nK-Means Clustering parameters:\n$params")
//    Logger.getRootLogger.setLevel(Level.WARN)

    val lines = sc.textFile(params.input)
    val records = lines.map(_.split(',')).map(a => {a.update(I_LABEL, a(I_LABEL).stripSuffix(".")); a})
    val col_protocol_type = records.map(_(I_PROTOCOL_TYPE)).distinct.zipWithIndex.collect().toMap
    val col_service = records.map(_(I_SERVICE)).distinct.zipWithIndex.collect().toMap
    val col_flag = records.map(_(I_FLAG)).distinct.zipWithIndex.collect().toMap
    val col_label = records.map(_(I_LABEL)).distinct.zipWithIndex.collect().toMap

    val numProtocols = col_protocol_type.size
    println(s"\n| Number of Protocols | $numProtocols |")
    println(  s"| ------------------- | ------------- |")
    col_protocol_type.toSeq.sortBy(_._2).foreach(f => println(s"| ${f._2} | ${f._1} |"))

    val numServices = col_service.size
    println(s"\n| Number of Services | $numServices |")
    println(  s"| ------------------ | ------------ |")
    col_service.toSeq.sortBy(_._2).foreach(f => println(s"| ${f._2} | ${f._1} |"))

    val numFlags = col_flag.size
    println(s"\n| Number of Flags | $numFlags |")
    println(  s"| --------------- | --------- |")
    col_flag.toSeq.sortBy(_._2).foreach(f => println(s"| ${f._2} | ${f._1} |"))

    val numLabels = col_label.size
    println(s"\n| Number of Intrusion Labels | $numLabels |")
    println(  s"| -------------------------- | ---------- |")
    col_label.toSeq.sortBy(_._2).foreach(f => println(s"| ${f._2} | ${f._1} |"))

    // Convert string features into integer.
    def strFields2i (r: Array[String]): Array[String] = {
      r(I_PROTOCOL_TYPE) = col_protocol_type(r(I_PROTOCOL_TYPE)).toString
      r(I_SERVICE) = col_service(r(I_SERVICE)).toString
      r(I_FLAG) = col_flag(r(I_FLAG)).toString
      r(I_LABEL) = col_label(r(I_LABEL)).toString
      r
    }

    val fullData = records.map(r => strFields2i(r))
    val parsedData = fullData.map(r => r.dropRight(1)) // remove the label.

//    print("\n### 5#\n")
//    parsedData.collect().foreach(l => { l.foreach(s => print(s + ",")); println() } )

    val vect = parsedData.map(l => Vectors.dense(l.map(_.toDouble))).cache

    val numSamples = vect.count()
    println(s"\n| Number of Samples | $numSamples |")
    println(   "| ----------------- | ----------- |")
//    print("\n### 6#\n")
//    vect.collect().foreach(v => println(v.toString))

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val km = new KMeans()
      .setInitializationMode(initMode)
      .setMaxIterations(params.numIterations)

    println("\n| K | WSSSE | Time (sec) |")
    println(  "| - | ----- | ---------- |")

    var WSSSE = 1.0
    var WSSSE2 = 0.0
//    case class KMeansResult (k:Int, model:KMeansModel, err:Double, sec:Double)
    val res = for {
      k <- params.k until (numSamples/2).toInt
      if Math.abs(WSSSE - WSSSE2)/WSSSE > params.relDifWSSSE // relative difference of WSSSE between two run of different k values, then stop.
    } yield {
      val startTime = System.nanoTime()
      val model = km.setK(k).run(vect)
      val elapsed = (System.nanoTime() - startTime) / 1e9
      // Evaluate clustering by computing Within Set Sum of Squared Errors
      WSSSE2 = WSSSE
      WSSSE = model.computeCost(vect)
//      println(s"### 7# $k")
      (k, model, WSSSE, elapsed)
    }

    for ((k, model, err, sec) <- res) {
      println(s"| $k | $err | $sec |")
    }

    println("""
              || Notes |
              || ----- | -------------------------------- |
              || WSSSE | Within Set Sum of Squared Errors |
              |""".stripMargin)

    /*

    model.clusterCenters.foreach(
      v =>
    )

    println("Cluster Centers: ")
    Basic features of individual TCP connections.
    |feature name |description  |type |
    |---------------|---------------|-----|
    |duration  |length (number of seconds) of the connection  |continuous|
    |protocol_type  |type of the protocol, e.g. tcp, udp, etc.  |discrete|
    |service  |network service on the destination, e.g., http, telnet, etc.  |discrete|
    |src_bytes  |number of data bytes from source to destination  |continuous|
      |dst_bytes  |number of data bytes from destination to source  |continuous|
      |flag  |normal or error status of the connection  |discrete|
    |land  |1 if connection is from/to the same host/port; 0 otherwise  |discrete|
    |wrong_fragment  |number of ``wrong'' fragments  |continuous|
      |urgent  |number of urgent packets  |continuous|
      ||||
    |Content features within a connection suggested by domain knowledge.|
    |hot  |number of ``hot'' indicators |continuous|
      |num_failed_logins  |number of failed login attempts  |continuous|
    |logged_in  |1 if successfully logged in; 0 otherwise  |discrete|
    |num_compromised  |number of ``compromised'' conditions  |continuous|
      |root_shell  |1 if root shell is obtained; 0 otherwise  |discrete|
    |su_attempted  |1 if ``su root'' command attempted; 0 otherwise  |discrete|
    |num_root  |number of ``root'' accesses  |continuous|
      |num_file_creations  |number of file creation operations  |continuous|
    |num_shells  |number of shell prompts  |continuous|
      |num_access_files  |number of operations on access control files  |continuous|
    |num_outbound_cmds |number of outbound commands in an ftp session  |continuous|
      |is_hot_login  |1 if the login belongs to the ``hot'' list; 0 otherwise  |discrete|
    |is_guest_login  |1 if the login is a ``guest''login; 0 otherwise  |discrete|
    ||||
    |Traffic features computed using a two-second time window.|
    |count  |number of connections to the same host as the current connection in the past two seconds  |continuous|
      ||Note: The following  features refer to these same-host connections.|
    |serror_rate  |% of connections that have ``SYN'' errors  |continuous|
      |rerror_rate  |% of connections that have ``REJ'' errors  |continuous|
      |same_srv_rate  |% of connections to the same service  |continuous|
      |diff_srv_rate  |% of connections to different services  |continuous|
    |srv_count  |number of connections to the same service as the current connection in the past two seconds  |continuous|
      ||Note: The following features refer to these same-service connections.|
    |srv_serror_rate  |% of connections that have ``SYN'' errors  |continuous|
      |srv_rerror_rate  |% of connections that have ``REJ'' errors  |continuous|
      |srv_diff_host_rate  |% of connections to different hosts  |continuous|
      */

    // Save model
    // TODO: Change path into cmd option.
//    model.save(sc, "myModelPath")

    sc.stop()
  }
}
// scalastyle:on println

