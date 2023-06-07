package com.spark.sql.training.execution

import com.spark.sql.training.data.Inputs
import com.spark.sql.training.config.Parameters
import com.typesafe.config.Config
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.{RDD}

object ResilientDistributedDataset extends Inputs with Parameters {

  def rddTraining(config: Config, spark: SparkSession): Unit = {
    basicOperations(spark)


  }

  def basicOperations(spark: SparkSession): Unit = {

    /** Declare an empty RDD
     * Use case: To create a single empty row to add this row inside the dataframe
     */
    val rddString = spark.sparkContext.emptyRDD[String]
    val rddFourPartitions = spark.sparkContext.parallelize(Seq.empty[String], 4)
    val num_of_partition = rddFourPartitions.getNumPartitions
    println(s"Number of partitions: $num_of_partition")

    /** Create a simple RDD
     * Use case: To prepare a POC or Demo or to create our unitary test
     */
    val columns = Seq("language","users_count")
    val data = Seq(("Rise", 1000), ("Grapes", 800), ("Sugar", 100))
    val rddParallelize: RDD[(String, Int)] = spark.sparkContext.parallelize(data)
    rddParallelize.foreach(println)
    //val df = spark.createDataFrame(rddParallelize).toDF(data:_*)
    //df.show()



    //La funcion readRDD se se creo solo para visualizar el RDD
    // descomentar para ver el resultado
    //readRDD(rdd)

    //Leer un archivo y ponerlo dentro de un RDD
    // De donde viene la variable inputPath?
    //val rddTextfile = spark.sparkContext.textFile(inputPath)
    //imprime todos los valores dentro del RDD
    //rddTextfile.collect().foreach(println)
    //nos muestra el tipo de dato dentro de la variable rddTextfile
    //println(rddTextfile)
    //Descomentar para ver como transforma el RDD en Dataframe
    //spark.createDataFrame(rdd).show()

    //Creacion de un RDD vacio sin particion. Esto puede ser utilizado para artificios.
    //Por ejemplo cuando quieres crear un dataframe vacio o quieres escribir un archivo controlador
    //para idenfiticar si el proceso termino
    //val rddEmpty = spark.sparkContext.emptyRDD
    //val rddString = spark.sparkContext.emptyRDD[String]

    //println(rddEmpty)
    //println(rddString)

    //Crea un RDD con particion
    //Por defecto cuando no pasamos el valor de particion, el sistema automaticamente partira el RDD en la cantidad
    //de nodos o particiones que vea conveniente
    //val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    //println(rdd2)

    //Podemos pasar los nodos en los que se paralelizara la creacion del RDD
    val rdd3 = spark.sparkContext.parallelize(Seq.empty[String], 4)
    //println(rdd3)

    //Tambien podemos reparticionar el rdd una vez creado.
    //val reparticionRdd = rddTextfile.repartition(2)
    //val coalesceRdd = rddTextfile.coalesce(4)
    //Descomentar para ver los numeros de particiones
    //println("re-particion:" + reparticionRdd.getNumPartitions)
    //println("coalesce:" + coalesceRdd.getNumPartitions)

    //NOTA: Podemos reparticionar con repartition o con coalesce.

    // repartition => metodo que junta los datos de todos los nodos
    //Ejemplo: se tiene data en 4 nodos y ejecutamos repartition(2), movera todos los 4 nodos y
    // juntara en 2 nodos resultantes

    // coalesce => metodo que junta los datos utilizando la minimia cantidad de nodos
    //Ejemplo: se tiene data en 4 nodos y ejecutamos coalesce(2), solo va a tomar data de 2 nodos y
    // movera a donde existen los otros 2 nodos
    //
    //    //RDD Operations:
    //    //-------------------
    //    // Transformation: crea un nuevo dataset desde uno ya existente
    //    // Actions: retorna un valor despues de ejecutar un calculo sobre el dataset
    //
    //    val lines = spark.sparkContext.textFile(pathRDDInput)
    //    //Ejemplo de transformacion = pasa cada elemento atravez de la funcion map y devuelve un nuevo RDD
    //    val lineLengths = lines.map(s => s.length)
    //    println(lineLengths)
    //    //Ejemplo de action = pasa la data y regresa la suma o funcion agregada de suma al controlador
    //    val totalLength = lineLengths.reduce((a, b) => a + b)
    //    //println(totalLength)
    //
    //
    //    //Shuffle operations gropByKey, reduceByKey, join

  }

}
