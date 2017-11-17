package br.cefet.outlierdetector

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import scala.math.BigDecimal.RoundingMode
import org.apache.spark.sql.SQLImplicits
import collection.JavaConverters._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType }
import scala.collection.mutable._

object OutlierDetector {

  case class Despesa(id: Long, txNomeParlamentar: String, idecadastro: String, nuCarteiraParlamentar: String,
                     sgUf: String, sgPartido: String, txtDescricao: String, vlrDocumento: BigDecimal, vlrGlosa: BigDecimal,
                     despesa: BigDecimal, numMes: String, numAno: String)

  private final val appName = "Outlier Detector"

  private final val logPath = "/home/gabriel/outliers/" + System.currentTimeMillis()

  val estruturaCSV = StructType(Array(
    StructField("numAno", StringType),
    StructField("txtDescricao", StringType),
    StructField("despesa", DoubleType),
    StructField("id", StringType)))

  case class Execucao(nomeLog: String, descricao: String, tempoInicial: Long, tempoFinal: Long)

  val execucao: MutableList[Execucao] = MutableList()

  def main(args: Array[String]) {

    val tempoInicial = System.currentTimeMillis()

    var tempoIni = tempoInicial

    val conf = new SparkConf().setAppName("Outlier Detector").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val session = SparkSession.builder()
      .appName("Outlier Detector")
      .config("spark.executor.memory", "1g")
      .config("spark.executor.cores", "1")
      .config("spark.driver.memory", "512m")
      .config("spark.debug.maxToStringFields", "255")
      .config("spark.eventLog.enabled", "true")
      .getOrCreate()
    import session.sqlContext.implicits._
    var tempoFim = System.currentTimeMillis()

    execucao += Execucao("CONFIG_SPARK_SESSION", "Configura Spark Session", tempoIni, tempoFim)

    tempoIni = System.currentTimeMillis()
    val sqlDF = session.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://asgard/outlier_detection")
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "public.despesa")
      .option("user", "mestre")
      .option("password", "S3cur1ty")
      .load()
    sqlDF.createOrReplaceTempView("DESPESA")
    tempoFim = System.currentTimeMillis()
    execucao += Execucao("CONFIG_SPARK_SQL", "Configurando conexao com o Postgresql e APACHE SPARK", tempoIni, tempoFim)

    tempoIni = System.currentTimeMillis()
    //sql semelhante ao executado pela outra aplicacao
    val sql = "select d.id, d.txNomeParlamentar, d.idecadastro, d.nuCarteiraParlamentar," +
      "    		d.sgUf, d.sgPartido, d.txtDescricao, d.vlrDocumento, d.vlrGlosa," +
      "    		d.vlrDocumento -  d.vlrGlosa as despesa, d.numMes, d.numAno FROM DESPESA d" +
      "				where d.numAno BETWEEN '2012' AND '2015'"

    val result = session.sql(sql)
    result.createOrReplaceTempView("DESPESA_FILTRADA")

    result.persist()

    result.show()
    tempoFim = System.currentTimeMillis()
    val encoder = Encoders.product[Despesa]
    execucao += Execucao("FILTRA_EXERCICIOS", "Filtra os exercicios a executar", tempoIni, tempoFim)

    val dsDespesas: Dataset[Despesa] = result.as(encoder)

    //Projecao
    tempoIni = System.currentTimeMillis()
    val despesasTipoAno = dsDespesas.select("numAno", "txtDescricao", "despesa", "id").rdd

    tempoFim = System.currentTimeMillis()
    execucao += Execucao("PROJECAO_DESPESAS", "Faz a projecao do ano, descricao e valor da despesa", tempoIni, tempoFim)
    
    tempoIni = System.currentTimeMillis()
    //Calcula media, despesa total e numero de elementos
    //primeiro guarda o id para o futuro
    val despesasPairRdd = despesasTipoAno.map(x => ((x(0).toString(), x(1).toString()), (BigDecimal(x(2).toString()), 1, x(3).toString()))).persist()
    //remove o id no map e faz o reduce para calcular o numero de itens e despesa total
    val despesasSumCount = despesasPairRdd.map(x => ((x._1._1, x._1._2), (x._2._1, x._2._2)))
      .reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))

    //(numAno, txtDescricao) , (despesa, numElementos, media)
    val despesasSumCountMean = despesasSumCount.map(x => ((x._1._1, x._1._2), (x._2._1, x._2._2, x._2._1 / x._2._2)))
    
    tempoFim = System.currentTimeMillis()
    execucao += Execucao("CALCULA_MEDIA_COUNT_GR_ANO_TIPO", "Calcula media e numero elementos por ano e descricao da despesa", tempoIni, tempoFim)
    
    tempoIni = System.currentTimeMillis()
    val chavesAnoTipo = session.sql("select distinct numAno, txtDescricao from DESPESA_FILTRADA").collect()
    tempoFim = System.currentTimeMillis()
    execucao += Execucao("PROJECAO_DISTINCT_ANO_TXTDESCRICAO", "Busca os anos e tipos de despesa existentes", tempoIni, tempoFim)
    var i = 0

    chavesAnoTipo.foreach(k => {
      val ano = k(0).toString()
      val despesa = k(1).toString()
      tempoIni = System.currentTimeMillis()
      //filtra e ordena para obter os percentis e quartis
      val despesasAnoTipoTmp = despesasPairRdd
        .filter(x => x._1._1.toString().equals(ano) && x._1._2.toString().equals(despesa))
        .sortBy(_._2._1)
        .persist()
        
      val despesas = despesasAnoTipoTmp.collect()
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("FILTRA_POR_ANO_DESCRICAO", "Filtra despesas por ano e tipo de despesa", tempoIni, tempoFim)
      //obtem a media e o numero de elementos
      tempoIni = System.currentTimeMillis()
      val despesasSumCountMeanFiltro = despesasSumCountMean
        .filter(x => x._1._1.toString().equals(ano) && x._1._2.toString().equals(despesa))
      val despesasSumCountMeanMap = despesasSumCountMeanFiltro
        .collectAsMap()
      val summ = despesasSumCountMeanMap(ano, despesa)
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("FILTRA_MEDIA_NUMERO_TOTAL_DESPESA_POR_ANO_TIPO", "Filtra média e numero de despesas por ano e tipo de despesa", tempoIni, tempoFim)
      

      
      //calcula a variancia
      //(numAno, txtDescricao), (despesa, variancia)
      tempoIni = System.currentTimeMillis()
      val distancias = despesasAnoTipoTmp
        .map(x => ((x._1._1, x._1._2), (Math.pow(x._2._1.doubleValue() - summ._3.doubleValue(), 2))))
        .reduceByKey(_ + _)
        .first()
      val variancia = distancias._2 / summ._2
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("CALCULA_VARIANCIA", "Calcula a variância", tempoIni, tempoFim)
      
        
      tempoIni = System.currentTimeMillis()  
      val min = despesas(0)._2._1
      val max = despesas(despesas.length - 1)._2._1

      val q1Position = Math.floor(despesas.length * 0.25).intValue()
      val q2Position = Math.floor(despesas.length * 0.5).intValue()
      val q3Position = Math.floor(despesas.length * 0.75).intValue()

      val q1 = despesas(q1Position)._2._1
      val q2 = despesas(q2Position)._2._1
      val q3 = despesas(q3Position)._2._1
      
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("CALCULAR_QUARTIS_PERCENTIS_ORDENADOS", "Define os quartis, percentis, max, min com os dados ja ordenados", tempoIni, tempoFim)  
      
      tempoIni = System.currentTimeMillis() 
      //Normaliza Z-Score
      val despesasZscoreRDD = despesasAnoTipoTmp.map((x) => ((x._1), ((x._2._1 - summ._3) / Math.sqrt(variancia))))
      //.persist()
      val despesasZScore = despesasZscoreRDD.collect()
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("NORMALIZACAO_ZSCORE", "Realiza a normalização z-score", tempoIni, tempoFim)  
      
      tempoIni = System.currentTimeMillis() 
      //Busca quartis e percentis normalizados
      val q1Norm = despesasZScore(q1Position)._2
      val q2Norm = despesasZScore(q2Position)._2
      val q3Norm = despesasZScore(q3Position)._2

      val p875Position = Math.floor(despesas.length * 0.875).intValue()
      val p875Norm = despesasZScore(p875Position)._2

      val p125Position = Math.floor(despesas.length * 0.125).intValue()
      val p125Norm = despesasZScore(p125Position)._2

      val oc = ((p875Norm - 2) * (q2Norm + p125Norm)) / (p875Norm + p125Norm)
      val outlierSuperiorMinimoNorm = ((q3Norm + 1.5) * (q3Norm - q1Norm)) * Math.pow(Math.E, 0.5 * oc.doubleValue())

      val outlierSuperiorMinimo = (outlierSuperiorMinimoNorm * Math.sqrt(variancia)) + summ._3
      
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("CALCULAR_VALOR_OUTLIER_MAXIMO", "Define os limites para outlier superior", tempoIni, tempoFim)
      
      tempoIni = System.currentTimeMillis()
      val outliersSuperiores = despesasAnoTipoTmp
        .map((x) => Row(x._1._1, x._1._2, x._2._1.toDouble, x._2._3))
        .filter((x) => x.get(2).asInstanceOf[Double] > outlierSuperiorMinimo)
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("FILTRAR_OUTLIERS_SUPERIORES", "Filtra os outliers superiores", tempoIni, tempoFim)
      
      tempoIni = System.currentTimeMillis()
      session.createDataFrame(outliersSuperiores, estruturaCSV)
        .write
        .option("header", "true")
        .option("delimiter", ";")
        .mode(SaveMode.Append).csv(logPath)
      tempoFim = System.currentTimeMillis()
      execucao += Execucao("GRAVAR_ARQUIVO_CSV_OUTLIERS_SUPERIORES", "Grava em disco os outliers superioes", tempoIni, tempoFim)  
     
    })
    val tempoFinal = System.currentTimeMillis()
    println("Tempo total: " + (tempoFinal - tempoInicial))
    execucao += Execucao("EXECUCAO_TOTAL", "Tempo de execucao total", tempoInicial, tempoFinal)  
    execucao.toDS()
        .coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", ";")
        .mode(SaveMode.Append).csv(logPath + "/log_execucao")

    

  }

}