import java.sql.DriverManager
import java.util.concurrent.Executors

import dispatch.{Http, host, as, Future}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import scalikejdbc.{HasExtractor, SQL, AutoSession, ConnectionPool}

import scala.concurrent.ExecutionContext

//import scala.concurrent.ExecutionContext.Implicits.global


trait CustomExecutionContext {
  val fixedThreadPool = Executors.newFixedThreadPool(30)
 implicit val executionContext = ExecutionContext.fromExecutor(fixedThreadPool)
}

trait ElasticSearch {

  val settings = ImmutableSettings.settingsBuilder()
    .put("cluster.name","tr_cloud_labs_es")
    .put("client.transport.sniff",true)
    .build()

  val elasticClient =
    new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress("localhost",9300))

  // close client
  def closeElasticClient = elasticClient.close()


  def putToElastic(id:String, data :String):Boolean = {
    val isCreated: Boolean = elasticClient
      .prepareIndex("records", "trial")
      .setSource(data).setId(id)
      .execute.actionGet
      .isCreated
    if(isCreated){
      println("Elastic success : " +id)
    }else{
      println("Elastic failure : "+id)
    }
    isCreated
  }


}

trait TrialsDao {

  import scalikejdbc._

  val driver = "oracle.jdbc.driver.OracleDriver"
  val url = "jdbc:oracle:thin:@//localhost:1521/dev"
  val username = "test"
  val password = "test"
  val TRIAL_ID_QUERY="select trial_id from ff_trial"

  Class.forName(driver)
  ConnectionPool.singleton(url, username, password)

  implicit val session = AutoSession

  def getTrialIds ={
   sql"select trial_id from ff_trial"
  }


}

trait RecordClient extends CustomExecutionContext{
  val hostName = host("localhost", 9092)
  val fullUrl= hostName / "recordserver" / "retrieve" / "Trialv1.json"

  /**
   * Get response for id from record server
   * @param id
   * @return
   */
  def getRecordForId(id : String):Future[String] = {
    val getRequest = fullUrl.GET
    val urlWithParams = getRequest.addQueryParameter("idList",id).setContentType("application/json","UTF-8")
    Http(urlWithParams OK as.String)(executionContext)
  }
}

object ElasticRecordLoader extends RecordClient with TrialsDao with ElasticSearch{


  /**
   * Index elastic search
   * @param id
   * @param data
   */
  def indexElastic(id :String , data : Future[String]) = {
    def pf = (id :String) => new PartialFunction[String, Boolean] {
      override def isDefinedAt(x: String): Boolean = !x.isEmpty
      override def apply(v1: String): Boolean = {
        putToElastic(id, v1)
      }
    }
    
    data.onSuccess(pf(id))(executionContext)
  }

  /**
   * Main Loader method
   * @param args
   */
  def main(args: Array[String]) {
    getTrialIds.foreach{wrs =>
      val id = wrs.string(1)
      val json = getRecordForId(id)
      indexElastic(id, json)
    }
    println("Done !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

  }

}
