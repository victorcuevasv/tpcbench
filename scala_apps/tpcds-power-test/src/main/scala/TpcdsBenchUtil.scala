import java.net.URI
import java.io.File
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object TpcdsBenchUtil {

  def addPathToURI(uriStr: String, pathStr: String) = {
    val uri = new URI(uriStr)
    val scheme = uri.getScheme
    val authority = uri.getAuthority
    val uriPath = uri.getPath
    // This constructor adds query (?) and anchor (#) characters that need to be later removed
    val extendedURI = new URI(scheme, authority, new File(uriPath, pathStr).getPath, "", "")
    val extendedURIstr = extendedURI.toString
    // Remove the query (?) and anchor (#) added characters
    extendedURIstr.substring(0, extendedURIstr.length() - 2)
  }

  def saveStringToS3(baseURL: String, filePath: String, contents: String) = {
    try {
        val s3Client: AmazonS3 = AmazonS3ClientBuilder.defaultClient
        val objURI = new URI(addPathToURI(baseURL, filePath))
        val bucketName = objURI.getAuthority
        val absPath = objURI.getPath
        val objKey = absPath.substring(1, absPath.length)
        s3Client.putObject(bucketName, objKey, contents)
    }
    catch {
      case e: Exception => {  
        println(e.getMessage)
      }
    }
  }


}
