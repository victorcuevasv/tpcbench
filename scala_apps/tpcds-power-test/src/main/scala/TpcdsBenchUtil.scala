import java.net.URI
import java.net.URL

object TpcdsBenchUtil {

  def addPathToURI(uriStr: String, pathStr: String) = {
    val uri = new URI(uriStr)
    val scheme = uri.getScheme()
    val uriHttp = uri.toString.replace(s"${scheme}://", "http://")
    val urlHttp = new URL(uriHttp)
    val mergedURL = new URL(urlHttp, pathStr)
    val mergedURI = mergedURL.toString.replace("http://", s"${scheme}://")
    mergedURI.toString
  }

}
