#!/local/groovy/bin/groovy

/**
 * The methods in this file may be executed directly from a native Groovy
 * file.  Here is a sample invocations that work from another Groovy file:
 * <PRE><CODE>
 *   println DocBookUtil.installStyleSheets(new File('/tmp/drivertst'), false)
 * </CODE></PRE>
 * Just change 'false' to 'true' to install the entire stylesheet collection.
 * Don't forget to set Java system property 'grape.config' if you want to
 * override Grape's default one.
 */

@Grab(group='org.ccil.cowan.tagsoup', module='tagsoup', version='1.2.1')
import groovy.util.slurpersupport.GPathResult
import org.ccil.cowan.tagsoup.Parser

/**
 * Unfortunately, Grape only supports Java system properties as properties
 * with the antsettings.xml file.
 * Therefore, if your antsettings.xml file has any ${references} which are
 * not system propertyes, you must add the properties to
 * System.getProperties() before calling this method.
 *
 * @return newly created directory name, relative to pDir.
 */
public static String installStyleSheets(File pDir, boolean allSheets) {
    String siteUrlString = 'http://sourceforge.net'
    String indexPathString = '/projects/docbook/files/docbook-xsl-ns'
    String latestVersionPath = null

    (siteUrlString + indexPathString).toURL().withReader('UTF-8') {
        GPathResult html = (new XmlSlurper(new Parser()).parse(it))
        GPathResult gPathR = html.'**'.find {
            it.name() == 'table' && it.@id == 'files_list'
        }
        GPathResult anchorGPathR = gPathR.tbody.tr[0].th.a
        latestVersionPath = anchorGPathR.@href.text()
    }

    //println "($latestVersionPath)"
    java.util.regex.Matcher m = latestVersionPath =~  /.*\/([-\w.]+)\//
    assert m.matches() :
        "Failed to parse a version from path:  $latestVersionPath"

    String latestVer = m.group(1)
    //println "($latestVer)"

    String zipFileName = 'docbook-xsl-ns-' + latestVer + '.zip'
    //println "($zipFileName)"
    String zipUrl = "http://sourceforge.net/projects/docbook/files/docbook-xsl-ns/$latestVer/$zipFileName/download"
    //println "($zipUrl)"

    File localZip = new File(System.properties['java.io.tmpdir'], zipFileName)
    BufferedOutputStream bos =
            new BufferedOutputStream(new FileOutputStream(localZip))
    localZip.deleteOnExit()
    zipUrl.toURL().withInputStream() { bos << it }
    bos.close()
    AntBuilder ant = new AntBuilder()

    if (allSheets) {
        File newDir = new File(pDir, "docbook-xsl-ns-$latestVer")
        assert !newDir.exists() :
            "New target directory already exists: $newDir.absolutePath"
        ant.unzip(src:localZip.absolutePath,
                dest:pDir.absolutePath, overwrite:'false')
        if (!new File(newDir, 'images').isDirectory())
            throw new IOException(
                    "Extraction into '$newDir.absolutePath' failed")
        return "docbook-xsl-ns-$latestVer"
    }
    File destDir = new File(pDir, "xsl-ns-images-$latestVer")
    assert !destDir.exists() :
        "New target directory already exists: $destDir.absolutePath"
    ant.unzip(src:localZip.absolutePath,
            dest:destDir.absolutePath, overwrite:'false') {
        patternset { include(name: "docbook-xsl-ns-$latestVer/images/**") }
        regexpmapper(from:'^[^/]+/images/(.+)$', to:/\1/)
    }
    if (!destDir.isDirectory())
        throw new IOException("Extraction to '$destDir' failed")
    return "xsl-ns-images-$latestVer"
}
