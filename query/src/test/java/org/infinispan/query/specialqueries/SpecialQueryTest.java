package org.infinispan.query.specialqueries;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * This test is designed to verify the used Lucene version is not affected by CVE-2017-12629. See
 * also: JDG-1311, SOLR-11477, https://bugzilla.redhat.com/show_bug.cgi?id=1501529,
 * http://lucene.472066.n3.nabble.com/Re-Several-critical-vulnerabilities-discovered-in-Apache-Solr-XXE-amp-RCE-tt4358355.html
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "query.specialqueries.SpecialQueryTest")
public class SpecialQueryTest {

   @Test
   public void verifyExternalXMLEntitiesDisabledInQueryParsing() {
      String xmlQuery = "<!DOCTYPE a SYSTEM \"http://localhost:4444/executed\"><a></a>";
      InputStream is = new ByteArrayInputStream(xmlQuery.getBytes(StandardCharsets.UTF_8));
      try {
         Query query = new CoreParser("title", null).parse(is);
         Assert.fail();
      } catch (ParserException e) {
         //N.B. the exact message matters for the purpose of this test!
         //Therefore not using the test helpers.
         Assert.assertEquals(
               "Error parsing XML stream: org.xml.sax.SAXException: External Entity resolving unsupported:  publicId=\"null\" systemId=\"http://localhost:4444/executed\"",
               e.getMessage());
         return;
      }
      Assert.fail();
   }

}
