package org.infinispan.query.dsl.embedded.impl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Ignore;
import org.junit.Test;

//todo [anistor] This is not an actual test, just an attempt to see what does lucene query parser find to be acceptable
// input. Should be removed once the new QL is complete.
@Ignore
public class LuceneQueryParserTest {

   private static final Analyzer STANDARD_ANALYZER = new StandardAnalyzer();

   private static final String DEFAULT_FIELD = "title";

   private Query parse(String qs) throws Exception {
      Query q1 = new StandardQueryParser().parse(qs, DEFAULT_FIELD);

      Query q2 = new QueryParser(DEFAULT_FIELD, STANDARD_ANALYZER).parse(qs);

      Query q = q2;

      System.out.print(qs + "  =>  " + q.toString() + "    " + q.getClass().getSimpleName() + "   ");
      if (q instanceof BooleanQuery) {
         System.out.print(((BooleanQuery) q).clauses());
         System.out.print("    " + ((BooleanQuery) q).clauses().get(0).getOccur());
      }
      System.out.println();
      return q;
   }

   @Test
   public void testParsing() throws Exception {
      Query qq = parse("name : (-foo +bar)");
      Query qqq = parse("name : (-(-foo))");

      Query q = parse("title: /zzzz/~3");
      parse("title: 'zzz?zzz'~3");

      parse("title: 'bicycle'^4 OR description : 'bicycle'^2");

      parse("name: (Adrian^3 Emmanuel)");
      parse("name: (Adrian Emmanuel)^3");

      //parse("name^4 : foo");  // INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name^4 : foo:

      parse("name : (+foo -boo)");
      parse("name : (foo -boo)");
      parse("name : (-foo -boo)");
      parse("name : (-foo boo)");

      parse("name : (xx) AND NOT surname:9");
      parse("name : (NOT 9)");
      parse("!name : 9");
      parse("name : 9");
      parse("NOT name : 9");
      parse("NOT name : (-9)");
      parse("+name : (-9)");
      parse("-name : 9");
      parse("-name : (!9)");
      parse("+name : (-9)");
      parse("-name : (-9)");
      parse("-name : (+9)");
      //parse("-name : (++aa)");  //INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse -name : (++aa):
      //parse("-name : (+-aa)");  //INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse -name : (+-aa):
      //parse("-name : (--aa)");  //INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse -name : (--aa):
      //parse("name : +9");  // INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name : +9:

      parse("(name > 6)^7");
      parse("(-name > 6)^7");
      //parse("name > 6^7");   // INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name > 6^7
      parse("name : 6^7");
      parse("(name : 6)^7");

      parse("name : aa~2^7~4");  // an odd case, second ~ is ignored  =>  name:aa~2^7.0
      //parse("name : \"aa\"~2^7~4");  // INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name : "aa"~2^7~4
      parse("name : \"aa\"~2^7");

      parse("name : aa~2^3");
      parse("name : aa^3~2");

      parse("* : *");
      //parse("name : *");   // LEADING_WILDCARD_NOT_ALLOWED: Leading wildcard is not allowed: name:*
      parse("name : [* *]");
      parse("name : [1 TO 7]");
      parse("name : [-1 TO 7]");
      //parse("name : [-1 TO 7]~3");  //INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name : [-1 TO 7]~3:
      parse("name : [1 7]");
      parse("name : (+adi)");
      parse("name : (-adi)");
//      parse("name : NOT adi");  // INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name : NOT adi:
      parse("name : (NOT adi)");
      parse("name : not(-adi)");
      //parse("name : -adi");   // INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse name : -adi:
      parse("name : (-[1 7])");

      parse("name : (+'Fritz' +(+or -and #not)^6 !'Purr')");
      parse("name : ('Fritz' or and not 'Purr'^5)^7");
      parse("eats");
      parse("f=eats");
      parse("+(eats)");
      parse("g > eats");
      parse("f:eats");
      parse("g:eats^7 or x:(fff xx)");
      parse("g:eats^7 and x:(fff xx)");

      parse("#g:eats^7");
      parse("g:#eats^7");
      parse("+g:eats^7");
      parse("-g:eats^7");
      parse("!g:eats");
      parse("g:eats");

      parse("g:eats and ");

      parse("f:(eats) ^6");
      parse("(f:eats)^6");
      parse("-(f:eats -g:drinks^4)^6");

      parse("f:(eats^6 drinks^3)");
      parse("f:(eats grass)^6");
      parse("(f:(eats grass))^6");
      parse("f:eats ~ 4");
      parse("f:2~4");
      parse("f:eats~0.89");   //!! 0.89 becomes 0

      parse("f:eats~4^5");
      parse("f:eats^5~ 6");    // !!! order of ^ and ~ does not matter here

      parse("f:(eats grass)");
      parse("f:(eats and grass)");

      parse("f:'eats grass'");
      parse("f:'eats grass'^7");
      parse("f:\"eats grass\"");
      parse("f:\"eats grass\"~5");
      parse("f:\"eats grass\"~5^7");
//      parse("f:\"eats grass\"^7~5");   // !!! order of ^ and ~ DOES matter here
      parse("f:[* TO *]");
      parse("f:[3 TO \"fff fff\"}^7");

      parse("f:aaa*b^7");
      parse("f:aaa*b~3");         // !!! slop is ignored
      parse("f:aaa*b^5~3");       // !!! slop is ignored
   }
}
