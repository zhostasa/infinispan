/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.infinispan.commons.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads a PEM file and converts it into a list of DERs so that they are imported into a {@link KeyStore} easily.
 */
final class PemReader {

   private static final Pattern CERT_PATTERN = Pattern.compile(
         "-+BEGIN\\s+.*CERTIFICATE[^-]*-+(?:\\s|\\r|\\n)+" + // Header
               "([a-z0-9+/=\\r\\n]+)" +                    // Base64 text
               "-+END\\s+.*CERTIFICATE[^-]*-+",            // Footer
         Pattern.CASE_INSENSITIVE);
   private static final Pattern KEY_PATTERN = Pattern.compile(
         "-+BEGIN\\s+.*PRIVATE\\s+KEY[^-]*-+(?:\\s|\\r|\\n)+" + // Header
               "([a-z0-9+/=\\r\\n]+)" +                       // Base64 text
               "-+END\\s+.*PRIVATE\\s+KEY[^-]*-+",            // Footer
         Pattern.CASE_INSENSITIVE);

   static byte[][] readCertificates(File file) throws CertificateException {
      try {
         InputStream in = new FileInputStream(file);

         try {
            return readCertificates(in);
         } finally {
            Util.close(in);
         }
      } catch (FileNotFoundException e) {
         throw new CertificateException("could not find certificate file: " + file);
      }
   }

   static byte[][] readCertificates(InputStream in) throws CertificateException {
      String content;
      try {
         content = readContent(in);
      } catch (IOException e) {
         throw new CertificateException("failed to read certificate input stream", e);
      }

      List<byte[]> certs = new ArrayList<>();
      Matcher m = CERT_PATTERN.matcher(content);
      int start = 0;
      for (; ; ) {
         if (!m.find(start)) {
            break;
         }
         certs.add(Base64.getDecoder().decode(m.group(1)));

         start = m.end();
      }

      if (certs.isEmpty()) {
         throw new CertificateException("found no certificates in input stream");
      }

      return certs.toArray(new byte[certs.size()][]);
   }

   static byte[] readPrivateKey(File file) throws KeyException {
      try {
         InputStream in = new FileInputStream(file);

         try {
            return readPrivateKey(in);
         } finally {
            Util.close(in);
         }
      } catch (FileNotFoundException e) {
         throw new KeyException("could not fine key file: " + file);
      }
   }

   static byte[] readPrivateKey(InputStream in) throws KeyException {
      String content;
      try {
         content = readContent(in);
      } catch (IOException e) {
         throw new KeyException("failed to read key input stream", e);
      }

      Matcher m = KEY_PATTERN.matcher(content);
      if (!m.find()) {
         throw new KeyException("could not find a PKCS #8 private key in input stream" +
               " (see http://netty.io/wiki/sslcontextbuilder-and-private-key.html for more information)");
      }

      return Base64.getDecoder().decode(m.group(1));
   }

   private static String readContent(InputStream in) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
         byte[] buf = new byte[8192];
         for (; ; ) {
            int ret = in.read(buf);
            if (ret < 0) {
               break;
            }
            out.write(buf, 0, ret);
         }
         return out.toString(StandardCharsets.US_ASCII.name());
      } finally {
         Util.close(out);
      }
   }

   public static X509Certificate[] toX509Certificates(File file) throws CertificateException {
      if (file == null) {
         return null;
      }
      return getCertificatesFromBuffers(PemReader.readCertificates(file));
   }

   private static X509Certificate[] getCertificatesFromBuffers(byte[][] certs) throws CertificateException {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      X509Certificate[] x509Certs = new X509Certificate[certs.length];

      int i = 0;

      for (; i < certs.length; i++) {
         InputStream is = new ByteArrayInputStream(certs[i]);
         try {
            x509Certs[i] = (X509Certificate) cf.generateCertificate(is);
         } finally {
            try {
               is.close();
            } catch (IOException e) {
               // This is not expected to happen, but re-throw in case it does.
               throw new RuntimeException(e);
            }
         }
      }

      return x509Certs;
   }

   public static KeyStore buildKeyStore(File pem)
         throws KeyStoreException, NoSuchAlgorithmException,
         CertificateException, IOException {
      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(null, null);
      X509Certificate[] certChain = toX509Certificates(pem);

      for (int i = 0; i < certChain.length; i++) {
         String alias = Integer.toString(i);
         ks.setCertificateEntry(alias, certChain[i]);
      }
      return ks;
   }

   private PemReader() {
   }
}
