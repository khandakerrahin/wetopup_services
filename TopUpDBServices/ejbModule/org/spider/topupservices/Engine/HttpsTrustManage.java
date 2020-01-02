package org.spider.topupservices.Engine;

import java.io.IOException;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


public class HttpsTrustManage {
	public Boolean TrustThyManager() throws IOException {	
		boolean retval=true;
		//Only to be used fro self signed certificate(https)
		javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
			    new javax.net.ssl.HostnameVerifier(){
			 
			        public boolean verify(String hostname,
			                javax.net.ssl.SSLSession sslSession) {
			            if (hostname.equals("10.10.1.13")) {//10.10.1.13
			                return true;
			            } else if (hostname.equals("10.10.1.12")) {//10.10.1.13
			                return true;
			            }
			            return false;
			        }
			    });
	
		TrustManager[] trustAllCerts = new TrustManager[]{
			    new X509TrustManager() {
			        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			            return null;
			        }
			        public void checkClientTrusted(
			            java.security.cert.X509Certificate[] certs, String authType) {
			        }
			        public void checkServerTrusted(
			            java.security.cert.X509Certificate[] certs, String authType) {
			        }
			    }
		};
		SSLContext sc;
		try {
		     sc = SSLContext.getInstance("SSL");
		    sc.init(null,trustAllCerts, new java.security.SecureRandom());
		    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		} catch (Exception e) {
			 retval=false;
		}
return retval;
	}
	
	
}

