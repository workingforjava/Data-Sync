package com.myretail.datasync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;

public class DataSyncClient {

	public static int cto = 10000;
	public static int soto = 10000;
	public static int maxConnection = 10000;
	public static int maxConPerHost = 10000;
	public static final String endPointURI = "http://localhost:8080/MyRetail/myretail/supply/toStore";

	public void submitJob(String fName) throws Exception {
		final String dsFile = fName;
		final HttpClient httpClient = createMultiThreadedHttpClient();

		ExecutorService executor = Executors.newFixedThreadPool(5);
		System.out.println("Data sync feed file = " + dsFile);
		File dir = new File(dsFile);
		if (!dir.exists() || !dir.isFile()) {
			throw new Exception(dsFile + " is not readable");
		}
		BufferedReader bReader = null;
		bReader = new BufferedReader(new FileReader(dsFile));
		String line = bReader.readLine();
        int i=0;
		while (line != null) {
			threadSubmission t = this.new threadSubmission(httpClient, line);
			executor.execute(t);
			line = bReader.readLine();
			i++;
		}
		bReader.close();
		executor.shutdown();
		executor.awaitTermination(2, TimeUnit.HOURS);
		System.out.println("Total product updated in stores"+i);
	}

	public static void main(String[] args) throws Exception {
		DataSyncClient s = new DataSyncClient();
		if (args[0] != null && !args[0].isEmpty()) {
			s.submitJob(args[0]);
		} else {
			System.out.println("Please run with valid file path.........");
		}
	}

	private static String sendRequest(HttpClient httpClient, String endPoint,
			String requestString) throws Exception {
		PostMethod post = new PostMethod(endPoint);

		try {
			RequestEntity requestEntity = null;
			requestEntity = new StringRequestEntity(requestString,
					"application/xml", "utf-8");
			post.setRequestEntity(requestEntity);
			System.out.println("Line Item to store Invetory: " + requestString);

			int code = httpClient.executeMethod(post);

			if (code != 200) {
				throw new Exception("service connection exception code :"
						+ code);
			}
			return "";
		} finally {
			post.releaseConnection();
		}
	}

	private static HttpClient createMultiThreadedHttpClient() {
		HttpClient httpClient = new HttpClient(
				new MultiThreadedHttpConnectionManager());
		httpClient.getHttpConnectionManager().getParams()
				.setConnectionTimeout(cto);
		httpClient.getHttpConnectionManager().getParams().setSoTimeout(soto);
		httpClient.getHttpConnectionManager().getParams().setTcpNoDelay(true);
		httpClient.getHttpConnectionManager().getParams()
				.setMaxTotalConnections(maxConnection);
		httpClient.getHttpConnectionManager().getParams()
				.setDefaultMaxConnectionsPerHost(maxConPerHost);
		return httpClient;
	}

	private class threadSubmission implements Runnable {

		private String req;
		private HttpClient httpClient;

		threadSubmission(HttpClient httpClient, String str) {
			this.req = str;
			this.httpClient = httpClient;
		}

		@Override
		public void run() {
			try {
				sendRequest(httpClient, endPointURI, req);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
