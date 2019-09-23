package cs523.sentiment;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class SentimentClient
{
  public String request(String comment) throws IOException {
    byte[] postData = comment.getBytes(StandardCharsets.UTF_8);
    int postDataLength = postData.length;
    
    String queryParams = String.format("properties=%s", new Object[] {
          
          URLEncoder.encode("{\"annotators\":\"sentiment\",\"outputFormat\":\"text\"}", "utf-8")
        });
    URL url = new URL("http", "sentiment", 9000, "?" + queryParams);
    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
    conn.setDoOutput(true);
    conn.setInstanceFollowRedirects(false);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/x-protobuf");
    conn.setRequestProperty("charset", "utf-8");
    conn.setRequestProperty("Content-Length", Integer.toString(postDataLength));
    conn.setUseCaches(false);
    
    conn.connect();
    conn.getOutputStream().write(postData);
    conn.getOutputStream().flush();
    
    try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      wr.write(postData);
    } 

    
    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    
    StringBuffer response = new StringBuffer();
    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();
    
    return response.toString();
  }
  public static void main(String[] args) throws IOException {
    String result = (new SentimentClient()).request("bad job").substring(34);
    if (result.startsWith("Positive")) {
      System.out.println("Positive");
    }
    if (result.startsWith("Negative"))
      System.out.println("Negative"); 
  }
}