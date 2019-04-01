package com.fabric.query.fqrest.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DRestUtil {
    final static Logger logger = LoggerFactory.getLogger(DRestUtil.class);

    @ResponseBody
    public ResponseEntity<String> queryGet(String url, String contentType, String dCookie) {
        String result = "";
        HttpHeaders headers = new HttpHeaders();
        HttpClient client = HttpClientBuilder.create().build();

        try {
            HttpGet getReq = new HttpGet(url);
            getReq.addHeader("Content-Type", contentType);
            if (dCookie != null)
                getReq.addHeader("Authorization", "_dremio" + dCookie);
            HttpResponse response = client.execute(getReq);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        headers.add("Content-Type", "Application/Json;charset=utf-8");
        return new ResponseEntity<String>(result, headers, HttpStatus.OK);
    }

    @ResponseBody
    public ResponseEntity<String> queryPost(String url, String contentType, String dCookie, String reqParams) {
        String result = "";
        HttpHeaders headers = new HttpHeaders();
        HttpClient client = HttpClientBuilder.create().build();

        try {
            HttpPost post = new HttpPost(url);
            StringEntity params = new StringEntity(reqParams);
            post.setEntity(params);
            post.addHeader("Content-Type", contentType);
            if (dCookie != null)
                post.addHeader("Authorization", "_dremio{" + dCookie + "}");
            HttpResponse response = client.execute(post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        headers.add("Content-Type", "Application/Json;charset=utf-8");
        return new ResponseEntity<String>(result, headers, HttpStatus.OK);
    }

    @ResponseBody
    public ResponseEntity<String> queryDelete(String url, String contentType, String dCookie) {
        String result = "";
        HttpHeaders headers = new HttpHeaders();
        HttpClient client = HttpClientBuilder.create().build();

        try {
            HttpDelete del = new HttpDelete(url);
            del.addHeader("Content-Type", contentType);
            if (dCookie != null)
                del.addHeader("Authorization", "_dremio{" + dCookie + "}");
            HttpResponse response = client.execute(del);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        headers.add("Content-Type", "Application/Json;charset=utf-8");
        return new ResponseEntity<String>(result, headers, HttpStatus.OK);
    }

    @ResponseBody
    public ResponseEntity<String> queryPut(String url, String contentType, String dCookie) {
        String result = "";
        HttpHeaders headers = new HttpHeaders();
        HttpClient client = HttpClientBuilder.create().build();

        try {
            HttpPut put = new HttpPut(url);
            put.addHeader("Content-Type", contentType);
            if (dCookie != null)
                put.addHeader("Authorization", "_dremio{" + dCookie + "}");
            HttpResponse response = client.execute(put);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        headers.add("Content-Type", "Application/Json;charset=utf-8");
        return new ResponseEntity<String>(result, headers, HttpStatus.OK);
    }

}
