package com.fabric.query.fqrest.controller;

import com.fabric.query.fqrest.util.DRestUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@RestController
@CrossOrigin(origins = "*")
@RequestMapping(value="/api/dremio/v1")
public class DremioController {
    final static Logger logger = LoggerFactory.getLogger(DremioController.class);
    static String dCookie = "";

    @Value("${dremio.credential}")
    private String credential;

    @Value("${dremio.url}")
    private String durl;

    @PostMapping(value="/login")
    @ResponseBody
    public ResponseEntity<String> login(@RequestBody String credential)
    {
        String result = "";
        HttpHeaders headers = new HttpHeaders();
        HttpClient client = HttpClientBuilder.create().build();

        try {
            HttpPost post = new HttpPost(durl + "/apiv2/login");
            StringEntity params = new StringEntity(credential);
            post.setEntity(params);
            post.addHeader("Content-Type", "application/json");
            HttpResponse response = client.execute(post);

            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            JSONObject myJson = new JSONObject(result);
            dCookie = myJson.getString("token");
            logger.info("token: " + dCookie);

        }
        catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        headers.add("Content-Type", "Application/Json;charset=utf-8");
        return new ResponseEntity<>(result, headers, HttpStatus.OK);
    }
    
    @GetMapping(value="/catalog")
    @ResponseBody
    public ResponseEntity<String> getCatalog()
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryGet(durl + "/api/v3/catalog","application/json", dCookie);
    }

    @GetMapping(value="/catalog/{id}")
    @ResponseBody
    public ResponseEntity<String> getCatalogDetail(@PathVariable String id)
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryGet(durl + "/api/v3/catalog/" + id,"application/json", dCookie);
    }

    @PostMapping(value="/catalog")
    @ResponseBody
    public ResponseEntity<String> createCatalog(@RequestBody String reqParams)
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryPost(durl + "/api/v3/catalog","application/json", dCookie, reqParams);
    }

    @DeleteMapping(value="/catalog/{id}")
    @ResponseBody
    public ResponseEntity<String> deleteCatalog(@PathVariable String id)
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryDelete(durl + "/api/v3/catalog/" + id,"application/json", dCookie);
    }

    @GetMapping(value="/dataset")
    @ResponseBody
    public ResponseEntity<String> getDataset()
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryGet(durl + "/apiv2/dataset","application/json", dCookie);
    }

    @GetMapping(value="/dataset/{id}")
    @ResponseBody
    public ResponseEntity<String> getCatalogDatasetDetail(@PathVariable String id)
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryGet(durl + "/apiv2/dataset/" + id,"application/json", dCookie);
    }

    @GetMapping(value="/home/{id}")
    @ResponseBody
    public ResponseEntity<String> getHomeDetail(@PathVariable String id)
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryGet(durl + "/apiv2/home/" + id,"application/json", dCookie);
    }

    @GetMapping(value="/job")
    @ResponseBody
    public ResponseEntity<String> getJob()
    {
        checkCredential();
        DRestUtil dRest = new DRestUtil();
        return dRest.queryGet(durl + "/api/v3/job","application/json", dCookie);
    }

    private void checkCredential() {
        if (dCookie.length() == 0) {
            login(this.credential);
        }
    }
}
