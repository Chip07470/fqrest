package com.fabric.query.fqrest.controller;

import com.fabric.query.fqrest.util.ZRestUtil;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Value;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@RestController
@CrossOrigin(origins = "*")
@RequestMapping(value="/api/note")
public class ZeppelinController {
    final static Logger logger = LoggerFactory.getLogger(ZeppelinController.class);

    static String zCookie = "";

    @Value("${z.credential}")
    private String credential;

    @Value("${z.url}")
    private String zurl;

    @RequestMapping(value="/login", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> login(@RequestBody String credential)
    {
        String result = "";
        HttpHeaders headers = new HttpHeaders();
        HttpClient client = HttpClientBuilder.create().build();
        try {
            HttpPost post = new HttpPost(zurl + "/api/login");
            StringEntity params = new StringEntity(credential);
            post.setEntity(params);
            post.addHeader("Content-Type", "application/x-www-form-urlencoded");
            HttpResponse response = client.execute(post);

            org.apache.http.Header[] resheaders = response.getAllHeaders();
            for (org.apache.http.Header header : resheaders) {
                if (header.getName().equalsIgnoreCase("set-cookie") && header.getValue().endsWith("; HttpOnly")) {
                    zCookie = header.getValue();
                }
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            while ((line = rd.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        finally {
        }

        headers.add("Content-Type", "Application/Json;charset=utf-8");
        return new ResponseEntity<>(result, headers, HttpStatus.OK);
    }

    @RequestMapping(value = "/notebook", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<String> getNotebooks()
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryGet(zurl + "/api/notebook", "application/json", zCookie);
    }

    @RequestMapping(value = "/notebook/{id}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<String> getNotebookDetail(@PathVariable String id)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryGet(zurl + "/api/notebook/" + id, "application/json", zCookie);
    }

    @RequestMapping(value = "/notebook", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> createNotebook(@RequestBody String reqParams)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPost(zurl + "/api/notebook", "application/json", zCookie, reqParams);
    }

    @RequestMapping(value = "/notebook/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    public ResponseEntity<String> deleteNotebook(@PathVariable String id)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryDelete(zurl + "/api/notebook/" + id, "application/json", zCookie);
    }

    //Create a new paragraph
    @RequestMapping(value = "/notebook/{noteid}/paragraph", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> createParagraph(@PathVariable String noteid, @RequestBody String reqParams)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPost(zurl + "/api/notebook/" + noteid, "application/json", zCookie, reqParams);
    }

    //Update a paragraph
    @RequestMapping(value = "/notebook/{noteId}/paragraph/{paragraphId}", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> updateParagraph(@PathVariable String noteId, @RequestBody String reqParams, @PathVariable String paragrapId)
    {
        checkCredential();

        deleteParagraph(noteId, paragrapId);

        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPost(zurl + "/api/notebook/" + noteId + "/paragraph", "application/json", zCookie, reqParams);
    }

    //Get a paragraph info
    @RequestMapping(value = "/notebook/{noteid}/paragraph/{paragraphId}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<String> getParagraph(@PathVariable String noteId, @PathVariable String paragraphId)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryGet(zurl + "/api/notebook/" + noteId + "paragraph/" + paragraphId, "application/json", zCookie);
    }

    //Get a status of single paragraph info
    @RequestMapping(value = "/notebook/job/{noteid}/{paragraphId}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<String> getParagraphSingle(@PathVariable String noteId, @PathVariable String paragraphId)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryGet(zurl + "/api/notebook/job/" + noteId + "/" + paragraphId, "application/json", zCookie);
    }

    //Update a paragraph configuration
    @RequestMapping(value = "/notebook/{noteid}/paragraph/{paragraphId}/config", method = RequestMethod.PUT)
    @ResponseBody
    public ResponseEntity<String> updateParagraph(@PathVariable String noteId, @PathVariable String paragraphId)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPut(zurl + "/api/notebook/" + noteId + "paragraph/" + paragraphId + "/config", "application/json", zCookie);
    }

    //Delete a paragraph
    @RequestMapping(value = "/notebook/{noteid}/paragraph/{paragraphId}", method = RequestMethod.DELETE)
    @ResponseBody
    public ResponseEntity<String> deleteParagraph(@PathVariable String noteid, @PathVariable String paragrapId)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryDelete(zurl + "/api/notebook/" + noteid + "/paragraph/" + paragrapId, "application/json", zCookie);
    }

    //Stop a paragraph
//    @RequestMapping(value = "/notebook/{noteid}/paragraph/{paragraphId}", method = RequestMethod.DELETE)
//    @ResponseBody
//    public ResponseEntity<String> stopParagraph(@PathVariable String noteid, @PathVariable String paragrapId)
//    {
//        checkCredential();
//        ZRestUtil zRest = new ZRestUtil();
//        return zRest.queryDelete(zurl + "/api/notebook/job/" + noteid + "/paragraph/" + paragrapId, "application/json", zCookie);
//    }

    //Run all paragraphs
    @RequestMapping(value = "/notebook/job/{noteid}", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> runAllParagraph(@PathVariable String noteid)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPost(zurl + "/api/notebook/job/" + noteid, "application/json", zCookie, "");
    }

    //Run a paragraph asynchronously
    @RequestMapping(value = "/notebook/job/{noteId}/{paragraphId}", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> runAsyncParagraph(@PathVariable String noteId, @PathVariable String paragraphId, @RequestBody String reqParams)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPost(zurl + "/api/notebook/job/" + noteId + "/" + paragraphId, "application/json", zCookie, "");
    }

    //Run a paragraph asynchronously
    @RequestMapping(value = "/notebook/run/{noteId}/{paragraphId}", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> runSyncParagraph(@PathVariable String noteId, @PathVariable String paragraphId, @RequestBody String reqParams)
    {
        checkCredential();
        ZRestUtil zRest = new ZRestUtil();
        return zRest.queryPost(zurl + "/api/notebook/run/" + noteId + "/" + paragraphId, "application/json", zCookie, "");
    }

    private void checkCredential() {
        if (zCookie.length() == 0) {
            login(this.credential);
        }
    }

}
