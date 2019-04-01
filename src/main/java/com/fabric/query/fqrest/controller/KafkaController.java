package com.fabric.query.fqrest.controller;

import com.fabric.query.fqrest.util.ZRestUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "*")
@RequestMapping(value="/api/kafka/v1")
public class KafkaController {

    @Value("${k.url}")
    private String url;

    @GetMapping(value="/topics")
    @ResponseBody
    public ResponseEntity<String> getTopics(@RequestParam String kurl)
    {
        String restUrl = kurl.trim();
        if (restUrl.length()==0){
            restUrl = this.url;
        }
        ZRestUtil kRest = new ZRestUtil();
        return kRest.queryGet(restUrl + "/topics", "application/json", null);
    }

}
