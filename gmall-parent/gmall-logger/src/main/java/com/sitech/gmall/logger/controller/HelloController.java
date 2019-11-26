package com.sitech.gmall.logger.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class HelloController {

    private Logger logger = LoggerFactory.getLogger(HelloController.class);


    @ResponseBody
    @RequestMapping("hello")
    public String hello(String name) {

        return "hello, " + name;
    }


}
