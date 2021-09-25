package com.atguigu.gmall.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author sunzhipeng
 * @create 2021-07-16 9:26
 */
@RestController
public class FirstController {
    @RequestMapping("/testDemo")
    public String test(@RequestParam("name") String nn,@RequestParam("age") int age){
        System.out.println(nn+":"+age);
        return "success";
    }
}
