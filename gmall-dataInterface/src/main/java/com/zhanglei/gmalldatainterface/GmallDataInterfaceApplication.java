package com.zhanglei.gmalldatainterface;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.zhanglei.gmalldatainterface.mapper")
public class GmallDataInterfaceApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallDataInterfaceApplication.class, args);
    }

}
