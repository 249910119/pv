package com.persagy.htable.datas;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class DatasApplication {

    public static void main(String[] args) {
        SpringApplication.run(DatasApplication.class, args);
    }

}
