package com.ljl.spark.launcher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class JavaLauncherApp {


    public static void main(String[] args) throws IOException, InterruptedException {


        SparkAppHandle handle = new SparkLauncher()
                .setAppResource("/my/app.jar")
                .setMainClass("my.spark.app.Main")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .startApplication();


        Process spark = new SparkLauncher()
                .setAppResource("/my/app.jar")
                .setMainClass("my.spark.app.Main")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .launch();
        spark.waitFor();
    }
}
