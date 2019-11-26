package com.ljl.scala.io

import scala.io.Source

object IODemo {

    def main(args: Array[String]): Unit = {

        var counter = 0


        val dataFile = Source.fromFile("d:\\data\\news_tensite_xml.dat", "UTF8")

        //        val line = dataFile.bufferedReader().readLine()

        val buffer = new Array[Char](1024)
        val str = dataFile.reader().read(buffer)


        println(buffer)
        dataFile.close()
    }


    def out(): Unit = {
        val values = Array[String](
            "mid_id",
            "user_id",
            "version_code",
            "version_name",
            "lang",
            "source",
            "os",
            "area",
            "model",
            "brand",
            "sdk_version",
            "gmail",
            "height_width",
            "app_time",
            "network",
            "lng",
            "lat",
            "entry",
            "open_ad_type",
            "action",
            "loading_time",
            "detail",
            "extend1"
        )

        val keys = Array[String](

        )

    }
}


