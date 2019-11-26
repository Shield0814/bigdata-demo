package com.ljl.java.process;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class ProcessBuilderDemo {

    public static void main(String[] args) throws IOException {
        Process p = new ProcessBuilder("ipconfig ", "/all")
                .start();

        InputStreamReader ir = new InputStreamReader(p.getInputStream(), Charset.forName("GB2312"));


        char[] buffer = new char[1024];
        int len = ir.read(buffer);
        while (len != -1) {
            System.out.println(new String(buffer));
            len = ir.read(buffer);
        }

        System.out.println(p.exitValue());


    }
}
