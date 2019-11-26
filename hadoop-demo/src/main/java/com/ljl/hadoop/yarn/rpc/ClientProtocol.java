package com.ljl.hadoop.yarn.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

public interface ClientProtocol extends VersionedProtocol {

    final long versionID = 1L;

    String echo(String value) throws IOException;

    int add(int v1, int v2) throws IOException;
}
