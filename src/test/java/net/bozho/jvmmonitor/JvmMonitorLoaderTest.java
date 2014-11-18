package net.bozho.jvmmonitor;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class JvmMonitorLoaderTest {

    @Test
    public void loaderTest() throws Exception {
        JvmMonitorLoader loader = new JvmMonitorLoader();
        loader.init(1, TimeUnit.SECONDS, new File("c:/tmp/"), 3000);
        Thread.sleep(100000);
        loader.close();
    }
}
