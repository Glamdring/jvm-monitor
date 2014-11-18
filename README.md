A simple JVM statistics monitor.

Usage: Instantiate when your application starts (e.g. in a ``ContextLoaderListener``):

    JvmMonitorLoader loader = new JvmMonitorLoader();
    loader.init(1, TimeUnit.SECONDS, new File("/tmp/"), 3000);

That will start an embedded http server on port 3000, an embedded database storing data in the tmp folder, 
and scheduled jobs that read JVM metrics and store them in the database.

Note: do not forget to call ``loader.close();`` when your application is destroyed. 

TODO:

- Build a nice UI
- display all charts
- fix chart labels
- gather more metrics
- support per-cluster data (cluster nodes will be detected in multiple ways: using AWS api; hard-coded list of nodes; using broadcast)