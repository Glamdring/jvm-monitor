package net.bozho.jvmmonitor;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.platform.derby.DerbyPlatform;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class JvmMonitorLoader implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(JvmMonitorLoader.class);
    
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("dd.MM.yyyy HH:mm:ss");
    
    private static final String DB_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DB_NAME = "jvmMonitoring";
    private static final String DB_CONNECTION_PREFIX = "jdbc:derby:directory:";
    
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledExecutorService databaseWriter = Executors.newScheduledThreadPool(1);
    private HttpServer server;
    
    private Set<String> YOUNG_GEN_GC = Sets.newHashSet("Copy", "PS Scavenge", "ParNew", "G1 Young Generation");
    private Set<String> OLD_GEN_GC = Sets.newHashSet("MarkSweepCompact", "PS MarkSweep", "ConcurrentMarkSweep", "MarkSweepCompact", "G1 Mixed Generation");
    
    private Map<MetricType, List<MetricEntry>> metrics = new ConcurrentHashMap<>();
    
    private String databasePath;
    
    private ObjectMapper mapper = new ObjectMapper();
    
    public void init(long interval, TimeUnit timeUnit, final File databasePath, int serverPort) {
        this.databasePath = databasePath.getAbsolutePath() + "/";
        
        initDB();
        initHttpServer(serverPort);
        
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    storeMetrics();
                } catch (Exception e) {
                    logger.error("Error calculating metrics", e);
                }
            }
            
        }, 0, interval, timeUnit);

        try {
            Class.forName(DB_DRIVER).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        
        databaseWriter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.debug("Storing metrics in DB...");
                
                try (Connection conn = getDatabaseConnection()) {
                    conn.setAutoCommit(false);
                    PreparedStatement pstm = conn.prepareStatement("INSERT INTO metrics "
                            + "(metric, longValue, doubleValue, dataType, timestamp) "
                            + "VALUES (?, ?, ?, ?, ?)");
                    
                    try {
                        // each MetricEntry already has its key, so we are flattening the structure
                        Collection<List<MetricEntry>> values = metrics.values();
                        for (List<MetricEntry> entries : values) {
                           List<MetricEntry> snapshot = new ArrayList<>(entries);
                           entries.clear();
                           for (MetricEntry entry : snapshot) {
                               insertEntry(entry, pstm);
                               pstm.addBatch();
                           }
                        }
                        pstm.executeBatch();
                        conn.commit();
                    } catch (SQLException ex) {
                        conn.rollback();
                        throw ex;
                    }
                } catch (SQLException e) {
                    logger.error("SQL problem storing metrics in the database", e);
                } catch (Exception e) {
                    logger.error("Problem storing metrics in the database", e);
                }
            }

            private void insertEntry(MetricEntry entry, PreparedStatement pstm) {
                try {
                    pstm.setString(1, entry.getKey());
                    if (entry.getLongValue() != null) {
                        pstm.setLong(2, entry.getLongValue());
                    }
                    if (entry.getDoubleValue() != null) {
                        pstm.setDouble(3, entry.getDoubleValue());
                    }
                    pstm.setString(4, entry.getType().toString());
                    pstm.setLong(5, entry.getTimestamp());
                } catch (SQLException e) {
                    logger.error("Problem inserting metric: " + entry, e);
                }
            }
            
        }, 50 * interval, 50 * interval, timeUnit);
    }
    
    
    private void initHttpServer(int serverPort) {
        try {
            server = HttpServer.create(new InetSocketAddress("0.0.0.0", serverPort), 0);
            server.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    logger.debug("Handling HTTP request");
                    String html = CharStreams.toString(new InputStreamReader(JvmMonitorLoader.class.getResourceAsStream("/templates/dashboard.html"), "UTF-8"));
                    try (Connection conn = getDatabaseConnection()) {
                        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM metrics");
                        Map<String, List<MetricEntry>> metrics = new HashMap<>();
                        
                        while (rs.next()) {
                            MetricEntry entry = getEntryFromResultSet(rs);
                            if (!metrics.containsKey(entry.getKey())) {
                                metrics.put(entry.getKey(), new ArrayList<MetricEntry>());
                            }
                            metrics.get(entry.getKey()).add(entry);
                        }
                        html = html.replace("$JSON$", mapper.writeValueAsString(metrics));
                    } catch (SQLException ex) {
                        logger.error("Problem reading metrics from DB", ex);
                    }
                    try {
                        exchange.getResponseHeaders().add("Content-Type", "text/html");
                        exchange.sendResponseHeaders(200, html.length());
                        OutputStream out = exchange.getResponseBody();
                        out.write(html.toString().getBytes("UTF-8"));
                        out.close();
                        exchange.close();
                    } catch (Exception ex) {
                        logger.error("Error writing response", ex);
                        throw ex;
                    }
                }
            });
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to setup http server", e);
        }
    }


    private void initDB() {
        try {
            logger.debug("Initializing DB...");
            Database db = new DatabaseIO().read(new InputStreamReader(JvmMonitorLoader.class.getResourceAsStream("/dbSchema.xml"), "UTF-8"));
            Platform platform = PlatformFactory.createNewPlatformInstance(DerbyPlatform.DATABASENAME);
            EmbeddedDataSource dataSource = new EmbeddedDataSource();
            dataSource.setDatabaseName(databasePath + DB_NAME);
            dataSource.setCreateDatabase("create");
            platform.setDataSource(dataSource);
            if (platform.readModelFromDatabase(DB_NAME).getTableCount() == 0) { //TODO is that needed?
                platform.createTables(db, false, false);
            } else {
                platform.alterTables(db, false);
            }
        } catch (DdlUtilsException | UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }


    public void storeMetrics() {
        logger.debug("Storing metrics...");
        
        addValue(MetricType.UPTIME, ManagementFactory.getRuntimeMXBean().getUptime());
        addValue(MetricType.LOADED_CLASSES, ManagementFactory.getClassLoadingMXBean().getLoadedClassCount());
        
        for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (YOUNG_GEN_GC.contains(mbean.getName())) {
                addValue(MetricType.YOUNG_GEN_GARBAGE_COLLECTION_COUNT, mbean.getCollectionCount());
                addValue(MetricType.YOUNG_GEN_GARBAGE_COLLECTION_COUNT, mbean.getCollectionTime());
            } else if (OLD_GEN_GC.contains(mbean.getName())) {
                addValue(MetricType.OLD_GEN_GARBAGE_COLLECTION_COUNT, mbean.getCollectionCount());
                addValue(MetricType.OLD_GEN_GARBAGE_COLLECTION_COUNT, mbean.getCollectionTime());
            }
        }
        
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        
        addValue(MetricType.HEAP_MEMORY_USED, memoryBean.getHeapMemoryUsage().getUsed());
        addValue(MetricType.HEAP_MEMORY_COMMITTED, memoryBean.getHeapMemoryUsage().getCommitted());
        addValue(MetricType.HEAP_MEMORY_MAX, memoryBean.getHeapMemoryUsage().getMax());
        
        addValue(MetricType.NONHEAP_MEMORY_USED, memoryBean.getNonHeapMemoryUsage().getUsed());
        addValue(MetricType.NONHEAP_MEMORY_COMMITTED, memoryBean.getNonHeapMemoryUsage().getCommitted());
        addValue(MetricType.NONHEAP_MEMORY_MAX, memoryBean.getNonHeapMemoryUsage().getMax());
        
        
        addValue(MetricType.SYSTEM_LOAD_AVAREGE, ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
        
        //TODO
        // getOpenFileDescriptorCount
        // getMaxFileDescriptorCount
        // get attributes via MBean server
        // https://github.com/dropwizard/metrics/blob/master/metrics-jvm/src/main/java/com/codahale/metrics/jvm/BufferPoolMetricSet.java
        ManagementFactory.getMemoryPoolMXBeans(); // TODO extract all
        
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        
        addValue(MetricType.THREADS, threadBean.getThreadCount());
        addValue(MetricType.DAEMON_THREADS, threadBean.getDaemonThreadCount());
        
        ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds());
        int totalBlockedCount = 0;
        int totalBlockedTime = 0;
        for (ThreadInfo info : infos) {
            totalBlockedCount += info.getBlockedCount();
            totalBlockedTime += info.getBlockedTime();
        }
        addValue(MetricType.AVAREGE_THREAD_BLOCK_COUNT, totalBlockedCount / (double) infos.length);
        addValue(MetricType.AVAREGE_THREAD_BLOCK_COUNT, totalBlockedTime / (double) infos.length);
        
        // TODO check this more rarely
        long[] deadlockedThreads = threadBean.findDeadlockedThreads();
        addValue(MetricType.DEADLOCKED_THREADS, deadlockedThreads != null ? deadlockedThreads.length : 0);
    }


    private void addValue(MetricType key, long value) {
        MetricEntry entry = addGenericEntry(key);
        entry.setType(DataType.LONG);
        entry.setLongValue(value);
    }
    
    private void addValue(MetricType key, double value) {
        MetricEntry entry = addGenericEntry(key);
        entry.setType(DataType.DOUBLE);
        entry.setDoubleValue(value);
    }


    private MetricEntry addGenericEntry(MetricType key) {
        List<MetricEntry> entries = metrics.get(key);
        if (entries == null) {
            entries = new ArrayList<>();
            metrics.put(key, entries);
        }
        MetricEntry entry = new MetricEntry();
        entries.add(entry);
        entry.setKey(key.toString());
        entry.setTimestamp(System.currentTimeMillis());
        return entry;
    }


    private Connection getDatabaseConnection() throws SQLException {
        return DriverManager.getConnection(DB_CONNECTION_PREFIX + 
                JvmMonitorLoader.this.databasePath + DB_NAME);
    }
    
    private MetricEntry getEntryFromResultSet(ResultSet rs) throws SQLException {
        MetricEntry entry = new MetricEntry();
        entry.setKey(rs.getString("metric"));
        DataType dataType = DataType.valueOf(rs.getString("dataType"));
        entry.setType(dataType);
        if (dataType == DataType.LONG) {
            entry.setLongValue(rs.getLong("longValue"));
        } else if (dataType == DataType.DOUBLE){
            entry.setDoubleValue(rs.getDouble("doubleValue"));
        }
        entry.setTimestamp(rs.getLong("timeStamp"));
        return entry;
    }
    
    @Override
    public void close() throws Exception {
        databaseWriter.shutdown();
        scheduler.shutdownNow();
        if (server != null) {
            server.stop(1000);
        }
    }
    
    @JsonInclude(Include.NON_NULL)
    public class MetricEntry {
        private long timestamp;
        private String key;
        private DataType type;
        private Long longValue;
        private Double doubleValue;

        public long getTimestamp() {
            return timestamp;
        }
        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
        public String getKey() {
            return key;
        }
        public void setKey(String key) {
            this.key = key;
        }
        public DataType getType() {
            return type;
        }
        public void setType(DataType type) {
            this.type = type;
        }
        public Long getLongValue() {
            return longValue;
        }
        public void setLongValue(Long longValue) {
            this.longValue = longValue;
        }
        public Double getDoubleValue() {
            return doubleValue;
        }
        public void setDoubleValue(Double doubleValue) {
            this.doubleValue = doubleValue;
        }
        
        @Override
        public String toString() {
            return "MetricEntry [timestamp=" + timestamp + ", key=" + key + ", type=" + type + ", longValue="
                    + longValue + ", doubleValue=" + doubleValue + "]";
        }
    }
    
    public static enum DataType {
        LONG, DOUBLE, STRING
    }
    
    public static enum MetricType {
        YOUNG_GEN_GARBAGE_COLLECTION_COUNT,
        YOUNG_GEN_GARBAGE_COLLECTION_TIME,
        OLD_GEN_GARBAGE_COLLECTION_COUNT,
        OLD_GEN_GARBAGE_COLLECTION_TIME,
        
        UPTIME,
        
        LOADED_CLASSES,
        
        HEAP_MEMORY_USED,
        HEAP_MEMORY_COMMITTED,
        HEAP_MEMORY_MAX,
        
        NONHEAP_MEMORY_USED,
        NONHEAP_MEMORY_COMMITTED,
        NONHEAP_MEMORY_MAX,
        
        OPEN_FILE_DESCRIPTORS,
        MAX_FILE_DESCRIPTORS,
        COMMITTED_VIRTUAL_MEMORY_SIZE,
        FREE_PHYSICAL_MEMORY_SIZE,
        FREE_SWAP_SPACE_SIZE,
        PROCESS_CPU_LOAD,
        PROCESS_CPU_TIME,
        SYSTEM_CPU_LOAD,
        SYSTEM_LOAD_AVAREGE,
        
        THREADS,
        DAEMON_THREADS,
        SUSPENDED_THREADS,
        AVAREGE_THREAD_BLOCK_COUNT,
        AVERAGE_THREAD_BLCOK_TIME,
        
        DEADLOCKED_THREADS,
        
        DIRECT_BUFFER_POOL_COUNT,
        DIRECT_BUFFER_POOL_MEMORY_USED,
        
        MAPPED_BUFFER_POOL_COUNT,
        MAPPED_BUFFER_POOL_MEMORY_USED
    }
}
