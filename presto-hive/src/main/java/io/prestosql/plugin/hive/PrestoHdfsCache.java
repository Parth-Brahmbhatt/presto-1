/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.plugin.hive;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class PrestoHdfsCache
{
    private String defaultFS;
    private FileSystem hadoopfs;
    private FileSystem s3fs;
    private Path cacheBasePath;
    private final int maxThreads = 16;
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final ExecutorService copyExecutorService = newFixedThreadPool(maxThreads, daemonThreadsNamed("copy-executor-service"));
    private static final Logger log = Logger.get(PrestoHdfsCache.class);
    DistinctBlockingQueue<CopyInfo> queue = new DistinctBlockingQueue<CopyInfo>();
    private Set<Path> concurrentHashSet = Sets.newConcurrentHashSet();

    public void setS3fs(FileSystem s3fs)
    {
        this.s3fs = s3fs;
    }

    private class CopyWorker
            implements Runnable
    {
        @Override
            public void run()
        {
            CopyInfo currentCopyInfo = null;
            while (!Thread.currentThread().isInterrupted() && !shutdown.get()) {
                try {
                    currentCopyInfo = queue.take();
                    if (!hadoopfs.exists(currentCopyInfo.getHdfsPath())) {
                        FileUtil.copy(s3fs, currentCopyInfo.getS3Path(), hadoopfs, currentCopyInfo.getHdfsPath(), false,
                                hadoopfs.getConf());
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                catch (IOException ignored) {
                    //do Nothing
                }
                finally {
                    if (currentCopyInfo != null) {
                        concurrentHashSet.remove(currentCopyInfo.getHdfsPath());
                    }
                }
            }
        }
    }

    public Path getHdfsPathOrCopyToHdfs(Path s3path) throws IOException
    {
        if (cacheBasePath != null) {
            String parentPath = s3path.getParent().toString();
            String parentPathDigest = DigestUtils.md5Hex(parentPath.getBytes());
            try {
                Path hdfsPath = new Path(cacheBasePath, parentPathDigest + "/" + s3path.getName());
                if (hadoopfs.exists(hdfsPath)) {
                    return hdfsPath;
                }
                else {
                    queue.add(new CopyInfo(s3path, hdfsPath));
                }
            }
            catch (IOException ignored) {
                return s3path;
            }
        }
        return s3path;
    }

    public LocatedFileStatus getLocatedStatus(Path path) throws IOException
    {
        return hadoopfs.listLocatedStatus(path).next();
    }

    @PostConstruct
    public void start() throws IOException
    {
        for (int i = 0; i < maxThreads; i++) {
            copyExecutorService.submit(new CopyWorker());
        }
    }

    @PreDestroy
    public void shutdown()
    {
        shutdown.set(true);
        copyExecutorService.shutdown();
    }

    public void setDefaultFS()
    {
        Configuration conf = new Configuration();
        String hadoopEnv = System.getenv("HADOOP_HOME");
        if (hadoopEnv != null) {
            log.info("Hadoop home: %s", hadoopEnv);
            Path path = new Path(hadoopEnv, "etc/hadoop/core-site.xml");
            try {
                FileSystem fs = FileSystem.get(path.toUri(), conf);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                try {
                    String line = br.readLine();
                    while (line != null) {
                        if (line.contains("fs.defaultFS")) {
                            line = br.readLine();
                            String p = ".*<value>(.*)</value>";
                            Pattern pattern = Pattern.compile(p);
                            Matcher matcher = pattern.matcher(line);
                            if (matcher.find()) {
                                defaultFS = matcher.group(1);
                            }
                        }
                        line = br.readLine();
                    }
                }
                finally {
                    br.close();
                }
            }
            catch (IOException e) {
                log.warn("Unable to read path %s: %s", path, e);
            }
        }
    }

    public void setHdfsCache(String defaultFS, int replication)
    {
        Configuration hadoopConf = new Configuration();
        if (defaultFS == null) {
            setDefaultFS();
        }
        else {
            this.defaultFS = defaultFS;
        }
        log.info("Setting hadoop default fs as %s and replication factor as %s", this.defaultFS, replication);
        hadoopConf.set("fs.defaultFS", defaultFS);
        hadoopConf.setInt("dfs.replication", replication);
        try {
            hadoopfs = FileSystem.get(hadoopConf);
            start();
        }
        catch (IOException e) {
            log.warn("Could not get filesystem from hadoop conf: %s", e);
            return;
        }

        cacheBasePath = new Path(defaultFS, "/ignitecache");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run()
            {
                copyExecutorService.shutdownNow();
            }
        });
    }

    public class CopyInfo
    {
        public CopyInfo(Path s3Path, Path hdfsPath)
        {
            this.s3Path = s3Path;
            this.hdfsPath = hdfsPath;
        }

        public Path getS3Path()
        {
            return s3Path;
        }

        public Path getHdfsPath()
        {
            return hdfsPath;
        }

        private Path s3Path;
        private Path hdfsPath;
    }

    public class DistinctBlockingQueue<E>
            extends LinkedBlockingQueue<E>
    {
        private final ReentrantLock addLock = new ReentrantLock();

        @Override
        public boolean add(E e)
        {
            if (e instanceof CopyInfo) {
                Path hdfsPath = ((PrestoHdfsCache.CopyInfo) e).getHdfsPath();
                final ReentrantLock addLock = this.addLock;
                addLock.lock();
                if (!concurrentHashSet.contains(hdfsPath)) {
                    concurrentHashSet.add(hdfsPath);
                    super.add(e);
                }
                addLock.unlock();
                return true;
            }
            else {
                throw new IllegalArgumentException("Queue supports only objects of copyInfo");
            }
        }
    }
}
