package com.bigdata.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * HDFS 文件操作工具类
 */
public class HDFSUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);

    private static volatile FileSystem fileSystem;

    // ====================== 初始化 ======================

    /**
     * 获取 FileSystem 实例（单例）
     */
    public static FileSystem getFileSystem(String hdfsUri) throws IOException {
        if (fileSystem == null) {
            synchronized (HDFSUtils.class) {
                if (fileSystem == null) {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", hdfsUri);
                    conf.set("dfs.replication", "2");
                    conf.set("dfs.block.size", "134217728"); // 128MB
                    fileSystem = FileSystem.get(conf);
                    LOG.info("HDFS FileSystem 初始化完成: {}", hdfsUri);
                }
            }
        }
        return fileSystem;
    }

    /**
     * 获取 FileSystem 实例（指定用户）
     */
    public static FileSystem getFileSystem(String hdfsUri, String user) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        return FileSystem.get(conf.get("fs.defaultFS") != null
                ? conf.get("fs.defaultFS").toCharArray()[0] == 'h'
                ? java.net.URI.create(hdfsUri) : java.net.URI.create(hdfsUri)
                : java.net.URI.create(hdfsUri), conf, user);
    }

    // ====================== 目录操作 ======================

    /**
     * 创建目录
     */
    public static boolean mkdir(FileSystem fs, String path) throws IOException {
        Path hdfsPath = new Path(path);
        if (fs.exists(hdfsPath)) {
            LOG.warn("目录已存在: {}", path);
            return true;
        }
        boolean result = fs.mkdirs(hdfsPath);
        LOG.info("创建目录: {} → {}", path, result ? "成功" : "失败");
        return result;
    }

    /**
     * 删除目录或文件
     */
    public static boolean delete(FileSystem fs, String path, boolean recursive) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            LOG.warn("路径不存在: {}", path);
            return false;
        }
        boolean result = fs.delete(hdfsPath, recursive);
        LOG.info("删除路径: {} (recursive={}) → {}", path, recursive, result ? "成功" : "失败");
        return result;
    }

    /**
     * 判断路径是否存在
     */
    public static boolean exists(FileSystem fs, String path) throws IOException {
        return fs.exists(new Path(path));
    }

    /**
     * 列出目录下的文件和子目录
     */
    public static List<FileStatus> listFiles(FileSystem fs, String path) throws IOException {
        List<FileStatus> result = new ArrayList<>();
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            LOG.warn("路径不存在: {}", path);
            return result;
        }

        FileStatus[] statuses = fs.listStatus(hdfsPath);
        for (FileStatus status : statuses) {
            result.add(status);
        }
        return result;
    }

    /**
     * 递归列出所有文件
     */
    public static List<LocatedFileStatus> listAllFiles(FileSystem fs, String path) throws IOException {
        List<LocatedFileStatus> result = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(path), true);
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    // ====================== 文件上传/下载 ======================

    /**
     * 上传本地文件到 HDFS
     */
    public static void upload(FileSystem fs, String localPath, String hdfsPath, boolean overwrite) throws IOException {
        Path src = new Path(localPath);
        Path dst = new Path(hdfsPath);

        // 确保目标目录存在
        Path parentDir = dst.getParent();
        if (!fs.exists(parentDir)) {
            fs.mkdirs(parentDir);
        }

        fs.copyFromLocalFile(false, overwrite, src, dst);
        LOG.info("上传文件: {} → {}", localPath, hdfsPath);
    }

    /**
     * 下载 HDFS 文件到本地
     */
    public static void download(FileSystem fs, String hdfsPath, String localPath) throws IOException {
        Path src = new Path(hdfsPath);
        Path dst = new Path(localPath);

        // 确保本地目录存在
        File localDir = new File(localPath).getParentFile();
        if (localDir != null && !localDir.exists()) {
            localDir.mkdirs();
        }

        fs.copyToLocalFile(false, src, dst);
        LOG.info("下载文件: {} → {}", hdfsPath, localPath);
    }

    // ====================== 文件读写 ======================

    /**
     * 读取文件内容为字符串
     */
    public static String readFile(FileSystem fs, String path) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            throw new FileNotFoundException("文件不存在: " + path);
        }

        try (FSDataInputStream in = fs.open(hdfsPath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(line);
            }
            return sb.toString();
        }
    }

    /**
     * 写入字符串到文件
     */
    public static void writeFile(FileSystem fs, String path, String content, boolean overwrite) throws IOException {
        Path hdfsPath = new Path(path);

        // 确保目录存在
        Path parentDir = hdfsPath.getParent();
        if (!fs.exists(parentDir)) {
            fs.mkdirs(parentDir);
        }

        try (FSDataOutputStream out = fs.create(hdfsPath, overwrite)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
            out.hflush();
        }
        LOG.info("写入文件: {} ({} bytes)", path, content.length());
    }

    /**
     * 追加内容到文件
     */
    public static void appendFile(FileSystem fs, String path, String content) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            writeFile(fs, path, content, true);
            return;
        }

        try (FSDataOutputStream out = fs.append(hdfsPath)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
            out.hflush();
        }
        LOG.info("追加文件: {} ({} bytes)", path, content.length());
    }

    // ====================== 文件信息 ======================

    /**
     * 获取文件大小（字节）
     */
    public static long getFileSize(FileSystem fs, String path) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            return -1;
        }
        return fs.getFileStatus(hdfsPath).getLen();
    }

    /**
     * 获取文件修改时间
     */
    public static long getModificationTime(FileSystem fs, String path) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            return -1;
        }
        return fs.getFileStatus(hdfsPath).getModificationTime();
    }

    /**
     * 重命名/移动文件
     */
    public static boolean rename(FileSystem fs, String srcPath, String dstPath) throws IOException {
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);

        // 确保目标目录存在
        Path parentDir = dst.getParent();
        if (!fs.exists(parentDir)) {
            fs.mkdirs(parentDir);
        }

        boolean result = fs.rename(src, dst);
        LOG.info("重命名: {} → {} ({})", srcPath, dstPath, result ? "成功" : "失败");
        return result;
    }

    // ====================== 分区操作（数仓常用） ======================

    /**
     * 检查分区目录是否存在
     */
    public static boolean partitionExists(FileSystem fs, String basePath, String partition) throws IOException {
        String fullPath = basePath + "/" + partition;
        return fs.exists(new Path(fullPath));
    }

    /**
     * 创建分区目录（数仓 ETL 常用）
     */
    public static boolean createPartition(FileSystem fs, String basePath, String dt) throws IOException {
        String partitionPath = basePath + "/dt=" + dt;
        return mkdir(fs, partitionPath);
    }

    /**
     * 清空分区目录（重跑时先清理）
     */
    public static boolean cleanPartition(FileSystem fs, String basePath, String dt) throws IOException {
        String partitionPath = basePath + "/dt=" + dt;
        Path path = new Path(partitionPath);
        if (fs.exists(path)) {
            boolean deleted = fs.delete(path, true);
            LOG.info("清空分区: {} → {}", partitionPath, deleted ? "成功" : "失败");
            return deleted;
        }
        return true;
    }

    /**
     * 获取分区下的文件总大小
     */
    public static long getPartitionSize(FileSystem fs, String basePath, String dt) throws IOException {
        String partitionPath = basePath + "/dt=" + dt;
        Path path = new Path(partitionPath);
        if (!fs.exists(path)) {
            return 0;
        }

        long totalSize = 0;
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
        while (iterator.hasNext()) {
            totalSize += iterator.next().getLen();
        }
        return totalSize;
    }

    // ====================== 关闭 ======================

    public static void close() {
        if (fileSystem != null) {
            try {
                fileSystem.close();
                fileSystem = null;
                LOG.info("HDFS FileSystem 已关闭");
            } catch (IOException e) {
                LOG.error("关闭 HDFS FileSystem 失败", e);
            }
        }
    }
}
