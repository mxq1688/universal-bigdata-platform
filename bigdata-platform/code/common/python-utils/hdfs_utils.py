"""
HDFS 文件操作工具类 (Python 版)
基于 hdfs (WebHDFS) 和 pyarrow (Native HDFS) 双模式
"""
import os
import logging

logger = logging.getLogger(__name__)

try:
    from hdfs import InsecureClient
    WEBHDFS_AVAILABLE = True
except ImportError:
    WEBHDFS_AVAILABLE = False

try:
    import pyarrow.fs as pafs
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False


class HDFSUtils:
    """HDFS 文件操作工具"""

    def __init__(self, namenode_url="http://localhost:9870", user="hadoop", mode="webhdfs"):
        """
        Args:
            namenode_url: WebHDFS URL (http://host:9870) 或 Native URL (hdfs://host:9000)
            user: HDFS 用户
            mode: webhdfs | arrow
        """
        self.namenode_url = namenode_url
        self.user = user
        self.mode = mode
        self.client = self._init_client()

    def _init_client(self):
        if self.mode == "webhdfs":
            if not WEBHDFS_AVAILABLE:
                raise ImportError("请安装 hdfs 库: pip install hdfs")
            client = InsecureClient(self.namenode_url, user=self.user)
            logger.info("WebHDFS 客户端初始化: %s (user=%s)", self.namenode_url, self.user)
            return client
        elif self.mode == "arrow":
            if not ARROW_AVAILABLE:
                raise ImportError("请安装 pyarrow: pip install pyarrow")
            client = pafs.HadoopFileSystem(self.namenode_url)
            logger.info("Arrow HDFS 客户端初始化: %s", self.namenode_url)
            return client
        else:
            raise ValueError(f"不支持的模式: {self.mode}，请使用 webhdfs 或 arrow")

    # ====================== 目录操作 ======================

    def mkdir(self, path):
        """创建目录"""
        if self.mode == "webhdfs":
            self.client.makedirs(path)
        else:
            self.client.create_dir(path)
        logger.info("创建目录: %s", path)

    def exists(self, path):
        """判断路径是否存在"""
        if self.mode == "webhdfs":
            return self.client.status(path, strict=False) is not None
        else:
            try:
                self.client.get_file_info(path)
                return True
            except Exception:
                return False

    def delete(self, path, recursive=False):
        """删除目录或文件"""
        if self.mode == "webhdfs":
            self.client.delete(path, recursive=recursive)
        else:
            self.client.delete_dir_contents(path) if recursive else self.client.delete_file(path)
        logger.info("删除路径: %s (recursive=%s)", path, recursive)

    def list_dir(self, path):
        """列出目录下的文件"""
        if self.mode == "webhdfs":
            return self.client.list(path)
        else:
            selector = pafs.FileSelector(path, recursive=False)
            return [f.path for f in self.client.get_file_info(selector)]

    # ====================== 文件上传/下载 ======================

    def upload(self, local_path, hdfs_path, overwrite=False):
        """上传本地文件到 HDFS"""
        if self.mode == "webhdfs":
            self.client.upload(hdfs_path, local_path, overwrite=overwrite)
        else:
            with open(local_path, "rb") as f:
                with self.client.open_output_stream(hdfs_path) as out:
                    out.write(f.read())
        logger.info("上传文件: %s → %s", local_path, hdfs_path)

    def download(self, hdfs_path, local_path, overwrite=False):
        """下载 HDFS 文件到本地"""
        local_dir = os.path.dirname(local_path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)

        if self.mode == "webhdfs":
            self.client.download(hdfs_path, local_path, overwrite=overwrite)
        else:
            with self.client.open_input_stream(hdfs_path) as f:
                with open(local_path, "wb") as out:
                    out.write(f.read())
        logger.info("下载文件: %s → %s", hdfs_path, local_path)

    # ====================== 文件读写 ======================

    def read_text(self, path, encoding="utf-8"):
        """读取文本文件"""
        if self.mode == "webhdfs":
            with self.client.read(path, encoding=encoding) as reader:
                return reader.read()
        else:
            with self.client.open_input_stream(path) as f:
                return f.read().decode(encoding)

    def write_text(self, path, content, encoding="utf-8", overwrite=True):
        """写入文本文件"""
        if self.mode == "webhdfs":
            self.client.write(path, content.encode(encoding), overwrite=overwrite)
        else:
            with self.client.open_output_stream(path) as f:
                f.write(content.encode(encoding))
        logger.info("写入文件: %s (%s bytes)", path, len(content))

    # ====================== 文件信息 ======================

    def get_file_size(self, path):
        """获取文件大小（字节）"""
        if self.mode == "webhdfs":
            status = self.client.status(path)
            return status.get("length", 0)
        else:
            info = self.client.get_file_info(path)
            return info.size

    # ====================== 分区操作 ======================

    def create_partition(self, base_path, dt):
        """创建数仓分区目录"""
        partition_path = f"{base_path}/dt={dt}"
        self.mkdir(partition_path)
        return partition_path

    def clean_partition(self, base_path, dt):
        """清空分区（重跑前清理）"""
        partition_path = f"{base_path}/dt={dt}"
        if self.exists(partition_path):
            self.delete(partition_path, recursive=True)
            logger.info("清空分区: %s", partition_path)
        return partition_path

    def partition_exists(self, base_path, dt):
        """检查分区是否存在"""
        return self.exists(f"{base_path}/dt={dt}")

    @classmethod
    def from_env(cls):
        """从环境变量创建"""
        return cls(
            namenode_url=os.getenv("HDFS_URL", "http://localhost:9870"),
            user=os.getenv("HDFS_USER", "hadoop"),
            mode=os.getenv("HDFS_MODE", "webhdfs"),
        )
