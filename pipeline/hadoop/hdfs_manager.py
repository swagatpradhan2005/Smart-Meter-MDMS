"""
HDFS Manager Module - Local Simulation
Simulates HDFS operations with local file system.
Provides upload, download, list, and delete operations.
"""

from pathlib import Path
import shutil
import logging
from typing import List, Dict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


class HDFSManager:
    """Manages HDFS-style operations with local simulation."""
    
    def __init__(self, hdfs_root: str = "data/hdfs"):
        """
        Initialize HDFS manager.
        
        Args:
            hdfs_root: Root directory for HDFS simulation
        """
        self.hdfs_root = Path(hdfs_root)
        self.hdfs_root.mkdir(parents=True, exist_ok=True)
        self.operations_log = []
        logger.info(f"HDFS Manager initialized with root: {self.hdfs_root}")
    
    def put(self, local_path: str, hdfs_path: str) -> bool:
        """
        Upload file to simulated HDFS.
        
        Args:
            local_path: Local file path
            hdfs_path: Target path in HDFS
            
        Returns:
            True if successful
        """
        try:
            src = Path(local_path)
            dst = self.hdfs_root / hdfs_path.lstrip('/')
            
            if src.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                msg = f"PUT: {src} -> hdfs://{hdfs_path}"
                self.operations_log.append(msg)
                logger.info(msg)
                return True
            elif src.is_dir():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(src, dst, dirs_exist_ok=True)
                msg = f"PUT (recursive): {src} -> hdfs://{hdfs_path}"
                self.operations_log.append(msg)
                logger.info(msg)
                return True
            else:
                logger.error(f"Source not found: {src}")
                return False
        except Exception as e:
            logger.error(f"PUT failed: {e}")
            return False
    
    def get(self, hdfs_path: str, local_path: str) -> bool:
        """
        Download file from simulated HDFS.
        
        Args:
            hdfs_path: Path in HDFS
            local_path: Target local path
            
        Returns:
            True if successful
        """
        try:
            src = self.hdfs_root / hdfs_path.lstrip('/')
            dst = Path(local_path)
            
            if src.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                msg = f"GET: hdfs://{hdfs_path} -> {dst}"
                self.operations_log.append(msg)
                logger.info(msg)
                return True
            elif src.is_dir():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(src, dst, dirs_exist_ok=True)
                msg = f"GET (recursive): hdfs://{hdfs_path} -> {dst}"
                self.operations_log.append(msg)
                logger.info(msg)
                return True
            else:
                logger.error(f"HDFS path not found: {src}")
                return False
        except Exception as e:
            logger.error(f"GET failed: {e}")
            return False
    
    def ls(self, hdfs_path: str = "/") -> List[str]:
        """
        List files in HDFS directory.
        
        Args:
            hdfs_path: Path in HDFS
            
        Returns:
            List of file/directory names
        """
        try:
            path = self.hdfs_root / hdfs_path.lstrip('/')
            if path.exists():
                items = [item.name for item in path.iterdir()]
                logger.info(f"LS: hdfs://{hdfs_path} -> {len(items)} items")
                return items
            else:
                logger.warning(f"Path not found: hdfs://{hdfs_path}")
                return []
        except Exception as e:
            logger.error(f"LS failed: {e}")
            return []
    
    def rm(self, hdfs_path: str, recursive: bool = False) -> bool:
        """
        Remove file or directory from HDFS.
        
        Args:
            hdfs_path: Path in HDFS
            recursive: Allow recursive delete
            
        Returns:
            True if successful
        """
        try:
            path = self.hdfs_root / hdfs_path.lstrip('/')
            
            if not path.exists():
                logger.warning(f"Path not found: {path}")
                return False
            
            if path.is_file():
                path.unlink()
                msg = f"RM: hdfs://{hdfs_path} (file)"
                self.operations_log.append(msg)
                logger.info(msg)
                return True
            elif path.is_dir():
                if recursive:
                    shutil.rmtree(path)
                    msg = f"RM: hdfs://{hdfs_path} (recursive)"
                    self.operations_log.append(msg)
                    logger.info(msg)
                    return True
                else:
                    logger.error("Use recursive=True to remove directories")
                    return False
        except Exception as e:
            logger.error(f"RM failed: {e}")
            return False
    
    def get_operations_log(self) -> List[str]:
        """Get all operations performed."""
        return self.operations_log.copy()


def main():
    """Example usage."""
    logger.info("HDFSManager module loaded successfully")


if __name__ == "__main__":
    main()
