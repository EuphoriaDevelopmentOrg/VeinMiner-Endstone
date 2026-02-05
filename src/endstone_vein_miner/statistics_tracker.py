"""
Statistics Tracker for VeinMiner
Tracks player vein mining statistics with persistent storage.
"""

from typing import Dict, Set, Any
from dataclasses import dataclass
from datetime import datetime
import os
import re
import yaml
from endstone import ColorFormat

try:
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover - optional dependency
    mysql_connector = None


@dataclass
class PlayerStats:
    """Player statistics data class"""

    player_name: str
    total_veins: int = 0
    total_blocks: int = 0
    largest_vein: int = 0
    last_mined: str = ""

    def increment_veins(self) -> None:
        """Increment total veins mined"""
        self.total_veins += 1

    def add_blocks(self, count: int) -> None:
        """Add to total blocks"""
        self.total_blocks += count

    def update_last_mined(self) -> None:
        """Update last mined timestamp"""
        self.last_mined = datetime.now().isoformat()

    def update_largest_vein(self, size: int) -> None:
        """Update largest vein if this one is bigger"""
        if size > self.largest_vein:
            self.largest_vein = size

    def get_formatted_stats(self) -> str:
        """Get formatted statistics string"""
        return (
            f"{ColorFormat.YELLOW}Player: {ColorFormat.WHITE}{self.player_name}\n"
            f"{ColorFormat.YELLOW}Total Veins Mined: {ColorFormat.WHITE}{self.total_veins}\n"
            f"{ColorFormat.YELLOW}Total Blocks Broken: {ColorFormat.WHITE}{self.total_blocks}\n"
            f"{ColorFormat.YELLOW}Largest Vein: {ColorFormat.WHITE}{self.largest_vein} blocks\n"
            f"{ColorFormat.YELLOW}Last Mined: {ColorFormat.WHITE}{self.last_mined if self.last_mined else 'Never'}"
        )


class StatisticsTracker:
    """Tracks vein mining statistics for players"""

    LOG_TAG = "[VeinMiner] "
    DEFAULT_CONNECT_TIMEOUT = 5
    MAX_TABLE_PREFIX_LENGTH = 32

    @staticmethod
    def _as_int(value, default: int = 0) -> int:
        """Best-effort integer conversion."""
        try:
            return int(value)
        except Exception:
            return default

    def __init__(self, plugin):
        self.plugin = plugin
        self.player_stats: Dict[str, PlayerStats] = {}
        self.achieved_milestones: Dict[str, Set[int]] = {}
        self.saving = False
        self.needs_save = False

        self.mysql_connection = None
        self.mysql_settings: Dict[str, Any] = {}
        self.mysql_table_prefix = "veinminer_"

        config = plugin.config
        stats_config = config.get("statistics", {})

        self.enabled = stats_config.get("enabled", True)
        self.save_to_file = stats_config.get("save-to-file", True)
        self.milestones_enabled = stats_config.get("milestones", {}).get("enabled", True)
        self.milestone_thresholds = stats_config.get("milestones", {}).get("thresholds", [])
        self.storage = str(stats_config.get("storage", "yaml")).lower()
        self.mysql_config = stats_config.get("mysql", {})

        # Legacy toggle support: [statistics.mysql].enabled = true
        if isinstance(self.mysql_config, dict) and self.mysql_config.get("enabled", False):
            self.storage = "mysql"

        if self.storage not in {"yaml", "mysql"}:
            self.plugin.logger.warning(
                f"{self.LOG_TAG}Unknown statistics storage '{self.storage}', falling back to yaml."
            )
            self.storage = "yaml"

        # Default milestones if not configured
        if not self.milestone_thresholds:
            self.milestone_thresholds = [100, 500, 1000, 5000, 10000]

        self.persistence_enabled = False
        if self.storage == "mysql":
            self.persistence_enabled = self.initialize_mysql_backend()
            if not self.persistence_enabled:
                self.storage = "yaml"
                self.persistence_enabled = bool(self.save_to_file)
                if self.persistence_enabled:
                    self.plugin.logger.warning(
                        f"{self.LOG_TAG}Falling back to YAML stats backend due to MySQL initialization failure."
                    )
        else:
            self.persistence_enabled = bool(self.save_to_file)

        if self.enabled and self.persistence_enabled:
            self.load_stats()

    def initialize_mysql_backend(self) -> bool:
        """Initialize MySQL backend and create tables if needed."""
        if mysql_connector is None:
            self.plugin.logger.error(
                f"{self.LOG_TAG}MySQL storage selected but mysql-connector-python is not installed."
            )
            return False

        if not isinstance(self.mysql_config, dict):
            self.plugin.logger.error(f"{self.LOG_TAG}Invalid statistics.mysql configuration block.")
            return False

        host = str(self.mysql_config.get("host", "127.0.0.1"))
        port = self._as_int(self.mysql_config.get("port", 3306), 3306)
        if port < 1 or port > 65535:
            port = 3306

        database = str(self.mysql_config.get("database", "")).strip()
        user = str(self.mysql_config.get("user", "")).strip()
        password = str(self.mysql_config.get("password", ""))
        connect_timeout = self._as_int(
            self.mysql_config.get("connect-timeout", self.DEFAULT_CONNECT_TIMEOUT),
            self.DEFAULT_CONNECT_TIMEOUT,
        )
        if connect_timeout < 1:
            connect_timeout = self.DEFAULT_CONNECT_TIMEOUT

        table_prefix = str(self.mysql_config.get("table-prefix", "veinminer_"))
        table_prefix = re.sub(r"[^a-zA-Z0-9_]", "", table_prefix)
        if len(table_prefix) > self.MAX_TABLE_PREFIX_LENGTH:
            table_prefix = table_prefix[: self.MAX_TABLE_PREFIX_LENGTH]
        if not table_prefix:
            table_prefix = "veinminer_"
        self.mysql_table_prefix = table_prefix

        if not database or not user:
            self.plugin.logger.error(
                f"{self.LOG_TAG}MySQL storage requires statistics.mysql.database and statistics.mysql.user."
            )
            return False

        self.mysql_settings = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "connection_timeout": connect_timeout,
            "autocommit": False,
        }

        try:
            conn = self.get_mysql_connection()
            self.ensure_mysql_tables(conn)
            if self.plugin.logging_enabled:
                self.plugin.logger.info(
                    f"{self.LOG_TAG}MySQL statistics backend initialized ({host}:{port}/{database})"
                )
            return True
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Failed to initialize MySQL backend: {str(e)}")
            self.close()
            return False

    def get_mysql_connection(self):
        """Return a live MySQL connection, reconnecting if necessary."""
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")

        if self.mysql_connection is not None:
            try:
                if self.mysql_connection.is_connected():
                    # Keep long-running server sessions healthy.
                    self.mysql_connection.ping(reconnect=True, attempts=1, delay=0)
                    return self.mysql_connection
            except Exception:
                try:
                    self.mysql_connection.close()
                except Exception:
                    pass
                self.mysql_connection = None

        self.mysql_connection = mysql_connector.connect(**self.mysql_settings)
        return self.mysql_connection

    def ensure_mysql_tables(self, conn) -> None:
        """Create MySQL tables if they do not exist."""
        stats_table = f"{self.mysql_table_prefix}player_stats"
        milestones_table = f"{self.mysql_table_prefix}player_milestones"

        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{stats_table}` (
                    `uuid` VARCHAR(36) NOT NULL,
                    `player_name` VARCHAR(64) NOT NULL,
                    `total_veins` BIGINT NOT NULL DEFAULT 0,
                    `total_blocks` BIGINT NOT NULL DEFAULT 0,
                    `largest_vein` INT NOT NULL DEFAULT 0,
                    `last_mined` VARCHAR(40) NOT NULL DEFAULT '',
                    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (`uuid`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{milestones_table}` (
                    `uuid` VARCHAR(36) NOT NULL,
                    `milestone` INT NOT NULL,
                    PRIMARY KEY (`uuid`, `milestone`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            conn.commit()
        finally:
            cursor.close()

    def record_vein_mine(self, player, block_count: int) -> None:
        """Record a vein mining event"""
        if not self.enabled:
            return

        uuid = str(player.unique_id)

        if uuid not in self.player_stats:
            self.player_stats[uuid] = PlayerStats(player.name)

        stats = self.player_stats[uuid]
        if stats.player_name != player.name:
            stats.player_name = player.name

        # Ensure milestones are loaded for this player
        self.ensure_milestones_loaded(uuid)

        previous_blocks = stats.total_blocks
        stats.increment_veins()
        stats.add_blocks(block_count)
        stats.update_last_mined()
        stats.update_largest_vein(block_count)

        # Check milestones
        if self.milestones_enabled:
            self.check_milestones(player, previous_blocks, stats.total_blocks)

        self.needs_save = True

        # Immediate save mode.
        if self.persistence_enabled and self.plugin.auto_save_interval == 0 and not self.saving:
            self.save_stats(async_save=False)

    def ensure_milestones_loaded(self, uuid: str) -> None:
        """Ensure player milestones are loaded from backend (lazy loading)."""
        if uuid in self.achieved_milestones:
            return

        if not self.persistence_enabled:
            return

        if self.storage == "mysql":
            self.load_player_milestones_from_mysql(uuid)
            return

        try:
            stats_file = os.path.join(self.plugin.data_folder, "stats.yml")
            if not os.path.exists(stats_file):
                return

            with open(stats_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if data and uuid in data:
                player_data = data[uuid]
                if "milestones" in player_data:
                    self.achieved_milestones[uuid] = set(player_data["milestones"])
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Error loading milestones for {uuid}: {str(e)}")

    def load_player_milestones_from_mysql(self, uuid: str) -> None:
        """Load milestones for a single player from MySQL."""
        if uuid in self.achieved_milestones:
            return

        try:
            conn = self.get_mysql_connection()
            milestones_table = f"{self.mysql_table_prefix}player_milestones"
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SELECT milestone FROM `{milestones_table}` WHERE uuid = %s",
                    (uuid,),
                )
                self.achieved_milestones[uuid] = {int(row[0]) for row in cursor.fetchall()}
            finally:
                cursor.close()
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Error loading MySQL milestones for {uuid}: {str(e)}")
            self.achieved_milestones[uuid] = set()

    def check_milestones(self, player, previous_blocks: int, current_blocks: int) -> None:
        """Check and announce milestones"""
        uuid = str(player.unique_id)

        if uuid not in self.achieved_milestones:
            self.achieved_milestones[uuid] = set()

        for threshold in self.milestone_thresholds:
            # If player just passed this threshold
            if previous_blocks < threshold <= current_blocks:
                # And hasn't been announced yet
                if threshold not in self.achieved_milestones[uuid]:
                    self.achieved_milestones[uuid].add(threshold)

                    # Announce milestone using configurable message template.
                    template = self.plugin.messages.get(
                        "milestone-reached",
                        "&6&l{player} &ehas mined &6{count} &eblocks with VeinMiner!",
                    )
                    message = (
                        template.replace("{player}", player.name)
                        .replace("{count}", str(threshold))
                        .replace("&", "§")
                    )

                    if self.plugin.broadcast_milestones:
                        for online_player in self.plugin.server.online_players:
                            online_player.send_message(message)
                    else:
                        player.send_message(message)

                    if self.plugin.logging_enabled:
                        self.plugin.logger.info(
                            f"{self.LOG_TAG}Player {player.name} reached milestone: {threshold} blocks"
                        )

    def get_stats(self, player) -> PlayerStats:
        """Get statistics for a player"""
        uuid = str(player.unique_id)

        if uuid not in self.player_stats:
            self.player_stats[uuid] = PlayerStats(player.name)

        return self.player_stats[uuid]

    def save_stats(self, async_save: bool = True) -> None:
        """Save statistics to configured backend."""
        if not self.persistence_enabled or self.saving:
            return
        if not self.needs_save:
            return

        self.saving = True
        try:
            if self.storage == "mysql":
                self.save_stats_to_mysql()
            else:
                self.save_stats_to_yaml()
        finally:
            self.saving = False

    def save_stats_to_yaml(self) -> None:
        """Save statistics to YAML file."""
        try:
            stats_file = os.path.join(self.plugin.data_folder, "stats.yml")

            # Create data folder if it doesn't exist
            os.makedirs(self.plugin.data_folder, exist_ok=True)

            data = {}

            for uuid, stats in self.player_stats.items():
                player_data = {
                    "name": str(stats.player_name),
                    "totalVeins": int(stats.total_veins),
                    "totalBlocks": int(stats.total_blocks),
                    "largestVein": int(stats.largest_vein),
                    "lastMined": str(stats.last_mined) if stats.last_mined else "",
                }

                # Include milestones - convert to plain Python types
                if uuid in self.achieved_milestones:
                    player_data["milestones"] = [int(m) for m in sorted(self.achieved_milestones[uuid])]

                data[str(uuid)] = player_data

            with open(stats_file, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, default_flow_style=False, sort_keys=True)

            if self.plugin.logging_enabled and self.plugin.log_config_loading:
                self.plugin.logger.info(f"{self.LOG_TAG}Statistics saved for {len(self.player_stats)} players")
            self.needs_save = False
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Error saving YAML statistics: {str(e)}")

    def save_stats_to_mysql(self) -> None:
        """Save statistics to MySQL backend."""
        try:
            conn = self.get_mysql_connection()
            stats_table = f"{self.mysql_table_prefix}player_stats"
            milestones_table = f"{self.mysql_table_prefix}player_milestones"

            cursor = conn.cursor()
            try:
                upsert_sql = (
                    f"INSERT INTO `{stats_table}` "
                    "(uuid, player_name, total_veins, total_blocks, largest_vein, last_mined) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE "
                    "player_name = VALUES(player_name), "
                    "total_veins = VALUES(total_veins), "
                    "total_blocks = VALUES(total_blocks), "
                    "largest_vein = VALUES(largest_vein), "
                    "last_mined = VALUES(last_mined)"
                )

                delete_milestones_sql = f"DELETE FROM `{milestones_table}` WHERE uuid = %s"
                insert_milestone_sql = f"INSERT INTO `{milestones_table}` (uuid, milestone) VALUES (%s, %s)"

                for uuid, stats in self.player_stats.items():
                    cursor.execute(
                        upsert_sql,
                        (
                            str(uuid),
                            str(stats.player_name),
                            int(stats.total_veins),
                            int(stats.total_blocks),
                            int(stats.largest_vein),
                            str(stats.last_mined) if stats.last_mined else "",
                        ),
                    )

                    cursor.execute(delete_milestones_sql, (str(uuid),))
                    milestones = self.achieved_milestones.get(uuid, set())
                    if milestones:
                        cursor.executemany(
                            insert_milestone_sql,
                            [(str(uuid), int(m)) for m in sorted(milestones)],
                        )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

            if self.plugin.logging_enabled and self.plugin.log_config_loading:
                self.plugin.logger.info(
                    f"{self.LOG_TAG}MySQL statistics saved for {len(self.player_stats)} players"
                )
            self.needs_save = False
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Error saving MySQL statistics: {str(e)}")

    def load_stats(self) -> None:
        """Load statistics from configured backend."""
        self.player_stats.clear()
        self.achieved_milestones.clear()

        if self.storage == "mysql":
            self.load_stats_from_mysql()
        else:
            self.load_stats_from_yaml()

    def load_stats_from_yaml(self) -> None:
        """Load statistics from YAML file."""
        try:
            stats_file = os.path.join(self.plugin.data_folder, "stats.yml")

            if not os.path.exists(stats_file):
                return

            with open(stats_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if not data:
                return

            for uuid, player_data in data.items():
                stats = PlayerStats(player_data.get("name", "Unknown"))
                stats.total_veins = self._as_int(player_data.get("totalVeins", 0))
                stats.total_blocks = self._as_int(player_data.get("totalBlocks", 0))
                stats.largest_vein = self._as_int(player_data.get("largestVein", 0))
                stats.last_mined = player_data.get("lastMined", "")

                self.player_stats[uuid] = stats

                # Load milestones
                if "milestones" in player_data:
                    self.achieved_milestones[uuid] = {
                        self._as_int(m) for m in player_data.get("milestones", []) if m is not None
                    }

            if self.plugin.logging_enabled and self.plugin.log_config_loading:
                self.plugin.logger.info(f"{self.LOG_TAG}Loaded statistics for {len(self.player_stats)} players")
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Error loading YAML statistics: {str(e)}")

    def load_stats_from_mysql(self) -> None:
        """Load statistics from MySQL backend."""
        try:
            conn = self.get_mysql_connection()
            stats_table = f"{self.mysql_table_prefix}player_stats"
            milestones_table = f"{self.mysql_table_prefix}player_milestones"

            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SELECT uuid, player_name, total_veins, total_blocks, largest_vein, last_mined "
                    f"FROM `{stats_table}`"
                )
                for row in cursor.fetchall():
                    uuid = str(row[0])
                    stats = PlayerStats(str(row[1]))
                    stats.total_veins = int(row[2])
                    stats.total_blocks = int(row[3])
                    stats.largest_vein = int(row[4])
                    stats.last_mined = str(row[5]) if row[5] else ""
                    self.player_stats[uuid] = stats

                cursor.execute(f"SELECT uuid, milestone FROM `{milestones_table}`")
                for row in cursor.fetchall():
                    uuid = str(row[0])
                    milestone = int(row[1])
                    if uuid not in self.achieved_milestones:
                        self.achieved_milestones[uuid] = set()
                    self.achieved_milestones[uuid].add(milestone)
            finally:
                cursor.close()

            if self.plugin.logging_enabled and self.plugin.log_config_loading:
                self.plugin.logger.info(
                    f"{self.LOG_TAG}Loaded MySQL statistics for {len(self.player_stats)} players"
                )
        except Exception as e:
            self.plugin.logger.error(f"{self.LOG_TAG}Error loading MySQL statistics: {str(e)}")

    def close(self) -> None:
        """Close backend resources."""
        if self.mysql_connection is not None:
            try:
                self.mysql_connection.close()
            except Exception:
                pass
            self.mysql_connection = None
