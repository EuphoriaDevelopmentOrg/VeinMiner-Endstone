"""
VeinMiner Plugin for Endstone
A powerful vein mining plugin that allows players to mine entire veins of ores, trees, and leaves with a single break.
"""

from endstone.plugin import Plugin
from endstone.event import event_handler, EventPriority, BlockBreakEvent, PlayerJoinEvent
from endstone import ColorFormat
from endstone.inventory import ItemStack
from typing import Set, Dict, List, Optional, Tuple
from collections import deque
import time
import random


class VeinMinerPlugin(Plugin):
    """Main plugin class for VeinMiner"""
    
    api_version = "0.10"
    version = "2.0.0"
    
    commands = {
        "veinminer": {
            "description": "Mine entire veins at once!",
            "usages": ["/veinminer help", "/veinminer reload", "/veinminer stats", "/veinminer toggle"],
            "aliases": ["vm", "vmine"],
            "permissions": ["veinminer.command"]
        }
    }
    
    permissions = {
        "veinminer.use": {
            "description": "Allows players to use vein mining",
            "default": True
        },
        "veinminer.command": {
            "description": "Allows use of /veinminer command",
            "default": True
        },
        "veinminer.reload": {
            "description": "Allows reloading the plugin configuration",
            "default": "op"
        },
        "veinminer.stats": {
            "description": "Allows viewing vein mining statistics",
            "default": True
        },
        "veinminer.toggle": {
            "description": "Allows toggling vein mining on/off",
            "default": True
        }
    }
    
    # Constants
    DEFAULT_MAX_BLOCKS = 64
    UNLIMITED_MAX_BLOCKS = 4096
    NEIGHBOR_RANGE = 1  # -1 to 1 for 3x3x3 cube
    DEFAULT_DURABILITY_MULTIPLIER = 1.0
    MAX_CACHE_SIZE = 1000
    COOLDOWN_MS = 100  # Cooldown between vein mining operations
    BATCH_SIZE = 10  # Process blocks in batches
    MIN_WORLD_HEIGHT = -64  # Minimum Y coordinate
    MAX_WORLD_HEIGHT = 320  # Maximum Y coordinate
    
    def __init__(self):
        super().__init__()
        self.max_blocks = self.DEFAULT_MAX_BLOCKS
        self.vein_blocks: Set[str] = set()
        self.tool_validation_cache: Dict[str, bool] = {}
        
        # Configuration settings
        self.auto_pickup_enabled = True
        self.full_inventory_action = "drop"
        self.inventory_full_message = ""
        self.auto_smelt_enabled = False
        self.auto_smelt_require_fortune = False
        self.auto_smelt_whitelist: Set[str] = set()
        self.auto_smelt_give_xp = True
        self.auto_smelt_xp_multiplier = 0.5
        self.logging_enabled = True
        self.log_vein_mining = True
        self.log_config_loading = True
        self.durability_multiplier = self.DEFAULT_DURABILITY_MULTIPLIER
        self.performance_logging = False
        
        # New config options
        self.min_vein_size = 2
        self.cooldown_ms = self.COOLDOWN_MS
        self.activation_mode = "sneak"  # sneak, stand, always
        self.per_block_permissions = False
        self.require_correct_tool = True
        self.max_reach_distance = 100
        self.respect_unbreaking = True
        self.break_on_exceed = True
        self.mining_pattern = "adjacent"
        self.pattern_radius = 1
        self.include_diagonals = True
        self.vertical_range = 4
        self.horizontal_range = 4
        
        # Limits and anti-abuse
        self.enable_limits = False
        self.max_veins_per_day = 1000
        self.max_blocks_per_day = 10000
        self.max_veins_per_minute = 60
        self.temporary_block_duration = 5
        self.log_suspicious_activity = True
        
        # Statistics
        self.auto_save_interval = 300
        self.track_per_block_type = True
        self.broadcast_milestones = True
        
        # Extended logging
        self.log_player_toggle = False
        self.log_cooldown_violations = False
        self.log_errors_to_file = True
        
        # Messages
        self.messages: Dict[str, str] = {}
        
        # Features
        self.disabled_worlds: List[str] = []
        self.particles_enabled = True
        self.sounds_enabled = True
        self.particle_type = "explosion"
        self.particle_count = 3
        self.particle_radius = 0.5
        self.particle_per_block = False
        self.completion_sound = "random.levelup"
        self.sound_volume = 1.0
        self.sound_pitch = 1.0
        self.per_block_sound = False
        self.block_sound = "dig.stone"
        self.update_checker_enabled = True
        self.github_repo = "EuphoriaDevelopmentOrg/VeinMiner-Endstone"
        
        # Block filtering
        self.configured_blocks: Dict[str, bool] = {}
        self.ores_enabled = True
        self.logs_enabled = True
        self.leaves_enabled = True
        
        # Components
        self.stats_tracker = None
        self.vein_miner_command = None
        self.disabled_players: Set[str] = set()
        
        # Performance tracking
        self.last_vein_mine: Dict[str, int] = {}  # player_id -> timestamp
        self.processing_vein: Set[str] = set()  # players currently processing veins
        self.last_error_message: Dict[str, int] = {}  # player_id -> timestamp for rate limiting
        self.last_cooldown_message: Dict[str, int] = {}  # player_id -> timestamp for cooldown messages
        self._neighbor_offsets = None  # Cache for BFS neighbor offsets
        
        # Limits tracking
        self.daily_vein_count: Dict[str, int] = {}  # player_id -> count
        self.daily_block_count: Dict[str, int] = {}  # player_id -> count
        self.minute_vein_count: Dict[str, List[int]] = {}  # player_id -> list of timestamps
        self.blocked_players: Dict[str, int] = {}  # player_id -> unblock timestamp
        
    def on_load(self) -> None:
        """Called when the plugin is loaded"""
        self.logger.info("VeinMiner plugin loaded!")
        
    def on_enable(self) -> None:
        """Called when the plugin is enabled"""
        # Load configuration
        self.load_config()
        
        # Load vein blocks
        if not self.vein_blocks:
            self.load_vein_blocks()
            
        # Initialize statistics tracker
        from endstone_vein_miner.statistics_tracker import StatisticsTracker
        self.stats_tracker = StatisticsTracker(self)

        # Schedule periodic stats auto-save (seconds -> ticks)
        if self.stats_tracker and self.stats_tracker.persistence_enabled and self.auto_save_interval > 0:
            save_period_ticks = max(20, int(self.auto_save_interval * 20))

            def auto_save_stats():
                if self.stats_tracker and self.stats_tracker.needs_save:
                    self.stats_tracker.save_stats(async_save=False)

            self.server.scheduler.run_task(self, auto_save_stats, delay=save_period_ticks, period=save_period_ticks)
        
        # Register event handlers - THIS IS REQUIRED!
        self.register_events(self)
        
        if self.debug_logging:
            self.logger.info("[DEBUG] Event handlers registered successfully!")
            self.logger.info("[DEBUG] Checking event handler setup...")
            self.logger.info(f"[DEBUG] BlockBreakEvent handler exists: {hasattr(self, 'on_block_break')}")
            self.logger.info(f"[DEBUG] Handler is callable: {callable(getattr(self, 'on_block_break', None))}")
        
        # Schedule periodic cache cleanup (every 30 minutes = 36000 ticks)
        def cleanup_cache():
            cache_cleared = False
            
            # Clean tool validation cache
            if len(self.tool_validation_cache) > self.MAX_CACHE_SIZE:
                self.tool_validation_cache.clear()
                cache_cleared = True
            
            # Clean old cooldown entries (older than 5 minutes)
            current_time = int(time.time() * 1000)
            old_cooldowns = [
                player_id for player_id, timestamp in self.last_vein_mine.items()
                if current_time - timestamp > 300000  # 5 minutes
            ]
            for player_id in old_cooldowns:
                del self.last_vein_mine[player_id]
            
            # Clean old error message rate limit entries
            old_errors = [
                player_id for player_id, timestamp in self.last_error_message.items()
                if current_time - timestamp > 300000  # 5 minutes
            ]
            for player_id in old_errors:
                del self.last_error_message[player_id]
            
            # Clean old cooldown message entries
            old_cooldown_messages = [
                player_id for player_id, timestamp in self.last_cooldown_message.items()
                if current_time - timestamp > 300000  # 5 minutes
            ]
            for player_id in old_cooldown_messages:
                del self.last_cooldown_message[player_id]
            
            if cache_cleared and self.logging_enabled and self.log_config_loading:
                self.logger.info("[Cache] Caches cleaned (tool validation + cooldowns + error rate limits)")
        
        self.server.scheduler.run_task(self, cleanup_cache, delay=36000, period=36000)
        
        # Schedule daily limit reset (every 24 hours = 1728000 ticks)
        if self.enable_limits:
            def reset_daily_limits():
                self.daily_vein_count.clear()
                self.daily_block_count.clear()
                if self.logging_enabled:
                    self.logger.info("[Limits] Daily limits reset")
            
            self.server.scheduler.run_task(self, reset_daily_limits, delay=1728000, period=1728000)
        
        # Startup message
        self.logger.info(f"Plugin enabled (v{self.version}) - Max blocks: {self.max_blocks}, Vein blocks: {len(self.vein_blocks)}")
        
        # Check for updates
        if self.update_checker_enabled:
            self.check_for_updates()
            
    def on_disable(self) -> None:
        """Called when the plugin is disabled"""
        # Wait for any ongoing vein processing to complete (max 5 seconds)
        wait_start = time.time()
        while self.processing_vein and (time.time() - wait_start) < 5:
            time.sleep(0.1)
        
        if self.processing_vein:
            self.logger.warning(f"Shutting down with {len(self.processing_vein)} active vein operations")
        
        # Save statistics synchronously
        if self.stats_tracker:
            self.stats_tracker.save_stats(async_save=False)
            self.stats_tracker.close()
        
        self.logger.info(ColorFormat.RED + "VeinMiner plugin disabled!")
    
    def on_command(self, sender, command, args):
        """Handle command execution"""
        from endstone_vein_miner.vein_miner_command import VeinMinerCommand
        return VeinMinerCommand.handle_command(self, sender, args)
        
    def load_config(self) -> None:
        """Load and validate configuration settings"""
        # Save default config if it doesn't exist
        self.save_default_config()
        
        config = self.config
        
        # Validate and load max blocks
        self.max_blocks = config.get("max-blocks", self.DEFAULT_MAX_BLOCKS)
        if self.max_blocks == -1:
            self.max_blocks = self.UNLIMITED_MAX_BLOCKS
            self.logger.warning(
                f"max-blocks is set to -1 (unlimited). Applying safety cap: {self.UNLIMITED_MAX_BLOCKS}"
            )
        elif self.max_blocks < 1 or self.max_blocks > self.UNLIMITED_MAX_BLOCKS:
            self.logger.warning(f"Invalid max-blocks value ({self.max_blocks}), using default: {self.DEFAULT_MAX_BLOCKS}")
            self.max_blocks = self.DEFAULT_MAX_BLOCKS
        
        # Load min vein size
        self.min_vein_size = config.get("min-vein-size", 2)
        if self.min_vein_size < 1:
            self.min_vein_size = 2
        
        # Load cooldown
        self.cooldown_ms = config.get("cooldown-ms", self.COOLDOWN_MS)
        if self.cooldown_ms < 0:
            self.cooldown_ms = self.COOLDOWN_MS
        
        # Load batch size (for performance tuning)
        self.batch_size = config.get("batch-size", self.BATCH_SIZE)
        if self.batch_size < 1 or self.batch_size > 100:
            self.logger.warning(f"Invalid batch-size value ({self.batch_size}), using default: {self.BATCH_SIZE}")
            self.batch_size = self.BATCH_SIZE
            
        # Load auto-pickup settings
        self.auto_pickup_enabled = config.get("auto-pickup", {}).get("enabled", True)
        self.full_inventory_action = config.get("auto-pickup", {}).get("full-inventory-action", "drop").lower()
        if self.full_inventory_action not in ["drop", "delete"]:
            self.logger.warning(f"Invalid full-inventory-action value ({self.full_inventory_action}), using default: drop")
            self.full_inventory_action = "drop"

        # Load auto-smelt settings (support both dashed and underscored keys).
        auto_smelt_config = config.get("auto-smelt", config.get("auto_smelt", {}))
        self.auto_smelt_enabled = bool(auto_smelt_config.get("enabled", auto_smelt_config.get("enable", False)))
        self.auto_smelt_require_fortune = bool(
            auto_smelt_config.get("require-fortune", auto_smelt_config.get("require_fortune", False))
        )
        self.auto_smelt_give_xp = bool(auto_smelt_config.get("give-xp", auto_smelt_config.get("give_xp", True)))
        raw_auto_smelt_xp_multiplier = auto_smelt_config.get("xp-multiplier", auto_smelt_config.get("xp_multiplier", 0.5))
        try:
            self.auto_smelt_xp_multiplier = float(raw_auto_smelt_xp_multiplier)
        except (TypeError, ValueError):
            self.logger.warning("Invalid auto-smelt xp-multiplier value, using default: 0.5")
            self.auto_smelt_xp_multiplier = 0.5
        if self.auto_smelt_xp_multiplier < 0:
            self.logger.warning("Invalid auto-smelt xp-multiplier value, using default: 0.5")
            self.auto_smelt_xp_multiplier = 0.5

        auto_smelt_whitelist = auto_smelt_config.get(
            "whitelist",
            auto_smelt_config.get("allow-list", auto_smelt_config.get("allow_list", [])),
        )
        self.auto_smelt_whitelist = set()
        if isinstance(auto_smelt_whitelist, list):
            for entry in auto_smelt_whitelist:
                if isinstance(entry, str) and entry.strip():
                    self.auto_smelt_whitelist.add(self.normalize_block_id(entry))
        elif auto_smelt_whitelist:
            self.logger.warning("Invalid auto-smelt whitelist format, expected list of block ids")
        
        # Load durability multiplier
        self.durability_multiplier = config.get("tool-durability", {}).get("multiplier", self.DEFAULT_DURABILITY_MULTIPLIER)
        if self.durability_multiplier < 0:
            self.logger.warning(f"Invalid durability multiplier, using default: {self.DEFAULT_DURABILITY_MULTIPLIER}")
            self.durability_multiplier = self.DEFAULT_DURABILITY_MULTIPLIER
        self.respect_unbreaking = config.get("tool-durability", {}).get("respect-unbreaking", True)
        self.break_on_exceed = config.get("tool-durability", {}).get("break-on-exceed", True)
            
        # Load XP multiplier settings
        self.xp_enabled = config.get("experience", {}).get("enabled", True)
        self.xp_bonus_enabled = config.get("experience", {}).get("bonus-enabled", True)
        self.xp_bonus_per_blocks = config.get("experience", {}).get("bonus-per-blocks", 10)
        self.xp_bonus_multiplier = config.get("experience", {}).get("multiplier", 0.05)
        
        if self.xp_bonus_per_blocks < 1:
            self.logger.warning(f"Invalid bonus-per-blocks value, using default: 10")
            self.xp_bonus_per_blocks = 10
        if self.xp_bonus_multiplier < 0:
            self.logger.warning("Invalid experience multiplier value, using default: 0.05")
            self.xp_bonus_multiplier = 0.05
        
        # Load statistics settings
        stats_config = config.get("statistics", {})
        self.auto_save_interval = stats_config.get("auto-save-interval", 300)
        self.track_per_block_type = stats_config.get("track-per-block-type", True)
        self.broadcast_milestones = stats_config.get("broadcast-milestones", True)
        if self.auto_save_interval < 0:
            self.logger.warning("Invalid auto-save-interval value, using default: 300")
            self.auto_save_interval = 300
            
        # Load logging settings
        logging_config = config.get("logging", {})
        self.logging_enabled = logging_config.get("enabled", True)
        self.log_vein_mining = logging_config.get("log-vein-mining", True)
        self.log_config_loading = logging_config.get("log-config-loading", True)
        self.debug_logging = logging_config.get("debug-logging", False)
        self.performance_logging = logging_config.get("performance-logging", False)
        self.log_player_toggle = logging_config.get("log-player-toggle", False)
        self.log_cooldown_violations = logging_config.get("log-cooldown-violations", False)
        self.log_errors_to_file = logging_config.get("log-errors-to-file", True)
        
        # Load world restrictions
        self.disabled_worlds = config.get("disabled-worlds", [])
        if not isinstance(self.disabled_worlds, list):
            self.logger.warning("Invalid disabled-worlds config, using empty list")
            self.disabled_worlds = []
        
        # Load activation settings
        activation_config = config.get("activation", {})
        self.activation_mode = activation_config.get("mode", "sneak").lower()
        if self.activation_mode not in ["sneak", "stand", "always"]:
            self.logger.warning(f"Invalid activation mode '{self.activation_mode}', using 'sneak'")
            self.activation_mode = "sneak"
        self.per_block_permissions = activation_config.get("per-block-permissions", False)
        self.require_correct_tool = activation_config.get("require-correct-tool", True)
        self.max_reach_distance = activation_config.get("max-reach-distance", 100)
        if self.max_reach_distance < 1:
            self.max_reach_distance = 100

        # Load mining pattern settings
        pattern_config = config.get("mining-pattern", {})
        self.mining_pattern = str(pattern_config.get("pattern", "adjacent")).lower()
        if self.mining_pattern not in ["adjacent", "cube", "sphere", "vertical", "horizontal"]:
            self.logger.warning(f"Invalid mining pattern '{self.mining_pattern}', using 'adjacent'")
            self.mining_pattern = "adjacent"

        self.pattern_radius = pattern_config.get("radius", 1)
        self.include_diagonals = pattern_config.get("include-diagonals", True)
        self.vertical_range = pattern_config.get("vertical-range", 4)
        self.horizontal_range = pattern_config.get("horizontal-range", 4)

        if self.pattern_radius < 1:
            self.pattern_radius = 1
        if self.pattern_radius > 6:
            self.logger.warning("Pattern radius too high, clamping to 6 for safety")
            self.pattern_radius = 6
        if self.vertical_range < 1:
            self.vertical_range = 1
        if self.horizontal_range < 1:
            self.horizontal_range = 1
        if self.vertical_range > 16:
            self.vertical_range = 16
        if self.horizontal_range > 16:
            self.horizontal_range = 16

        # Load limits and anti-abuse settings
        limits_config = config.get("limits", {})
        self.enable_limits = limits_config.get("enable-limits", False)
        self.max_veins_per_day = limits_config.get("max-veins-per-day", 1000)
        self.max_blocks_per_day = limits_config.get("max-blocks-per-day", 10000)

        anti_abuse_config = config.get("anti-abuse", {})
        self.max_veins_per_minute = anti_abuse_config.get("max-veins-per-minute", 60)
        self.temporary_block_duration = anti_abuse_config.get("temporary-block-duration", 5)
        self.log_suspicious_activity = anti_abuse_config.get("log-suspicious-activity", True)

        # Validate limits and anti-abuse values
        try:
            self.max_veins_per_day = int(self.max_veins_per_day)
            if self.max_veins_per_day < 0:
                raise ValueError()
        except (TypeError, ValueError):
            self.logger.warning("Invalid max-veins-per-day value, using default: 1000")
            self.max_veins_per_day = 1000

        try:
            self.max_blocks_per_day = int(self.max_blocks_per_day)
            if self.max_blocks_per_day < 0:
                raise ValueError()
        except (TypeError, ValueError):
            self.logger.warning("Invalid max-blocks-per-day value, using default: 10000")
            self.max_blocks_per_day = 10000

        try:
            self.max_veins_per_minute = int(self.max_veins_per_minute)
            if self.max_veins_per_minute < 0:
                raise ValueError()
        except (TypeError, ValueError):
            self.logger.warning("Invalid max-veins-per-minute value, using default: 60")
            self.max_veins_per_minute = 60

        try:
            self.temporary_block_duration = int(self.temporary_block_duration)
            if self.temporary_block_duration < 1:
                raise ValueError()
        except (TypeError, ValueError):
            self.logger.warning("Invalid temporary-block-duration value, using default: 5")
            self.temporary_block_duration = 5
        
        # Load effects
        effects_config = config.get("effects", {})
        particles_config = effects_config.get("particles", {})
        sounds_config = effects_config.get("sounds", {})

        # Support both nested-table and boolean config formats.
        self.particles_enabled = particles_config.get("enabled", True) if isinstance(particles_config, dict) else bool(particles_config)
        self.sounds_enabled = sounds_config.get("enabled", True) if isinstance(sounds_config, dict) else bool(sounds_config)
        if isinstance(particles_config, dict):
            self.particle_type = str(particles_config.get("type", "explosion"))
            self.particle_count = int(particles_config.get("count", 3))
            self.particle_radius = float(particles_config.get("radius", 0.5))
            self.particle_per_block = particles_config.get("per-block", False)
            if self.particle_count < 1:
                self.particle_count = 1
            if self.particle_count > 20:
                self.particle_count = 20
            if self.particle_radius < 0:
                self.particle_radius = 0
        if isinstance(sounds_config, dict):
            self.completion_sound = str(sounds_config.get("completion-sound", "random.levelup"))
            self.sound_volume = float(sounds_config.get("volume", 1.0))
            self.sound_pitch = float(sounds_config.get("pitch", 1.0))
            self.per_block_sound = sounds_config.get("per-block-sound", False)
            self.block_sound = str(sounds_config.get("block-sound", "dig.stone"))
            if self.sound_volume < 0:
                self.sound_volume = 0
            if self.sound_volume > 1.0:
                self.sound_volume = 1.0
            if self.sound_pitch < 0.5:
                self.sound_pitch = 0.5
            if self.sound_pitch > 2.0:
                self.sound_pitch = 2.0
        
        # Load update checker
        update_config = config.get("update-checker", {})
        self.update_checker_enabled = update_config.get("enabled", True)
        self.github_repo = update_config.get("repository", "EuphoriaDevelopmentOrg/VeinMiner-Endstone")
        
        # Load messages
        messages = config.get("messages", {})
        self.messages = {
            "reload-success": messages.get("reload-success", "&aConfiguration reloaded successfully!"),
            "vein-too-large": messages.get("vein-too-large", "&cVein size limited to {max} blocks!"),
            "cooldown-active": messages.get("cooldown-active", "&cPlease wait before vein mining again!"),
            "wrong-tool": messages.get("wrong-tool", "&cYou need the correct tool to vein mine this block!"),
            "no-permission": messages.get("no-permission", "&cYou don't have permission to vein mine this block type!"),
            "limit-reached": messages.get("limit-reached", "&cYou've reached your daily vein mining limit!"),
            "milestone-reached": messages.get("milestone-reached", "&6&l{player} &ehas mined &6{count} &eblocks with VeinMiner!"),
            "toggle-enabled": messages.get("toggle-enabled", "&aVein Mining enabled!"),
            "toggle-disabled": messages.get("toggle-disabled", "&cVein Mining disabled!"),
        }
        self.inventory_full_message = messages.get("inventory-full", "&eInventory full! {count} items were {action}.")
        
        # Load block category settings
        enabled_blocks = config.get("enabled-blocks", {})
        self.ores_enabled = enabled_blocks.get("ores", True)
        self.logs_enabled = enabled_blocks.get("logs", True)
        self.leaves_enabled = enabled_blocks.get("leaves", True)
        
        # Load specific block configurations
        self.configured_blocks = {}
        blocks_config = config.get("blocks", {})
        if blocks_config:
            for block_name, enabled in blocks_config.items():
                if isinstance(enabled, bool):
                    self.configured_blocks[block_name.upper()] = enabled
                    
        if self.logging_enabled and self.log_config_loading:
            self.logger.info(ColorFormat.GREEN + f"[Config] Auto-pickup: {'enabled' if self.auto_pickup_enabled else 'disabled'}")
            self.logger.info(ColorFormat.GREEN + f"[Config] Auto-smelt: {'enabled' if self.auto_smelt_enabled else 'disabled'}")
            self.logger.info(ColorFormat.GREEN + f"[Config] Full inventory action: {self.full_inventory_action}")
            self.logger.info(ColorFormat.GREEN + "[Config] Console logging: enabled")
            self.logger.info(ColorFormat.GREEN + f"[Config] Disabled worlds: {len(self.disabled_worlds)}")
            self.logger.info(ColorFormat.GREEN + f"[Config] Effects: {'enabled' if (self.particles_enabled or self.sounds_enabled) else 'disabled'}")
            self.logger.info(ColorFormat.GREEN + f"[Config] Durability multiplier: {self.durability_multiplier}x")
            self.logger.info(ColorFormat.GREEN + f"[Config] Mining pattern: {self.mining_pattern}")
            
    def reload_configuration(self) -> None:
        """Reload configuration and reset all caches"""
        # Flush/close active stats backend before reloading config so backend switches apply cleanly.
        if self.stats_tracker:
            try:
                self.stats_tracker.save_stats(async_save=False)
            except Exception:
                pass
            try:
                self.stats_tracker.close()
            except Exception:
                pass

        # Clear all caches and state
        self.vein_blocks.clear()
        self.tool_validation_cache.clear()
        self.last_vein_mine.clear()
        self.processing_vein.clear()
        self.last_error_message.clear()
        self.last_cooldown_message.clear()
        self._neighbor_offsets = None  # Reset cached offsets
        
        # Reload configuration
        self.load_config()
        self.load_vein_blocks()

        # Recreate stats tracker so new statistics/backend settings are applied.
        from endstone_vein_miner.statistics_tracker import StatisticsTracker
        self.stats_tracker = StatisticsTracker(self)
        
        if self.logging_enabled:
            self.logger.info(ColorFormat.GREEN + "Configuration reloaded and caches cleared")
    
    def is_player_blocked(self, player_id: str) -> bool:
        """Check if player is temporarily blocked for abuse"""
        if player_id not in self.blocked_players:
            return False
        
        unblock_time = self.blocked_players[player_id]
        current_time = int(time.time() * 1000)
        
        if current_time >= unblock_time:
            del self.blocked_players[player_id]
            return False
        
        return True
    
    def block_player_temporarily(self, player_id: str) -> None:
        """Temporarily block a player for abuse"""
        current_time = int(time.time() * 1000)
        self.blocked_players[player_id] = current_time + (self.temporary_block_duration * 60 * 1000)
    
    def check_rate_limits(self, player_id: str) -> bool:
        """Check if player exceeds rate limits. Returns True if OK, False if limited"""
        if self.max_veins_per_minute <= 0:
            return True
        
        current_time = int(time.time() * 1000)
        
        # Initialize or clean old entries
        if player_id not in self.minute_vein_count:
            self.minute_vein_count[player_id] = []
        
        # Remove entries older than 1 minute
        self.minute_vein_count[player_id] = [
            t for t in self.minute_vein_count[player_id]
            if current_time - t < 60000
        ]
        
        # Check limit
        if len(self.minute_vein_count[player_id]) >= self.max_veins_per_minute:
            if self.log_suspicious_activity:
                self.logger.warning(f"[Anti-Abuse] {player_id} exceeded rate limit ({self.max_veins_per_minute}/min)")
            return False
        
        return True
    
    def check_daily_limits(self, player_id: str, block_count: int) -> bool:
        """Check if player exceeds daily limits. Returns True if OK, False if limited"""
        if not self.enable_limits:
            return True
        
        vein_count = self.daily_vein_count.get(player_id, 0)
        blocks_count = self.daily_block_count.get(player_id, 0)
        
        if vein_count >= self.max_veins_per_day:
            return False
        
        if blocks_count + block_count > self.max_blocks_per_day:
            return False
        
        return True
    
    def record_vein_usage(self, player_id: str, block_count: int) -> None:
        """Record vein mining usage for limits tracking"""
        current_time = int(time.time() * 1000)
        
        # Rate limiting
        if player_id not in self.minute_vein_count:
            self.minute_vein_count[player_id] = []
        self.minute_vein_count[player_id].append(current_time)
        
        # Daily limits
        if self.enable_limits:
            self.daily_vein_count[player_id] = self.daily_vein_count.get(player_id, 0) + 1
            self.daily_block_count[player_id] = self.daily_block_count.get(player_id, 0) + block_count
    
    def send_message(self, player, message_key: str, **kwargs) -> None:
        """Send a formatted message to player"""
        if message_key in self.messages:
            message = self.messages[message_key]
            for key, value in kwargs.items():
                message = message.replace(f"{{{key}}}", str(value))
            message = message.replace("&", "§")
            player.send_message(message)
        
    def load_vein_blocks(self) -> None:
        """Load all vein-mineable block types into cache"""
        # Helper function
        def is_block_enabled(*block_names: str) -> bool:
            for block_name in block_names:
                block_key = block_name.upper()
                if block_key in self.configured_blocks:
                    return self.configured_blocks[block_key]
            return True
        
        # Ores
        ore_types = [
            ("COAL_ORE", "minecraft:coal_ore"),
            ("IRON_ORE", "minecraft:iron_ore"),
            ("GOLD_ORE", "minecraft:gold_ore"),
            ("DIAMOND_ORE", "minecraft:diamond_ore"),
            ("EMERALD_ORE", "minecraft:emerald_ore"),
            ("LAPIS_ORE", "minecraft:lapis_ore"),
            ("COPPER_ORE", "minecraft:copper_ore"),
            ("DEEPSLATE_COAL_ORE", "minecraft:deepslate_coal_ore"),
            ("DEEPSLATE_IRON_ORE", "minecraft:deepslate_iron_ore"),
            ("DEEPSLATE_GOLD_ORE", "minecraft:deepslate_gold_ore"),
            ("DEEPSLATE_DIAMOND_ORE", "minecraft:deepslate_diamond_ore"),
            ("DEEPSLATE_EMERALD_ORE", "minecraft:deepslate_emerald_ore"),
            ("DEEPSLATE_LAPIS_ORE", "minecraft:deepslate_lapis_ore"),
            ("DEEPSLATE_COPPER_ORE", "minecraft:deepslate_copper_ore"),
            ("QUARTZ_ORE", "minecraft:quartz_ore"),
            ("QUARTZ_ORE", "minecraft:nether_quartz_ore"),
            ("NETHER_GOLD_ORE", "minecraft:nether_gold_ore"),
        ]
        
        if self.ores_enabled:
            for config_name, block_id in ore_types:
                if is_block_enabled(config_name):
                    self.vein_blocks.add(block_id)
            
            # Special cases
            if is_block_enabled("REDSTONE_ORE"):
                self.vein_blocks.add("minecraft:redstone_ore")
                self.vein_blocks.add("minecraft:lit_redstone_ore")
                
            if is_block_enabled("DEEPSLATE_REDSTONE_ORE"):
                self.vein_blocks.add("minecraft:deepslate_redstone_ore")
                self.vein_blocks.add("minecraft:lit_deepslate_redstone_ore")
                
            # Ancient debris
            if is_block_enabled("ANCIENT_DEBRIS"):
                self.vein_blocks.add("minecraft:ancient_debris")
            
            # Amethyst clusters
            if is_block_enabled("AMETHYST_CLUSTER"):
                self.vein_blocks.add("minecraft:amethyst_cluster")
                self.vein_blocks.add("minecraft:large_amethyst_bud")
                self.vein_blocks.add("minecraft:medium_amethyst_bud")
                self.vein_blocks.add("minecraft:small_amethyst_bud")
                
        # Logs
        log_types = [
            ("OAK_LOG", "minecraft:oak_log", "LOG"),
            ("SPRUCE_LOG", "minecraft:spruce_log", "LOG"),
            ("BIRCH_LOG", "minecraft:birch_log", "LOG"),
            ("JUNGLE_LOG", "minecraft:jungle_log", "LOG"),
            ("ACACIA_LOG", "minecraft:acacia_log", "LOG2"),
            ("DARK_OAK_LOG", "minecraft:dark_oak_log", "LOG2"),
            ("MANGROVE_LOG", "minecraft:mangrove_log", "MANGROVE_LOG"),
            ("CHERRY_LOG", "minecraft:cherry_log", "CHERRY_LOG"),
            ("CRIMSON_STEM", "minecraft:crimson_stem", "CRIMSON_STEM"),
            ("WARPED_STEM", "minecraft:warped_stem", "WARPED_STEM"),
        ]
        
        if self.logs_enabled:
            for config_name, block_id, legacy_name in log_types:
                if is_block_enabled(config_name, legacy_name):
                    self.vein_blocks.add(block_id)
                        
        # Leaves
        leaves_types = [
            ("OAK_LEAVES", "minecraft:oak_leaves", "LEAVES"),
            ("SPRUCE_LEAVES", "minecraft:spruce_leaves", "LEAVES"),
            ("BIRCH_LEAVES", "minecraft:birch_leaves", "LEAVES"),
            ("JUNGLE_LEAVES", "minecraft:jungle_leaves", "LEAVES"),
            ("ACACIA_LEAVES", "minecraft:acacia_leaves", "LEAVES2"),
            ("DARK_OAK_LEAVES", "minecraft:dark_oak_leaves", "LEAVES2"),
            ("MANGROVE_LEAVES", "minecraft:mangrove_leaves", "MANGROVE_LEAVES"),
            ("CHERRY_LEAVES", "minecraft:cherry_leaves", "CHERRY_LEAVES"),
        ]
        
        if self.leaves_enabled:
            for config_name, block_id, legacy_name in leaves_types:
                if is_block_enabled(config_name, legacy_name):
                    self.vein_blocks.add(block_id)
                        
        if self.logging_enabled and self.log_config_loading:
            self.logger.info(ColorFormat.GREEN + f"[Config] Loaded {len(self.vein_blocks)} vein-mineable block types")
    
    @event_handler
    def on_player_join(self, event: PlayerJoinEvent):
        """Test event handler to verify events work"""
        if self.debug_logging:
            self.logger.info(f"[DEBUG] Player join event triggered: {event.player.name}")
            event.player.send_message(ColorFormat.YELLOW + "[DEBUG] VeinMiner event handler is working!")
            
    @event_handler(priority=EventPriority.HIGH)
    def on_block_break(self, event: BlockBreakEvent) -> None:
        """Handle block break events for vein mining"""
        if event.is_cancelled:
            return
        
        # Early validation
        if not event or not event.player or not event.block:
            return
            
        player = event.player
        block = event.block
        player_id = str(player.unique_id)
        
        # Debug: Log every block break
        if self.debug_logging:
            self.logger.info(f"[DEBUG] Block break: {block.type} by {player.name}, Sneaking: {player.is_sneaking}")
        
        # Security: Check if player is temporarily blocked
        if self.is_player_blocked(player_id):
            if self.debug_logging:
                self.logger.info(f"[DEBUG] {player.name} is temporarily blocked")
            return
        
        # Check permission
        if not player.has_permission("veinminer.use"):
            if self.debug_logging:
                self.logger.info(f"[DEBUG] No permission for {player.name}")
            return
            
        # Check if player has toggled vein mining off
        if player.unique_id in self.disabled_players:
            if self.debug_logging:
                self.logger.info(f"[DEBUG] Vein mining disabled for {player.name}")
            return
        
        # Security: Check rate limits
        if not self.check_rate_limits(player_id):
            if self.debug_logging:
                self.logger.info(f"[DEBUG] {player.name} exceeded rate limit")
            self.block_player_temporarily(player_id)
            self.send_message(player, "limit-reached")
            return
        
        # Check if player is already processing a vein (prevent concurrent operations)
        if player_id in self.processing_vein:
            if self.debug_logging:
                self.logger.info(f"[DEBUG] {player.name} already processing a vein")
            return
            
        # Check cooldown
        current_time = int(time.time() * 1000)  # milliseconds
        last_mine = self.last_vein_mine.get(player_id, 0)
        if current_time - last_mine < self.cooldown_ms:
            if self.debug_logging:
                self.logger.info(f"[DEBUG] {player.name} is on cooldown")
            if self.log_cooldown_violations:
                self.logger.info(f"[Cooldown] {player.name} attempted vein mining while on cooldown")
            last_notice = self.last_cooldown_message.get(player_id, 0)
            if current_time - last_notice > 1000:
                self.send_message(player, "cooldown-active")
                self.last_cooldown_message[player_id] = current_time
            return
            
        # Check if world is disabled
        if player.level.name in self.disabled_worlds:
            if self.debug_logging:
                self.logger.info(f"[DEBUG] World {player.level.name} is disabled")
            return
        
        # Check activation mode
        activation_ok = False
        if self.activation_mode == "always":
            activation_ok = True
        elif self.activation_mode == "sneak":
            activation_ok = player.is_sneaking
        elif self.activation_mode == "stand":
            activation_ok = not player.is_sneaking
            
        if not activation_ok:
            if self.debug_logging:
                self.logger.info(f"[DEBUG] Activation mode not met (mode: {self.activation_mode}, sneaking: {player.is_sneaking})")
            return
            
        # Check if block is vein-mineable
        block_id = block.type
        
        if block_id not in self.vein_blocks:
            return
        
        # Security: Check per-block permissions if enabled
        if self.per_block_permissions:
            block_perm = f"veinminer.blocks.{block_id.replace('minecraft:', '')}"
            if not player.has_permission(block_perm):
                if self.debug_logging:
                    self.logger.info(f"[DEBUG] {player.name} lacks permission: {block_perm}")
                self.send_message(player, "no-permission")
                return
        
        # Mark player as processing
        self.processing_vein.add(player_id)
        
        try:
            # Get the tool being used (main hand)
            tool = player.inventory.item_in_main_hand
            
            # Check if proper tool when enabled in config.
            if self.require_correct_tool and not self.is_proper_tool(block_id, tool):
                self.send_message(player, "wrong-tool")
                return
                
            # Find all connected blocks of the same type
            start_time = time.time() if self.performance_logging else 0
            vein = self.find_vein(block, player.dimension)
            
            # Validation: Ensure vein is valid
            if not vein or not isinstance(vein, set):
                if self.debug_logging:
                    self.logger.warning(f"[DEBUG] Invalid vein returned for {player.name}")
                return
            
            if self.performance_logging and vein:
                elapsed = (time.time() - start_time) * 1000
                self.logger.info(f"[Performance] Vein search took {elapsed:.2f}ms for {len(vein)} blocks")
            
            # Enforce min and max blocks limit
            if vein and len(vein) >= self.min_vein_size:
                actual_size = len(vein)
                
                # Sanity check: Prevent absurdly large veins
                if actual_size > self.max_blocks * 2:
                    self.logger.warning(f"[Security] Abnormally large vein detected ({actual_size} blocks) for {player.name}")
                    if self.log_suspicious_activity:
                        self.logger.warning(f"[Anti-Abuse] Suspicious vein size: {actual_size} blocks at {block.x}, {block.y}, {block.z}")
                    return
                
                # Security: Check daily limits
                if not self.check_daily_limits(player_id, min(actual_size, self.max_blocks)):
                    if self.debug_logging:
                        self.logger.info(f"[DEBUG] {player.name} exceeded daily limit")
                    self.send_message(player, "limit-reached")
                    return
                
                # Notify if vein was truncated
                if actual_size >= self.max_blocks and self.max_blocks > 0:
                    self.send_message(player, "vein-too-large", max=self.max_blocks)
                
                if actual_size <= self.max_blocks:
                    # Cancel the event to prevent normal drop behavior
                    event.is_cancelled = True
                    
                    # Log vein mining activation
                    if self.logging_enabled and self.log_vein_mining:
                        self.logger.info(ColorFormat.YELLOW + f"[VeinMine] Player: {player.name} | Block: {block_id} | Vein size: {actual_size}")
                    
                    # Process the vein mining
                    process_start = time.time() if self.performance_logging else 0
                    process_result = self.process_vein_mining(player, vein, tool)
                    successful_breaks = process_result.get("successful_breaks", 0)
                    xp_gained = process_result.get("xp_gained", 0)

                    # Security: Record usage for rate limiting and daily limits
                    if successful_breaks > 0:
                        self.record_vein_usage(player_id, successful_breaks)

                        # Record statistics
                        if self.stats_tracker:
                            self.stats_tracker.record_vein_mine(player, successful_breaks)

                        # Send status tip
                        tip = ColorFormat.GOLD + "Vein Mining: " + ColorFormat.WHITE + f"{successful_breaks} blocks"
                        if xp_gained > 0:
                            tip += ColorFormat.GRAY + f" (+{xp_gained} XP)"
                        player.send_tip(tip)
                    
                    if self.performance_logging:
                        elapsed = (time.time() - process_start) * 1000
                        self.logger.info(f"[Performance] Vein processing took {elapsed:.2f}ms for {successful_breaks} blocks")
                
                # Update last mine time
                self.last_vein_mine[player_id] = int(time.time() * 1000)
                
        except Exception as e:
            self.logger.error(f"Error during vein mining: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # Rate limit error messages to players (max once per 5 seconds)
            current_time = int(time.time() * 1000)
            last_error = self.last_error_message.get(player_id, 0)
            if current_time - last_error > 5000:
                player.send_message(ColorFormat.RED + "An error occurred during vein mining.")
                self.last_error_message[player_id] = current_time
        finally:
            # Always remove processing flag
            self.processing_vein.discard(player_id)
            
    def process_vein_mining(self, player, vein: Set, tool) -> Dict[str, int]:
        """Process the mining of all blocks in a vein and return summary stats."""
        if not vein or not player:
            return {"successful_breaks": 0, "xp_gained": 0}
        
        # Validation
        if not isinstance(vein, set) or len(vein) == 0:
            return {"successful_breaks": 0, "xp_gained": 0}
        
        vein_size = len(vein)
        start_time = time.time() if self.performance_logging else 0
        successful_breaks = 0
        total_xp = 0.0
        
        # Track items to give for auto-pickup
        items_to_give: Dict[str, int] = {}  # {item_type: count}
        
        try:
            # Process blocks in batches for better performance
            vein_list = list(vein)
            
            for i in range(0, len(vein_list), self.batch_size):
                batch = vein_list[i:i + self.batch_size]
                for vein_block in batch:
                    try:
                        result = self.process_block_break_internal(player, vein_block, tool, items_to_give)
                        if result.get("success", False):
                            successful_breaks += 1
                            total_xp += result.get("xp", 0.0)
                            
                            if self.particles_enabled and self.particle_per_block:
                                self.play_particle_effect(player, vein_block.location)
                            if self.sounds_enabled and self.per_block_sound:
                                self.play_sound_effect(player, vein_block.location, per_block=True)
                    except Exception as e:
                        if self.debug_logging:
                            self.logger.error(f"Error breaking individual block: {str(e)}")
                        continue
            
            # Give all collected items at once (if auto-pickup is enabled)
            if self.auto_pickup_enabled and items_to_give:
                overflow_count = self.give_items_to_player(player, items_to_give)
                if overflow_count > 0:
                    self.send_inventory_full_message(player, overflow_count)
            
            if self.debug_logging and successful_breaks < len(vein_list):
                self.logger.warning(f"Only {successful_breaks}/{len(vein_list)} blocks broken successfully")
        except Exception as e:
            if self.debug_logging:
                self.logger.error(f"Error in vein processing: {str(e)}")
        
        if self.performance_logging:
            elapsed = (time.time() - start_time) * 1000
            self.logger.info(f"[Performance] Block breaking took {elapsed:.2f}ms for {vein_size} blocks")
        
        xp_gained = self.grant_vein_experience(player, total_xp, successful_breaks)
        
        # Apply tool durability damage
        self.apply_tool_durability(player, tool, successful_breaks)
        
        # Play completion effects
        if self.sounds_enabled:
            self.play_sound_effect(player, player.location, per_block=False)
        if self.particles_enabled:
            self.play_particle_effect(player, player.location)
        
        return {
            "successful_breaks": successful_breaks,
            "xp_gained": xp_gained,
        }
            
    def process_block_break(self, player, vein_block, tool) -> Dict[str, int]:
        """Process a single block break within vein mining (with gamerule management)"""
        # Suppress command feedback unless debug mode is enabled
        if not self.debug_logging:
            self.server.dispatch_command(self.server.command_sender, "gamerule commandBlockOutput false")
        
        try:
            result = self.process_block_break_internal(player, vein_block, tool)
        finally:
            # Re-enable command block output if it was disabled
            if not self.debug_logging:
                self.server.dispatch_command(self.server.command_sender, "gamerule commandBlockOutput true")
        
        return result
    
    def process_block_break_internal(self, player, vein_block, tool, items_to_give: dict = None) -> Dict[str, int]:
        """Internal method to break a single block (without gamerule management)"""
        success = False
        xp = 0.0
        
        try:
            block_type = vein_block.type
            drop_items, xp = self.calculate_block_rewards(block_type, tool)
            
            # Remove block without command spam.
            try:
                vein_block.set_type("minecraft:air", apply_physics=False)
            except Exception:
                command = f"setblock {vein_block.x} {vein_block.y} {vein_block.z} air"
                self.server.dispatch_command(self.server.command_sender, command)

            for drop_item, drop_count in drop_items.items():
                if not drop_item or drop_count <= 0:
                    continue
                if self.auto_pickup_enabled and items_to_give is not None:
                    # Collect items for batched inventory insert.
                    items_to_give[drop_item] = items_to_give.get(drop_item, 0) + drop_count
                else:
                    self.drop_item_stack(vein_block.location, drop_item, drop_count, player=player)
            
            success = True
            
        except Exception as e:
            if self.debug_logging:
                self.logger.error(f"Error breaking block at ({vein_block.x}, {vein_block.y}, {vein_block.z}): {str(e)}")
            
        return {
            "success": success,
            "xp": xp,
        }
    
    def normalize_block_id(self, block_id: str) -> str:
        """Normalize block identifiers to lowercase ids without namespace."""
        if not block_id:
            return ""
        return str(block_id).replace("minecraft:", "").lower()
    
    def get_item_type_id(self, item_type, fallback: str = "minecraft:air") -> str:
        """Best-effort conversion of ItemType/ItemStack/object to namespaced item id."""
        try:
            if hasattr(item_type, "type"):
                item_type = item_type.type
            if hasattr(item_type, "id"):
                item_id = str(item_type.id).lower()
            else:
                item_id = str(item_type).lower()
            if not item_id:
                return fallback
            if not item_id.startswith("minecraft:"):
                item_id = f"minecraft:{item_id}"
            return item_id
        except Exception:
            return fallback
    
    def get_enchantment_level(self, tool, enchantment: str) -> int:
        """Get enchantment level from an item tool, supporting namespaced and short ids."""
        if not tool or not hasattr(tool, "item_meta"):
            return 0
        
        try:
            meta = tool.item_meta
        except Exception:
            return 0
        
        normalized = enchantment.lower().replace(" ", "_")
        aliases = [normalized]
        if ":" in normalized:
            aliases.append(normalized.split(":", 1)[1])
        else:
            aliases.append(f"minecraft:{normalized}")
        
        # First check enchant map copy (fast path).
        try:
            enchant_map = {str(k).lower(): int(v) for k, v in meta.enchants.items()}
            for key in aliases:
                if key in enchant_map and enchant_map[key] > 0:
                    return enchant_map[key]
        except Exception:
            pass
        
        # Fallback to direct lookups.
        for key in aliases:
            try:
                level = int(meta.get_enchant_level(key))
                if level > 0:
                    return level
            except Exception:
                continue
        
        return 0
    
    def get_tool_traits(self, tool) -> Set[str]:
        """Classify a tool into gameplay traits used by tool validation."""
        if not tool:
            return set()
        
        tool_id = self.get_item_type_id(tool)
        short_id = self.normalize_block_id(tool_id)
        
        traits: Set[str] = set()
        if short_id.endswith("_pickaxe"):
            traits.add("pickaxe")
        if short_id.endswith("_axe"):
            traits.add("axe")
        if short_id.endswith("_shovel"):
            traits.add("shovel")
        if short_id.endswith("_hoe"):
            traits.add("hoe")
        if short_id.endswith("_sword"):
            traits.add("sword")
        if short_id == "shears":
            traits.add("shears")
        
        return traits
    
    def required_tool_traits_for_block(self, block_id: str) -> Optional[Set[str]]:
        """Return the accepted tool categories for this block type."""
        short_id = self.normalize_block_id(block_id)
        
        if short_id.endswith("_log") or short_id.endswith("_stem"):
            return {"axe"}
        if short_id.endswith("_leaves"):
            # Vanilla fast tools for leaves.
            return {"shears", "hoe", "axe"}
        if short_id.endswith("_ore") or short_id in {
            "ancient_debris",
            "amethyst_cluster",
            "large_amethyst_bud",
            "medium_amethyst_bud",
            "small_amethyst_bud",
        }:
            return {"pickaxe"}
        
        return None
    
    def is_proper_tool(self, block_id: str, tool) -> bool:
        """Check if the tool is appropriate for mining this block."""
        tool_key = self.get_item_type_id(tool) if tool else "hand"
        cache_key = f"{block_id}:{tool_key}"
        
        if cache_key in self.tool_validation_cache:
            return self.tool_validation_cache[cache_key]
        
        required_traits = self.required_tool_traits_for_block(block_id)
        if required_traits is None:
            is_valid = True
        else:
            tool_traits = self.get_tool_traits(tool)
            is_valid = len(required_traits.intersection(tool_traits)) > 0
        
        self.tool_validation_cache[cache_key] = is_valid
        return is_valid
    
    def build_neighbor_offsets(self) -> List[Tuple[int, int, int]]:
        """Build BFS neighbor offsets based on configured mining pattern."""
        offsets: List[Tuple[int, int, int]] = []
        
        if self.mining_pattern == "vertical":
            for dy in range(-self.vertical_range, self.vertical_range + 1):
                if dy != 0:
                    offsets.append((0, dy, 0))
        
        elif self.mining_pattern == "horizontal":
            for dx in range(-self.horizontal_range, self.horizontal_range + 1):
                for dz in range(-self.horizontal_range, self.horizontal_range + 1):
                    if dx == 0 and dz == 0:
                        continue
                    if not self.include_diagonals and abs(dx) + abs(dz) != 1:
                        continue
                    offsets.append((dx, 0, dz))
        
        elif self.mining_pattern == "cube":
            radius = self.pattern_radius
            for dx in range(-radius, radius + 1):
                for dy in range(-radius, radius + 1):
                    for dz in range(-radius, radius + 1):
                        if dx == 0 and dy == 0 and dz == 0:
                            continue
                        offsets.append((dx, dy, dz))
        
        elif self.mining_pattern == "sphere":
            radius = self.pattern_radius
            radius_sq = radius * radius
            for dx in range(-radius, radius + 1):
                for dy in range(-radius, radius + 1):
                    for dz in range(-radius, radius + 1):
                        if dx == 0 and dy == 0 and dz == 0:
                            continue
                        if (dx * dx + dy * dy + dz * dz) <= radius_sq:
                            offsets.append((dx, dy, dz))
        
        else:
            # Adjacent (default)
            for dx in range(-self.NEIGHBOR_RANGE, self.NEIGHBOR_RANGE + 1):
                for dy in range(-self.NEIGHBOR_RANGE, self.NEIGHBOR_RANGE + 1):
                    for dz in range(-self.NEIGHBOR_RANGE, self.NEIGHBOR_RANGE + 1):
                        if dx == 0 and dy == 0 and dz == 0:
                            continue
                        if not self.include_diagonals and abs(dx) + abs(dy) + abs(dz) != 1:
                            continue
                        offsets.append((dx, dy, dz))
        
        # Hard safety cap so extreme configs don't freeze the server.
        if len(offsets) > 512:
            offsets = offsets[:512]
        
        return offsets
    
    def should_auto_smelt(self, block_id: str, silk_touch_level: int, fortune_level: int) -> bool:
        """Determine whether auto-smelt should apply for a mined block."""
        if not self.auto_smelt_enabled:
            return False
        if silk_touch_level > 0:
            return False
        if self.auto_smelt_require_fortune and fortune_level <= 0:
            return False
        
        short_id = self.normalize_block_id(block_id)
        if short_id not in self.get_auto_smelt_outputs():
            return False
        if self.auto_smelt_whitelist and short_id not in self.auto_smelt_whitelist:
            return False
        
        return True
    
    def get_auto_smelt_outputs(self) -> Dict[str, str]:
        """Map raw block ids to auto-smelted item ids."""
        return {
            "iron_ore": "minecraft:iron_ingot",
            "deepslate_iron_ore": "minecraft:iron_ingot",
            "gold_ore": "minecraft:gold_ingot",
            "deepslate_gold_ore": "minecraft:gold_ingot",
            "nether_gold_ore": "minecraft:gold_ingot",
            "copper_ore": "minecraft:copper_ingot",
            "deepslate_copper_ore": "minecraft:copper_ingot",
            "ancient_debris": "minecraft:netherite_scrap",
        }
    
    def get_smelt_xp_value(self, block_id: str) -> float:
        """Return furnace-like XP for smeltable blocks."""
        short_id = self.normalize_block_id(block_id)
        smelt_xp_values = {
            "iron_ore": 0.7,
            "deepslate_iron_ore": 0.7,
            "gold_ore": 1.0,
            "deepslate_gold_ore": 1.0,
            "nether_gold_ore": 1.0,
            "copper_ore": 0.7,
            "deepslate_copper_ore": 0.7,
            "ancient_debris": 2.0,
        }
        return smelt_xp_values.get(short_id, 0.0)
    
    def get_fortune_drop_amount(self, block_id: str, fortune_level: int) -> int:
        """Compute item drop count using a vanilla-like fortune approximation."""
        short_id = self.normalize_block_id(block_id)
        
        # Base drops for notable ores.
        if short_id in {"lapis_ore", "deepslate_lapis_ore"}:
            base_count = random.randint(4, 9)
        elif short_id in {"redstone_ore", "deepslate_redstone_ore", "lit_redstone_ore", "lit_deepslate_redstone_ore"}:
            base_count = random.randint(4, 5)
        elif short_id == "nether_gold_ore":
            base_count = random.randint(2, 6)
        else:
            base_count = 1
        
        fortune_applicable = short_id in {
            "coal_ore", "deepslate_coal_ore",
            "iron_ore", "deepslate_iron_ore",
            "copper_ore", "deepslate_copper_ore",
            "gold_ore", "deepslate_gold_ore",
            "redstone_ore", "deepslate_redstone_ore", "lit_redstone_ore", "lit_deepslate_redstone_ore",
            "lapis_ore", "deepslate_lapis_ore",
            "diamond_ore", "deepslate_diamond_ore",
            "emerald_ore", "deepslate_emerald_ore",
            "quartz_ore", "nether_quartz_ore",
            "nether_gold_ore",
        }
        
        if fortune_level <= 0 or not fortune_applicable:
            return max(1, base_count)
        
        bonus_roll = random.randint(-1, fortune_level)
        if bonus_roll < 0:
            bonus_roll = 0
        
        return max(1, base_count * (bonus_roll + 1))
    
    def get_leaf_sapling_drop(self, block_id: str) -> Tuple[Optional[str], float]:
        """Return sapling-like drop item and base chance for a leaf block."""
        short_id = self.normalize_block_id(block_id)
        sapling_drops = {
            "oak_leaves": ("minecraft:oak_sapling", 0.05),
            "spruce_leaves": ("minecraft:spruce_sapling", 0.05),
            "birch_leaves": ("minecraft:birch_sapling", 0.05),
            "jungle_leaves": ("minecraft:jungle_sapling", 0.025),
            "acacia_leaves": ("minecraft:acacia_sapling", 0.05),
            "dark_oak_leaves": ("minecraft:dark_oak_sapling", 0.025),
            "mangrove_leaves": ("minecraft:mangrove_propagule", 0.05),
            "cherry_leaves": ("minecraft:cherry_sapling", 0.05),
        }
        return sapling_drops.get(short_id, (None, 0.0))
    
    def calculate_leaf_rewards(self, block_id: str, tool) -> Dict[str, int]:
        """Calculate stick/sapling rewards for leaves using vanilla-style base odds."""
        short_id = self.normalize_block_id(block_id)
        silk_touch_level = self.get_enchantment_level(tool, "silk_touch")
        tool_traits = self.get_tool_traits(tool)
        
        # Shears or Silk Touch keeps the leaf block itself.
        if silk_touch_level > 0 or "shears" in tool_traits:
            return {f"minecraft:{short_id}": 1}
        
        rewards: Dict[str, int] = {}
        
        sapling_item, sapling_chance = self.get_leaf_sapling_drop(short_id)
        if sapling_item and random.random() < sapling_chance:
            rewards[sapling_item] = rewards.get(sapling_item, 0) + 1
        
        # Sticks: 2% chance, 1-2 sticks.
        if random.random() < 0.02:
            rewards["minecraft:stick"] = rewards.get("minecraft:stick", 0) + random.randint(1, 2)
        
        return rewards
    
    def calculate_block_rewards(self, block_type: str, tool) -> Tuple[Dict[str, int], float]:
        """Calculate dropped items and XP for a mined block."""
        short_id = self.normalize_block_id(block_type)

        if short_id.endswith("_leaves"):
            return self.calculate_leaf_rewards(short_id, tool), 0.0
        
        silk_touch_level = self.get_enchantment_level(tool, "silk_touch")
        fortune_level = self.get_enchantment_level(tool, "fortune")
        
        # Silk Touch keeps the block itself where possible.
        if silk_touch_level > 0:
            silk_touch_overrides = {
                "lit_redstone_ore": "redstone_ore",
                "lit_deepslate_redstone_ore": "deepslate_redstone_ore",
            }
            silk_block = silk_touch_overrides.get(short_id, short_id)
            drop_item = f"minecraft:{silk_block}"
            return {drop_item: 1}, 0.0
        
        auto_smelt_applied = self.should_auto_smelt(short_id, silk_touch_level, fortune_level)
        
        if auto_smelt_applied:
            drop_item = self.get_auto_smelt_outputs().get(short_id, self.get_ore_drop(short_id))
        else:
            drop_item = self.get_ore_drop(short_id)
        
        drop_count = self.get_fortune_drop_amount(short_id, fortune_level)
        
        xp = float(self.calculate_ore_xp(short_id))
        if auto_smelt_applied and self.auto_smelt_give_xp:
            xp += self.get_smelt_xp_value(short_id) * self.auto_smelt_xp_multiplier * drop_count
        
        return {drop_item: drop_count}, xp
    
    def drop_item_stack(self, location, item_id: str, amount: int, player=None) -> None:
        """Drop item stacks, using /give for player overflow on Bedrock."""
        if amount <= 0:
            return
        
        namespaced_item_id = item_id if str(item_id).startswith("minecraft:") else f"minecraft:{item_id}"
        x, y, z = int(location.x), int(location.y), int(location.z)
        
        remaining = amount
        while remaining > 0:
            stack_amount = min(64, remaining)
            dropped = False

            # Bedrock does not allow summoning item entities directly; giving to the
            # player safely spills extras when inventory is full.
            if player is not None:
                give_command = f'give "{player.name}" {namespaced_item_id} {stack_amount}'
                dropped = bool(self.server.dispatch_command(self.server.command_sender, give_command))

            # Keep legacy summon fallback for environments that support item summon NBT.
            if not dropped:
                summon_command = f'summon item {x} {y} {z} {{Item:{{id:"{namespaced_item_id}",Count:{stack_amount}b}}}}'
                dropped = bool(self.server.dispatch_command(self.server.command_sender, summon_command))

            if not dropped and self.debug_logging:
                self.logger.warning(
                    f"Failed to drop item stack {namespaced_item_id} x{stack_amount} at ({x}, {y}, {z})"
                )
            remaining -= stack_amount
    
    def give_items_to_player(self, player, items_to_give: Dict[str, int]) -> int:
        """Give collected items to inventory and handle overflow."""
        overflow_total = 0
        
        for item_id, count in items_to_give.items():
            remaining = count
            while remaining > 0:
                stack_amount = min(64, remaining)
                
                try:
                    stack = ItemStack(item_id, stack_amount)
                    overflow = player.inventory.add_item(stack)
                    if overflow:
                        for overflow_stack in overflow.values():
                            overflow_amount = int(getattr(overflow_stack, "amount", 0))
                            overflow_id = self.get_item_type_id(overflow_stack, fallback=item_id)
                            overflow_total += overflow_amount
                            if self.full_inventory_action == "drop":
                                self.drop_item_stack(player.location, overflow_id, overflow_amount, player=player)
                except Exception:
                    # Fallback to command-based give if ItemStack construction fails.
                    try:
                        give_command = f'give "{player.name}" {item_id} {stack_amount}'
                        self.server.dispatch_command(self.server.command_sender, give_command)
                    except Exception:
                        overflow_total += stack_amount
                        if self.full_inventory_action == "drop":
                            self.drop_item_stack(player.location, item_id, stack_amount, player=player)
                
                remaining -= stack_amount
        
        if self.full_inventory_action == "delete":
            return overflow_total
        
        return overflow_total
    
    def grant_vein_experience(self, player, base_xp: float, successful_breaks: int) -> int:
        """Apply configured XP logic and grant experience to the player."""
        if not self.xp_enabled or successful_breaks <= 0:
            return 0
        
        total_xp = float(base_xp)
        if total_xp <= 0:
            return 0
        
        if self.xp_bonus_enabled and self.xp_bonus_per_blocks > 0:
            bonus_factor = 1.0 + ((successful_breaks / self.xp_bonus_per_blocks) * self.xp_bonus_multiplier)
            if bonus_factor < 0:
                bonus_factor = 0
            total_xp *= bonus_factor
        
        xp_to_give = max(0, int(round(total_xp)))
        if xp_to_give > 0:
            try:
                player.give_exp(xp_to_give)
            except Exception as e:
                if self.debug_logging:
                    self.logger.warning(f"Failed to grant XP: {str(e)}")
                xp_to_give = 0
        
        return xp_to_give
    
    def apply_tool_durability(self, player, tool, successful_breaks: int) -> None:
        """Apply durability damage with optional unbreaking handling."""
        if not tool or successful_breaks <= 0 or self.durability_multiplier <= 0:
            return
        
        try:
            meta = tool.item_meta
        except Exception:
            return
        
        try:
            if getattr(meta, "is_unbreakable", False):
                return
        except Exception:
            pass
        
        max_durability = 0
        try:
            max_durability = int(tool.type.max_durability)
        except Exception:
            max_durability = 0
        
        if max_durability <= 0:
            return
        
        base_damage = successful_breaks * self.durability_multiplier
        damage_rolls = int(base_damage)
        if random.random() < (base_damage - damage_rolls):
            damage_rolls += 1
        
        if damage_rolls <= 0:
            return
        
        if self.respect_unbreaking:
            unbreaking_level = self.get_enchantment_level(tool, "unbreaking")
            if unbreaking_level > 0:
                applied = 0
                consume_chance = 1.0 / (unbreaking_level + 1)
                for _ in range(damage_rolls):
                    if random.random() <= consume_chance:
                        applied += 1
                damage_rolls = applied
        
        if damage_rolls <= 0:
            return
        
        current_damage = 0
        try:
            current_damage = int(meta.damage)
        except Exception:
            current_damage = 0
        
        new_damage = current_damage + damage_rolls
        
        if new_damage >= max_durability:
            if self.break_on_exceed:
                try:
                    player.inventory.item_in_main_hand = ItemStack("minecraft:air", 1)
                except Exception:
                    player.inventory.remove(tool)
                if self.logging_enabled and self.log_vein_mining:
                    self.logger.info(f"[VeinMine] {player.name}'s tool broke after mining {successful_breaks} blocks")
                return
            new_damage = max_durability - 1
        
        try:
            meta.damage = new_damage
            tool.set_item_meta(meta)
            player.inventory.item_in_main_hand = tool
        except Exception as e:
            if self.logging_enabled:
                self.logger.warning(f"Failed to apply tool durability: {str(e)}")
    
    def play_sound_effect(self, player, location, per_block: bool) -> None:
        """Play configured sound effects with safe fallbacks."""
        sound_name = self.block_sound if per_block else self.completion_sound
        if not sound_name:
            return
        try:
            player.play_sound(location, sound_name, self.sound_volume, self.sound_pitch)
        except Exception:
            if self.debug_logging:
                self.logger.warning(f"[Effects] Failed to play sound '{sound_name}'")
    
    def play_particle_effect(self, player, location) -> None:
        """Spawn configured particle effects with simple density control."""
        if not self.particle_type:
            return
        
        spawn_count = max(1, min(self.particle_count, 20))
        for _ in range(spawn_count):
            try:
                px = location.x + random.uniform(-self.particle_radius, self.particle_radius)
                py = location.y + random.uniform(0, self.particle_radius)
                pz = location.z + random.uniform(-self.particle_radius, self.particle_radius)
                player.spawn_particle(self.particle_type, px, py, pz)
            except Exception:
                if self.debug_logging:
                    self.logger.warning(f"[Effects] Failed to spawn particle '{self.particle_type}'")
                break
    
    def get_ore_drop(self, block_type: str) -> str:
        """Get the item that should drop from breaking this ore block"""
        block_id = self.normalize_block_id(block_type)
        
        # Map ore blocks to their drops
        ore_drops = {
            # Coal ores drop coal
            "coal_ore": "minecraft:coal",
            "deepslate_coal_ore": "minecraft:coal",
            
            # Iron ores drop raw iron
            "iron_ore": "minecraft:raw_iron",
            "deepslate_iron_ore": "minecraft:raw_iron",
            
            # Copper ores drop raw copper
            "copper_ore": "minecraft:raw_copper",
            "deepslate_copper_ore": "minecraft:raw_copper",
            
            # Gold ores drop raw gold
            "gold_ore": "minecraft:raw_gold",
            "deepslate_gold_ore": "minecraft:raw_gold",
            "nether_gold_ore": "minecraft:gold_nugget",
            
            # Redstone drops redstone dust
            "redstone_ore": "minecraft:redstone",
            "deepslate_redstone_ore": "minecraft:redstone",
            "lit_redstone_ore": "minecraft:redstone",
            "lit_deepslate_redstone_ore": "minecraft:redstone",
            
            # Lapis drops lapis lazuli
            "lapis_ore": "minecraft:lapis_lazuli",
            "deepslate_lapis_ore": "minecraft:lapis_lazuli",
            
            # Diamond drops diamond
            "diamond_ore": "minecraft:diamond",
            "deepslate_diamond_ore": "minecraft:diamond",
            
            # Emerald drops emerald
            "emerald_ore": "minecraft:emerald",
            "deepslate_emerald_ore": "minecraft:emerald",
            
            # Quartz drops quartz
            "quartz_ore": "minecraft:quartz",
            "nether_quartz_ore": "minecraft:quartz",
            
            # Ancient debris drops itself
            "ancient_debris": "minecraft:ancient_debris",
            
            # Logs drop themselves
            "oak_log": "minecraft:oak_log",
            "birch_log": "minecraft:birch_log",
            "spruce_log": "minecraft:spruce_log",
            "jungle_log": "minecraft:jungle_log",
            "acacia_log": "minecraft:acacia_log",
            "dark_oak_log": "minecraft:dark_oak_log",
            "crimson_stem": "minecraft:crimson_stem",
            "warped_stem": "minecraft:warped_stem",
            "mangrove_log": "minecraft:mangrove_log",
            "cherry_log": "minecraft:cherry_log",
            "oak_leaves": "minecraft:oak_leaves",
            "spruce_leaves": "minecraft:spruce_leaves",
            "birch_leaves": "minecraft:birch_leaves",
            "jungle_leaves": "minecraft:jungle_leaves",
            "acacia_leaves": "minecraft:acacia_leaves",
            "dark_oak_leaves": "minecraft:dark_oak_leaves",
            "mangrove_leaves": "minecraft:mangrove_leaves",
            "cherry_leaves": "minecraft:cherry_leaves",
        }
        
        return ore_drops.get(block_id, f"minecraft:{block_id}")
    
    def calculate_ore_xp(self, block_type: str) -> int:
        """Calculate experience for breaking an ore block"""
        block_id = self.normalize_block_id(block_type)
        
        # XP values based on Minecraft ore XP
        xp_values = {
            "coal_ore": 1,
            "deepslate_coal_ore": 1,
            "iron_ore": 1,
            "deepslate_iron_ore": 1,
            "copper_ore": 1,
            "deepslate_copper_ore": 1,
            "gold_ore": 1,
            "deepslate_gold_ore": 1,
            "redstone_ore": 2,
            "lit_redstone_ore": 2,
            "deepslate_redstone_ore": 2,
            "lit_deepslate_redstone_ore": 2,
            "lapis_ore": 3,
            "deepslate_lapis_ore": 3,
            "diamond_ore": 4,
            "deepslate_diamond_ore": 4,
            "emerald_ore": 5,
            "deepslate_emerald_ore": 5,
            "quartz_ore": 2,
            "nether_quartz_ore": 2,
            "nether_gold_ore": 1,
            "ancient_debris": 2,
        }
        
        return xp_values.get(block_id, 0)
        
    def find_vein(self, start_block, dimension) -> Set:
        """Find all connected blocks of the same type using optimized BFS"""
        if not start_block or not dimension:
            return set()
        
        # Validate start block
        try:
            block_type = start_block.type
            start_x, start_y, start_z = start_block.x, start_block.y, start_block.z
        except Exception as e:
            if self.debug_logging:
                self.logger.error(f"[Security] Invalid start block: {str(e)}")
            return set()
        
        vein = set()
        visited = set()  # Use tuples for faster hashing
        queue = deque([(start_x, start_y, start_z)])
        visited.add((start_x, start_y, start_z))
        
        # Cache neighbor offsets for reuse based on configured mining pattern.
        if self._neighbor_offsets is None:
            self._neighbor_offsets = self.build_neighbor_offsets()
        
        neighbors = self._neighbor_offsets
        max_distance_sq = self.max_reach_distance * self.max_reach_distance
        iterations = 0
        max_iterations = max(100, self.max_blocks * 10)  # Prevent infinite loops
        
        try:
            while queue and len(vein) < self.max_blocks:
                # Security: Prevent infinite loops
                iterations += 1
                if iterations > max_iterations:
                    if self.log_suspicious_activity:
                        self.logger.warning(f"[Security] BFS iteration limit reached (possible exploit attempt)")
                    break
                
                cx, cy, cz = queue.popleft()
                
                # Security: Check distance from start
                dx, dy, dz = cx - start_x, cy - start_y, cz - start_z
                dist_sq = dx*dx + dy*dy + dz*dz
                if dist_sq > max_distance_sq:
                    continue
                
                # Get block and validate type
                try:
                    current_block = dimension.get_block_at(cx, cy, cz)
                    if not current_block or current_block.type != block_type:
                        continue
                except (AttributeError, TypeError) as e:
                    if self.debug_logging:
                        self.logger.warning(f"[Security] Invalid block at ({cx}, {cy}, {cz}): {str(e)}")
                    continue
                except Exception:
                    continue
                
                vein.add(current_block)
                
                # Early termination if we reached max
                if len(vein) >= self.max_blocks:
                    break
                
                # Check neighbors
                for dx, dy, dz in neighbors:
                    nx, ny, nz = cx + dx, cy + dy, cz + dz
                    
                    # Skip if outside world height limits
                    if ny < self.MIN_WORLD_HEIGHT or ny > self.MAX_WORLD_HEIGHT:
                        continue
                    
                    pos = (nx, ny, nz)
                    
                    if pos not in visited:
                        visited.add(pos)
                        try:
                            neighbor = dimension.get_block_at(nx, ny, nz)
                            if neighbor.type == block_type:
                                queue.append(pos)
                        except Exception:
                            pass  # Skip invalid positions
        except Exception as e:
            if self.debug_logging:
                self.logger.error(f"Error in find_vein: {str(e)}")
            
        return vein
        
    def send_inventory_full_message(self, player, count: int) -> None:
        """Send inventory full message to player"""
        if self.inventory_full_message:
            action = "dropped" if self.full_inventory_action == "drop" else "deleted"
            message = self.inventory_full_message.replace("{count}", str(count)).replace("{action}", action)
            message = message.replace("&", "§")  # Convert color codes
            player.send_message(message)
            
        if self.logging_enabled and self.log_vein_mining:
            action = "dropped" if self.full_inventory_action == "drop" else "deleted"
            self.logger.info(ColorFormat.YELLOW + f"[VeinMine] Player {player.name} had full inventory: {count} items {action}")
            
    def check_for_updates(self) -> None:
        """Check for plugin updates from GitHub"""
        def check_update_task():
            try:
                import urllib.request
                import json
                
                current_version = self.version
                url = f"https://api.github.com/repos/{self.github_repo}/releases/latest"
                
                req = urllib.request.Request(url)
                req.add_header('User-Agent', 'VeinMiner-UpdateChecker')
                
                with urllib.request.urlopen(req, timeout=5) as response:
                    data = json.loads(response.read().decode())
                    latest_version = data.get('tag_name', '').replace('v', '').strip()
                    
                    if self.is_newer_version(current_version, latest_version):
                        def notify():
                            self.logger.warning("=" * 43)
                            self.logger.warning("A new update for VeinMiner is available!")
                            self.logger.warning(f"Current: {ColorFormat.RED}{current_version}{ColorFormat.YELLOW} | Latest: {ColorFormat.GREEN}{latest_version}")
                            self.logger.warning(f"Download: {ColorFormat.AQUA}https://github.com/{self.github_repo}/releases")
                            self.logger.warning("=" * 43)
                            
                            # Notify online ops
                            for player in self.server.online_players:
                                if player.is_op:
                                    player.send_message(ColorFormat.YELLOW + "[VeinMiner] " + ColorFormat.GOLD + "A new update is available!")
                                    player.send_message(ColorFormat.YELLOW + f"Current: {ColorFormat.RED}{current_version}{ColorFormat.YELLOW} | Latest: {ColorFormat.GREEN}{latest_version}")
                                    player.send_message(ColorFormat.GRAY + "Download: " + ColorFormat.AQUA + f"https://github.com/{self.github_repo}/releases")
                                    
                        self.server.scheduler.run_task(self, notify)
                    else:
                        if self.logging_enabled:
                            def log_info():
                                self.logger.info(ColorFormat.GREEN + "[Update] You are running the latest version!")
                            self.server.scheduler.run_task(self, log_info)
                            
            except Exception as e:
                if self.logging_enabled:
                    self.logger.warning(f"Failed to check for updates: {str(e)}")
                    
        # Run task with delay to not block plugin enable
        self.server.scheduler.run_task(self, check_update_task, delay=100)
        
    def is_newer_version(self, current: str, latest: str) -> bool:
        """Compare version strings"""
        try:
            current_parts = [int(p) for p in current.split('.')]
            latest_parts = [int(p) for p in latest.split('.')]
            
            max_length = max(len(current_parts), len(latest_parts))
            
            for i in range(max_length):
                current_part = current_parts[i] if i < len(current_parts) else 0
                latest_part = latest_parts[i] if i < len(latest_parts) else 0
                
                if latest_part > current_part:
                    return True
                elif latest_part < current_part:
                    return False
                    
            return False
        except Exception:
            return False
