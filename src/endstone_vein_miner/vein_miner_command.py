"""
Command handler for VeinMiner
Handles all player commands for the VeinMiner plugin
"""

from endstone.command import CommandSender
from endstone import ColorFormat
from typing import List


class VeinMinerCommand:
    """VeinMiner command handler (static helper)"""
    
    LOG_TAG = "[VeinMiner] "
    
    @staticmethod
    def handle_command(plugin, sender: CommandSender, args: List[str]) -> bool:
        """Execute command"""
        try:
            if not sender.has_permission("veinminer.command"):
                sender.send_message(ColorFormat.RED + "You don't have permission to use this command.")
                return True
                
            if len(args) == 0:
                VeinMinerCommand.send_detailed_help(sender)
                return True
                
            subcommand = args[0].lower()
            
            if subcommand in ["help", "?"]:
                VeinMinerCommand.send_detailed_help(sender)
                return True
                
            elif subcommand in ["reload", "rl"]:
                if not sender.has_permission("veinminer.reload"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to reload.")
                    return True
                    
                plugin.reload_configuration()
                if hasattr(plugin, "send_message"):
                    plugin.send_message(sender, "reload-success")
                else:
                    sender.send_message(ColorFormat.GREEN + "VeinMiner configuration reloaded!")
                return True
                
            elif subcommand in ["stats", "statistics"]:
                if not hasattr(sender, 'unique_id'):
                    sender.send_message(ColorFormat.RED + "This command can only be used by players.")
                    return True
                    
                if not sender.has_permission("veinminer.stats"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to view stats.")
                    return True
                    
                stats = plugin.stats_tracker.get_stats(sender)
                sender.send_message(ColorFormat.GOLD + "▬" * 34)
                sender.send_message(stats.get_formatted_stats())
                sender.send_message(ColorFormat.GOLD + "▬" * 34)
                return True
                
            elif subcommand in ["toggle", "t"]:
                if not hasattr(sender, 'unique_id'):
                    sender.send_message(ColorFormat.RED + "This command can only be used by players.")
                    return True
                    
                if not sender.has_permission("veinminer.toggle"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to toggle vein mining.")
                    return True
                    
                uuid = sender.unique_id
                
                if uuid in plugin.disabled_players:
                    plugin.disabled_players.remove(uuid)
                    if hasattr(plugin, "send_message"):
                        plugin.send_message(sender, "toggle-enabled")
                    else:
                        sender.send_message(ColorFormat.GREEN + "VeinMiner enabled! Sneak while mining to activate.")
                else:
                    plugin.disabled_players.add(uuid)
                    if hasattr(plugin, "send_message"):
                        plugin.send_message(sender, "toggle-disabled")
                    else:
                        sender.send_message(ColorFormat.RED + "VeinMiner disabled! You'll mine normally.")
                    
                return True
                
            elif subcommand in ["on", "enable"]:
                if not hasattr(sender, 'unique_id'):
                    sender.send_message(ColorFormat.RED + "This command can only be used by players.")
                    return True
                    
                if not sender.has_permission("veinminer.toggle"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to toggle vein mining.")
                    return True
                    
                uuid = sender.unique_id
                
                if uuid not in plugin.disabled_players:
                    sender.send_message(ColorFormat.YELLOW + "VeinMiner is already enabled!")
                else:
                    plugin.disabled_players.remove(uuid)
                    if hasattr(plugin, "send_message"):
                        plugin.send_message(sender, "toggle-enabled")
                    else:
                        sender.send_message(ColorFormat.GREEN + "VeinMiner enabled! Sneak while mining to activate.")
                    
                return True
                
            elif subcommand in ["off", "disable"]:
                if not hasattr(sender, 'unique_id'):
                    sender.send_message(ColorFormat.RED + "This command can only be used by players.")
                    return True
                    
                if not sender.has_permission("veinminer.toggle"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to toggle vein mining.")
                    return True
                    
                uuid = sender.unique_id
                
                if uuid in plugin.disabled_players:
                    sender.send_message(ColorFormat.YELLOW + "VeinMiner is already disabled!")
                else:
                    plugin.disabled_players.add(uuid)
                    if hasattr(plugin, "send_message"):
                        plugin.send_message(sender, "toggle-disabled")
                    else:
                        sender.send_message(ColorFormat.RED + "VeinMiner disabled! You'll mine normally.")
                    
                return True
                
            elif subcommand in ["chain"]:
                if not hasattr(sender, 'unique_id'):
                    sender.send_message(ColorFormat.RED + "This command can only be used by players.")
                    return True

                if not sender.has_permission("veinminer.chain"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to use chain mining.")
                    return True

                if not plugin.chain_mining_enabled:
                    sender.send_message(ColorFormat.RED + "Chain mining is disabled in the config.")
                    return True

                if len(args) < 2:
                    sender.send_message(ColorFormat.YELLOW + "Usage: /vm chain <toggle|on|off|status>")
                    return True

                chain_cmd = args[1].lower()
                uuid = sender.unique_id

                if chain_cmd in ["toggle", "t"]:
                    if uuid in plugin.chain_disabled_players:
                        plugin.chain_disabled_players.remove(uuid)
                        if hasattr(plugin, "send_message"):
                            plugin.send_message(sender, "chain-toggle-enabled")
                        else:
                            sender.send_message(ColorFormat.GREEN + "Chain Mining enabled!")
                    else:
                        plugin.chain_disabled_players.add(uuid)
                        if hasattr(plugin, "send_message"):
                            plugin.send_message(sender, "chain-toggle-disabled")
                        else:
                            sender.send_message(ColorFormat.RED + "Chain Mining disabled!")
                    return True

                if chain_cmd in ["on", "enable"]:
                    if uuid not in plugin.chain_disabled_players:
                        sender.send_message(ColorFormat.YELLOW + "Chain Mining is already enabled!")
                    else:
                        plugin.chain_disabled_players.remove(uuid)
                        if hasattr(plugin, "send_message"):
                            plugin.send_message(sender, "chain-toggle-enabled")
                        else:
                            sender.send_message(ColorFormat.GREEN + "Chain Mining enabled!")
                    return True

                if chain_cmd in ["off", "disable"]:
                    if uuid in plugin.chain_disabled_players:
                        sender.send_message(ColorFormat.YELLOW + "Chain Mining is already disabled!")
                    else:
                        plugin.chain_disabled_players.add(uuid)
                        if hasattr(plugin, "send_message"):
                            plugin.send_message(sender, "chain-toggle-disabled")
                        else:
                            sender.send_message(ColorFormat.RED + "Chain Mining disabled!")
                    return True

                if chain_cmd in ["status", "info"]:
                    is_enabled = uuid not in plugin.chain_disabled_players
                    sender.send_message(
                        ColorFormat.GOLD + "â–¬" * 8
                        + " " + ColorFormat.BOLD + "Chain Mining Status" + ColorFormat.RESET
                        + ColorFormat.GOLD + " " + "â–¬" * 8
                    )
                    sender.send_message(
                        ColorFormat.YELLOW + "Status: "
                        + (ColorFormat.GREEN + "âœ“ Enabled" if is_enabled else ColorFormat.RED + "âœ— Disabled")
                    )
                    sender.send_message(ColorFormat.YELLOW + "Mode: " + ColorFormat.WHITE + plugin.chain_activation_mode)
                    sender.send_message(ColorFormat.GOLD + "â–¬" * 34)
                    return True

                sender.send_message(ColorFormat.RED + "Unknown chain subcommand: " + ColorFormat.GRAY + chain_cmd)
                sender.send_message(ColorFormat.YELLOW + "Use " + ColorFormat.WHITE + "/vm chain toggle" + ColorFormat.YELLOW + " to toggle.")
                return True

            elif subcommand in ["status", "info"]:
                if not hasattr(sender, 'unique_id'):
                    sender.send_message(ColorFormat.RED + "This command can only be used by players.")
                    return True
                    
                if not sender.has_permission("veinminer.toggle"):
                    sender.send_message(ColorFormat.RED + "You don't have permission to check status.")
                    return True
                    
                uuid = sender.unique_id
                is_enabled = uuid not in plugin.disabled_players
                
                sender.send_message(ColorFormat.GOLD + "▬" * 8 + " " + ColorFormat.BOLD + "VeinMiner Status" + ColorFormat.RESET + ColorFormat.GOLD + " " + "▬" * 8)
                sender.send_message(ColorFormat.YELLOW + "Status: " + (ColorFormat.GREEN + "✓ Enabled" if is_enabled else ColorFormat.RED + "✗ Disabled"))
                sender.send_message(ColorFormat.YELLOW + "Version: " + ColorFormat.WHITE + plugin.version)
                
                if is_enabled:
                    sender.send_message(ColorFormat.GRAY + "Tip: Sneak while mining to activate!")
                else:
                    sender.send_message(ColorFormat.GRAY + "Use /vm on to enable vein mining.")
                    
                sender.send_message(ColorFormat.GOLD + "▬" * 34)
                return True
                
            else:
                sender.send_message(ColorFormat.RED + "Unknown subcommand: " + ColorFormat.GRAY + subcommand)
                sender.send_message(ColorFormat.YELLOW + "Use " + ColorFormat.WHITE + "/vm help" + ColorFormat.YELLOW + " for a list of commands.")
                return True
                
        except Exception as e:
            plugin.logger.error(f"{VeinMinerCommand.LOG_TAG}Error executing command: {str(e)}")
            import traceback
            traceback.print_exc()
            
            try:
                sender.send_message(ColorFormat.RED + f"An error occurred: {str(e)}")
            except Exception:
                plugin.logger.error(f"{VeinMinerCommand.LOG_TAG}Error sending error message")
                
            return True
    
    @staticmethod        
    def send_detailed_help(sender: CommandSender) -> None:
        """Send detailed help message"""
        sender.send_message(ColorFormat.GOLD + "▬" * 34)
        sender.send_message(ColorFormat.YELLOW + ColorFormat.BOLD + "VeinMiner Commands")
        sender.send_message("")
        sender.send_message(ColorFormat.AQUA + "/vm help" + ColorFormat.GRAY + " - Show this help menu")
        
        if sender.has_permission("veinminer.reload"):
            sender.send_message(ColorFormat.AQUA + "/vm reload" + ColorFormat.GRAY + " - Reload configuration")
            
        if sender.has_permission("veinminer.stats"):
            sender.send_message(ColorFormat.AQUA + "/vm stats" + ColorFormat.GRAY + " - View your mining stats")
            
        if sender.has_permission("veinminer.toggle"):
            sender.send_message(ColorFormat.AQUA + "/vm toggle" + ColorFormat.GRAY + " - Toggle vein mining on/off")
            sender.send_message(ColorFormat.AQUA + "/vm on" + ColorFormat.GRAY + " - Enable vein mining")
            sender.send_message(ColorFormat.AQUA + "/vm off" + ColorFormat.GRAY + " - Disable vein mining")
            sender.send_message(ColorFormat.AQUA + "/vm status" + ColorFormat.GRAY + " - Check your status")
        if sender.has_permission("veinminer.chain"):
            sender.send_message(ColorFormat.AQUA + "/vm chain toggle" + ColorFormat.GRAY + " - Toggle chain mining on/off")
            sender.send_message(ColorFormat.AQUA + "/vm chain on" + ColorFormat.GRAY + " - Enable chain mining")
            sender.send_message(ColorFormat.AQUA + "/vm chain off" + ColorFormat.GRAY + " - Disable chain mining")
            sender.send_message(ColorFormat.AQUA + "/vm chain status" + ColorFormat.GRAY + " - Check chain status")
            
        sender.send_message("")
        sender.send_message(ColorFormat.GRAY + "Tip: Sneak (Shift) while mining to activate!")
        sender.send_message(ColorFormat.GOLD + "▬" * 34)
