# VeinMiner Plugin for Endstone

A configurable vein-mining plugin for Endstone servers.

## Features

- Vein mining for ores, logs, and leaves.
- Activation modes: sneak, stand, or always.
- Tool validation by block category (pickaxe/axe/shears/hoe).
- Enchantment-aware drops:
  - Silk Touch keeps ore blocks.
  - Fortune increases item yields for supported ores.
  - Unbreaking can reduce durability damage.
- Auto-smelt support with optional fortune requirement and whitelist.
- XP rewards:
  - Per-block ore XP.
  - Optional vein-size bonus multiplier.
  - Optional extra smelting XP when auto-smelt is active.
- Auto-pickup with overflow handling (`drop` or `delete`).
- Anti-abuse and limits (per-minute and daily caps).
- Mining pattern controls (`adjacent`, `cube`, `sphere`, `vertical`, `horizontal`).
- Particle and sound effects (per-block or completion).
- Player statistics and milestone announcements.
- Update checker against GitHub releases.

## Installation

```bash
pip install endstone-vein-miner
```

Or from source:

```bash
git clone https://github.com/EuphoriaDevelopmentOrg/VeinMiner-Endstone.git
cd VeinMiner-Endstone
pip install -e .
```

Start your Endstone server. The plugin is auto-discovered through the `endstone` entry point.

## Commands

| Command | Description | Permission |
|---|---|---|
| `/veinminer` or `/vm` | Help | `veinminer.command` |
| `/vm reload` | Reload config | `veinminer.reload` |
| `/vm stats` | View personal stats | `veinminer.stats` |
| `/vm toggle` | Toggle vein miner | `veinminer.toggle` |
| `/vm on` | Enable vein miner | `veinminer.toggle` |
| `/vm off` | Disable vein miner | `veinminer.toggle` |
| `/vm status` | Show current status | `veinminer.toggle` |

Aliases: `/vm`, `/vmine`

## Permissions

- `veinminer.use` (default: true)
- `veinminer.command` (default: true)
- `veinminer.reload` (default: op)
- `veinminer.stats` (default: true)
- `veinminer.toggle` (default: true)
- `veinminer.*` (default: op)

## Configuration

The plugin writes `config.toml` to the plugin data folder.

### Main sections in `config.toml`

- Root keys:
  - `max-blocks`, `batch-size`, `min-vein-size`, `cooldown-ms`
- `[auto-pickup]`
  - `enabled`, `full-inventory-action`
- `[auto-smelt]`
  - `enabled`, `require-fortune`, `whitelist`, `give-xp`, `xp-multiplier`
- `[tool-durability]`
  - `multiplier`, `respect-unbreaking`, `break-on-exceed`
- `[experience]`
  - `enabled`, `bonus-enabled`, `bonus-per-blocks`, `multiplier`
- `[activation]`
  - `mode`, `per-block-permissions`, `require-correct-tool`, `max-reach-distance`
- `[mining-pattern]`
  - `pattern`, `radius`, `include-diagonals`, `vertical-range`, `horizontal-range`
- `[effects.particles]`
  - `enabled`, `type`, `count`, `radius`, `per-block`
- `[effects.sounds]`
  - `enabled`, `completion-sound`, `volume`, `pitch`, `per-block-sound`, `block-sound`
- `[limits]`
  - `enable-limits`, `max-veins-per-day`, `max-blocks-per-day`, `reset-time`
- `[anti-abuse]`
  - `max-veins-per-minute`, `temporary-block-duration`, `log-suspicious-activity`
- `[statistics]`
  - `enabled`, `storage`, `save-to-file`, `auto-save-interval`, `track-per-block-type`, `milestones`, `broadcast-milestones`
- `[statistics.mysql]`
  - `enabled`, `host`, `port`, `database`, `user`, `password`, `table-prefix`, `connect-timeout`
- `[update-checker]`, `[logging]`, `[enabled-blocks]`, `[messages]`, `[blocks]`

Notes:
- `max-blocks = -1` is accepted and internally capped for safety.
- `auto-smelt.whitelist` accepts block ids with or without `minecraft:` prefix.
- `statistics.auto-save-interval = 0` means save on each update; otherwise periodic saves are used.

## Behavior Notes

- Tool validation is category-based:
  - Ores/amethyst/ancient debris -> pickaxe
  - Logs/stems -> axe
  - Leaves -> shears, hoe, or axe
- If `activation.require-correct-tool = false`, tool checks are skipped.
- `auto-pickup.enabled = false` drops computed rewards at block location.
- XP is granted directly to the player (not orb entities).
- Fortune/Silk Touch are applied to plugin-managed drops.

## Statistics

Statistics support two backends:
- `storage = "yaml"`: persisted to `stats.yml` in the plugin data folder.
- `storage = "mysql"`: persisted to MySQL tables (`<table-prefix>player_stats`, `<table-prefix>player_milestones`).

MySQL can be enabled by either:
- `statistics.storage = "mysql"`, or
- `statistics.mysql.enabled = true` (legacy toggle compatibility).

If MySQL initialization fails, the plugin falls back to YAML when `statistics.save-to-file = true`.

Tracked fields per player:
- total veins mined
- total blocks mined through vein miner
- largest vein
- last mined timestamp
- achieved milestones

## Troubleshooting

- Vein mining not triggering:
  - Check `veinminer.use`
  - Verify activation mode in `[activation]`
  - Confirm world is not in `disabled-worlds`
  - Check `/vm status`
- Wrong drops or no auto-smelt:
  - Check `[auto-smelt]`
  - Check tool enchantments and `require-fortune`
  - Confirm block is in whitelist if whitelist is set
- No stats updates:
  - Check `[statistics].enabled`
  - If using YAML, ensure plugin data folder is writable
  - If using MySQL, verify `[statistics.mysql]` credentials/database and that `mysql-connector-python` is installed

## Development

```bash
pip install -e .
python -m build
```

Source files:
- `src/endstone_vein_miner/vein_miner_plugin.py`
- `src/endstone_vein_miner/vein_miner_command.py`
- `src/endstone_vein_miner/statistics_tracker.py`

## License

MIT
