# cockatiel_module-roll-rs

A tiny Cockatiel module: dice + coin, for fun.

## Commands
- `!roll d20` — one twenty-sided die
- `!roll 20` — same (bare sides)
- `!roll 2d6` — two six-sided dice, shown individually + total
- `!coinflip` — heads or tails

Reply example: `🎲 bob rolled 2d6: 3 + 5 = 8`.

## Config (`config.json` → `module_specific`)
- `command_flag` — the command prefix, default `!`
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff bounds

Limits (compile-time): at most 100 dice and 1,000,000,000 sides per roll.

## Build
```sh
cargo build --release
```
Then start it from the TUI (Modules window → select `roll` → `s`), or it autostarts.
