//! roll — a tiny chat-command module: dice.
//!
//! Registers `!roll` and replies in chat with the result. Supports:
//!   `!roll d20`  → one twenty-sided die
//!   `!roll 20`   → same (bare sides)
//!   `!roll 2d6`  → two six-sided dice, shown individually + total
//!   `!roll 5d20 3d3` → several groups, each with its own total + the sum
//!   `!coinflip`  → heads or tails
//!
//! Modeled on the other command modules: register the command on every fresh
//! session, ack each pipeline stage, answer liveness probes, and reconnect with
//! backoff. All pure logic lives here (parse + roll) so it unit-tests without a
//! socket.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    WsMessage,
>;

const ROLL_COMMAND: &str = "roll";
const COIN_COMMAND: &str = "coinflip";
const DEFAULT_FLAG: &str = "!";
const DEFAULT_DATA_DIR: &str = "~/.cockatiel/roll";
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;

/// The engine→module query that asks this module for the bans it holds, and the
/// module→engine report that answers it. These strings are the whole exchange
/// (over the existing `DatabaseQueryResult` / `DatabaseQuery` payloads), so no
/// proto change is needed.
const MODULE_BANS_QUERY: &str = "module_bans_query";
const MODULE_BANS_REPORT: &str = "module_bans_report";
/// The key used when a message has no guild (e.g. a DM).
const DM_DB: &str = "_dm";

/// Guardrails: at most this many dice per roll, and at most this many sides.
/// (The operator can raise the side cap in code if they truly want a d10^9.)
const MAX_DICE: u32 = 100;
const MAX_SIDES: u32 = 1_000_000_000;

/// At most this many dice GROUPS in one `!roll` (e.g. `5d20 3d3 4d4` is 3).
const MAX_GROUPS: usize = 8;

// ── config ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ModuleSettings {
    command_flag: String,
    /// The module's data dir; `bans.json` lives inside it.
    data_dir: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
}

/// Create any missing settings (with defaults) so every key is editable in place.
fn ensure_defaults() -> ModuleSettings {
    let root: Option<serde_json::Value> = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok());
    let ms = root
        .as_ref()
        .and_then(|r| r.get("module_specific"))
        .and_then(|ms| ms.as_object())
        .cloned()
        .unwrap_or_default();
    let command_flag = ms
        .get("command_flag")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_FLAG.to_string());
    let data_dir = ms
        .get("data_dir")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_DATA_DIR.to_string());
    let reconnect_base_secs = ms
        .get("reconnect_base_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms
        .get("reconnect_max_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_MAX_SECS);
    if let Some(mut root) = root {
        if let Some(obj) = root.as_object_mut() {
            if let Some(ms) = obj
                .entry("module_specific".to_string())
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                ms.entry("command_flag".to_string())
                    .or_insert_with(|| serde_json::json!(command_flag));
                ms.entry("data_dir".to_string())
                    .or_insert_with(|| serde_json::json!(data_dir));
                ms.entry("reconnect_base_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_base_secs));
                ms.entry("reconnect_max_secs".to_string())
                    .or_insert_with(|| serde_json::json!(reconnect_max_secs));
            }
            let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
        }
    }
    ModuleSettings {
        command_flag,
        data_dir,
        reconnect_base_secs,
        reconnect_max_secs,
    }
}

// ── pure logic ─────────────────────────────────────────────────────────────

/// Parse a dice spec: `d20`, `20`, or `2d6` → `(count, sides)`.
/// Only the *shape* is parsed here; range limits are enforced by [`roll`].
fn parse_roll(arg: &str) -> Option<(u32, u32)> {
    let a = arg.trim().to_ascii_lowercase();
    if a.is_empty() {
        return None;
    }
    let (count, sides) = match a.split_once('d') {
        Some((c, s)) => {
            let count = if c.trim().is_empty() {
                1
            } else {
                c.trim().parse::<u32>().ok()?
            };
            (count, s.trim().parse::<u32>().ok()?)
        }
        None => (1, a.parse::<u32>().ok()?),
    };
    if count == 0 || count > MAX_DICE || sides == 0 || sides > MAX_SIDES {
        return None;
    }
    Some((count, sides))
}

/// Parse one or more space-separated dice specs: `5d20`, `d20`, `5d20 3d3 4d4`.
/// Returns every group, or None if any token is malformed or there are too many.
fn parse_rolls(arg: &str) -> Option<Vec<(u32, u32)>> {
    let groups: Vec<(u32, u32)> = arg.split_whitespace().map(parse_roll).collect::<Option<_>>()?;
    if groups.is_empty() || groups.len() > MAX_GROUPS {
        return None;
    }
    Some(groups)
}

/// Roll `count` dice of `sides`, returning each face. Uses an inclusive range.
fn roll(count: u32, sides: u32) -> Vec<u32> {
    let mut rng = rand::thread_rng();
    (0..count).map(|_| rng.gen_range(1..=sides)).collect()
}

/// The chat reply for a `!roll` invocation, or `None` when the args are unusable.
fn roll_reply(handle: &str, arg: &str) -> Option<String> {
    let groups = parse_rolls(arg)?;
    let who = if handle.is_empty() { "you".to_string() } else { handle.to_string() };

    // A single group keeps the original compact line.
    if groups.len() == 1 {
        let (count, sides) = groups[0];
        let faces = roll(count, sides);
        return Some(if count == 1 {
            format!("🎲 {who} rolled d{sides}: {}", faces[0])
        } else {
            let sum: u32 = faces.iter().sum();
            let shown = faces.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(" + ");
            format!("🎲 {who} rolled {count}d{sides}: {shown} = {sum}")
        });
    }

    // Several groups: each group's faces + its own total, then the grand total.
    let mut spec = Vec::new();
    let mut parts = Vec::new();
    let mut total: u32 = 0;
    for &(count, sides) in &groups {
        let faces = roll(count, sides);
        let sum: u32 = faces.iter().sum();
        total += sum;
        spec.push(format!("{count}d{sides}"));
        if count == 1 {
            parts.push(format!("{count}d{sides} = {}", faces[0]));
        } else {
            let shown = faces.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(" + ");
            parts.push(format!("{count}d{sides}: {shown} = {sum}"));
        }
    }
    Some(format!(
        "🎲 {who} rolled {}:\n{}\n(total {total})",
        spec.join(" + "),
        parts.join("\n")
    ))
}

fn coin_reply(handle: &str) -> String {
    let who = if handle.is_empty() { "you".to_string() } else { handle.to_string() };
    let side = if rand::thread_rng().gen_bool(0.5) { "Heads" } else { "Tails" };
    format!("🪙 {who} flipped… {side}!")
}

const USAGE: &str = "usage: !roll [<n>]d<m> […] (e.g. !roll d20, !roll 5d20 3d3 4d4), or !coinflip";

/// Extract everything after `<flag><command>` verbatim.
fn strip_command(raw: &str, flag: &str, name: &str) -> String {
    let mut rest = raw.trim().to_string();
    if let Some(idx) = rest.find(flag) {
        rest = rest[idx + flag.len()..].trim_start().to_string();
    }
    if rest.starts_with(name) {
        rest = rest[name.len()..].trim_start().to_string();
    }
    rest
}

// ── bans.json ───────────────────────────────────────────────────────────────

/// Expand a leading `~` to `$HOME` (module config paths are written by hand).
fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    } else if path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return home;
        }
    }
    path.to_string()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The Discord message id, guild id, and first attachment URL from the
/// adapter's raw JSON (`raw_data`). Missing fields come back empty.
fn discord_meta(raw_data: &[u8]) -> (String, String, String) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw_data) else {
        return (String::new(), String::new(), String::new());
    };
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let attach = v
        .get("attachments")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|a| a.get("url"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string();
    (s("id"), s("guild_id"), attach)
}

/// The author's Discord id and username from the adapter's `raw_data` (best
/// effort — an absent `author` yields empty strings).
fn discord_author(raw_data: &[u8]) -> (String, String) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw_data) else {
        return (String::new(), String::new());
    };
    let author = v.get("author");
    let id = author
        .and_then(|a| a.get("id"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let username = author
        .and_then(|a| a.get("username"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    (id, username)
}

/// One ban record, as stored in `bans.json` and reported to the engine.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct BanEntry {
    /// The banned identity (normalized `ban` target, e.g. a handle or id).
    user: String,
    /// The key the entry is stored under (kept with the record for clarity).
    key: String,
    /// Who issued the ban (the actor's user-db uuid7/handle).
    by: String,
    reason: String,
    /// Epoch-milliseconds when the ban was recorded.
    at: i64,
}

/// The whole `bans.json`: guild id -> (ban key -> entry).
type BanMap = std::collections::HashMap<String, std::collections::HashMap<String, BanEntry>>;

/// Read the authoritative ban file. A missing/corrupt file is an empty record
/// (the file is created on the first ban), never an error on the hot path.
fn load_bans(path: &str) -> BanMap {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|d| serde_json::from_str::<BanMap>(&d).ok())
        .unwrap_or_default()
}

/// Persist the ban file atomically-ish (write then rename over the target), so
/// an interrupted write cannot leave a truncated `bans.json`.
fn save_bans(path: &str, map: &BanMap) -> Result<(), String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let json = serde_json::to_string_pretty(map).map_err(|e| e.to_string())?;
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, json).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())
}

/// Normalize a `ban <target>` argument. Discord mentions (`<@123>`, `<@!123>`,
/// `<@&123>`) become the bare id; a leading `@` is dropped.
fn normalize_ban_user(tok: &str) -> String {
    let t = tok.trim();
    if let Some(inner) = t.strip_prefix("<@") {
        let inner = inner.trim_end_matches('>');
        let inner = inner.trim_start_matches(['!', '&']);
        return inner.trim().to_string();
    }
    t.trim_start_matches('@').trim().to_string()
}

/// Every distinct banned identity across all guilds (the engine only needs a
/// flat set; guild scoping stays a module concern).
fn all_ban_ids(map: &BanMap) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for guild in map.values() {
        for entry in guild.values() {
            let id = entry.user.trim();
            if !id.is_empty() && seen.insert(id.to_string()) {
                out.push(id.to_string());
            }
        }
    }
    out
}

/// Whether any of `candidates` is banned in `guild`. A missing guild / empty
/// candidate is never a match.
fn is_banned_in_guild(map: &BanMap, guild: &str, candidates: &[&str]) -> bool {
    let Some(banned) = map.get(guild) else {
        return false;
    };
    candidates.iter().any(|c| {
        let c = c.trim();
        !c.is_empty() && banned.values().any(|b| b.user == c)
    })
}

/// The actor and mod-gate for a ban command. The actor is the sender's
/// `user_data` identity (canonical uuid7 when present, else username); the
/// command is gated to moderators/admins/owners.
fn ban_actor(chat: &ChatMessage) -> (bool, String) {
    let Some(ud) = chat.user_data.as_ref() else {
        return (false, chat.user_uuid7.clone());
    };
    let allowed = ud.is_moderator || ud.is_admin || ud.is_owner;
    let actor = if !ud.uuid.is_empty() {
        ud.uuid.clone()
    } else if !ud.username.is_empty() {
        ud.username.clone()
    } else {
        chat.user_uuid7.clone()
    };
    (allowed, actor)
}

/// If `args` begins with `word` as a whole word, return the remainder (trimmed).
fn subcommand<'a>(args: &'a str, word: &str) -> Option<&'a str> {
    let a = args.trim_start();
    let head = a.get(..word.len())?;
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    match a[word.len()..].chars().next() {
        None => Some(""),
        Some(c) if c.is_whitespace() => Some(a[word.len()..].trim_start()),
        _ => None,
    }
}

/// Parse `ban <user> [reason]`: the first token is the target, the rest is the
/// free-text reason.
fn parse_ban_args(rest: &str) -> Option<(String, String)> {
    let rest = rest.trim();
    let (user, reason) = match rest.split_once(char::is_whitespace) {
        Some((u, r)) => (u, r.trim()),
        None => (rest, ""),
    };
    if user.is_empty() {
        return None;
    }
    Some((user.to_string(), reason.to_string()))
}

/// The cached ban record for this module. The file is re-read whenever its
/// mtime changes, so a hand edit is honoured without a restart.
struct BanStore {
    path: String,
    cache: Option<BanMap>,
    mtime: Option<std::time::SystemTime>,
}

impl BanStore {
    fn new(data_dir: &str) -> Self {
        Self {
            path: format!("{}/bans.json", expand_tilde(data_dir).trim_end_matches('/')),
            cache: None,
            mtime: None,
        }
    }

    fn path(&self) -> &str {
        &self.path
    }

    /// Force the next `bans()` call to re-read the file.
    fn invalidate(&mut self) {
        self.cache = None;
        self.mtime = None;
    }

    fn bans(&mut self) -> &BanMap {
        let mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        if self.cache.is_none() || mtime != self.mtime {
            self.cache = Some(load_bans(&self.path));
            self.mtime = mtime;
        }
        self.cache.as_ref().unwrap()
    }
}

// ── connection / message loop ──────────────────────────────────────────────

#[derive(Clone)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

async fn send_container(write_shared: &Arc<AsyncMutex<WsWriteHalf>>, container: ContainerForEngine) {
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_shared.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

async fn register_commands(
    write_shared: &Arc<AsyncMutex<WsWriteHalf>>,
    identity: &Arc<AsyncMutex<EngineIdentity>>,
    command_flag: &str,
) {
    let id = identity.lock().await.clone();
    let commands = ContainerForEngine {
        version: 2,
        auth_token: id.auth,
        module_name: id.module,
        module_instance_uuid7: id.instance,
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![
                Command {
                    command_name: ROLL_COMMAND.to_string(),
                    command_flag: command_flag.to_string(),
                    command_description: "roll dice — !roll [<n>]d<m> (e.g. !roll 5d20)".to_string(),
                    command_flags: vec![],
                },
                Command {
                    command_name: COIN_COMMAND.to_string(),
                    command_flag: command_flag.to_string(),
                    command_description: "flip a coin".to_string(),
                    command_flags: vec![],
                },
            ],
            alert_on_unknown_command: false,
        })),
    };
    send_container(write_shared, commands).await;
    info!("registered !{} + !{} commands", ROLL_COMMAND, COIN_COMMAND);
}

/// Build a base SendToPlatforms (actor = the Discord author).
fn base_send(id: &EngineIdentity, chat: &ChatMessage, msg: String) -> SendToPlatforms {
    let handle = chat
        .user_data
        .as_ref()
        .map(|u| u.username.clone())
        .unwrap_or_default();
    SendToPlatforms {
        msg,
        level: 0,
        module_uuid7: id.instance.clone(),
        pid: String::new(),
        platform: chat.platform.clone(),
        actor_platform: chat.platform.clone(),
        actor_handle: handle,
        actor_uuid7: chat.user_uuid7.clone(),
        channel_id: chat.channel_id.clone(),
    }
}

/// A plain chat reply (command feedback) as an engine container.
fn reply_container(id: &EngineIdentity, chat: &ChatMessage, msg: String) -> ContainerForEngine {
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(base_send(id, chat, msg))),
    }
}

/// Build the module→engine ban report from an already-loaded ban map.
fn bans_report_container(id: &EngineIdentity, map: &BanMap) -> ContainerForEngine {
    let json = serde_json::json!({ "bans": all_ban_ids(map) }).to_string();
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
            query_id: MODULE_BANS_REPORT.to_string(),
            sql: json,
            params: Vec::new(),
        })),
    }
}

/// Handle a `ban <user> [reason]` subcommand, returning the replies + report.
fn do_ban(
    id: &EngineIdentity,
    chat: &ChatMessage,
    bans: &mut BanStore,
    guild: &str,
    rest: &str,
    cmd: &str,
) -> Vec<ContainerForEngine> {
    let (allowed, actor) = ban_actor(chat);
    if !allowed {
        return vec![reply_container(
            id,
            chat,
            format!("Only moderators can manage {cmd} bans."),
        )];
    }
    let Some((user, reason)) = parse_ban_args(rest) else {
        return vec![reply_container(
            id,
            chat,
            format!("Usage: `!{cmd} ban <user> [reason]`."),
        )];
    };
    let target = normalize_ban_user(&user);
    if target.is_empty() {
        return vec![reply_container(
            id,
            chat,
            format!("Usage: `!{cmd} ban <user> [reason]`."),
        )];
    }
    let mut map = load_bans(bans.path());
    let entry = BanEntry {
        user: target.clone(),
        key: target.clone(),
        by: actor,
        reason: reason.clone(),
        at: now_ms(),
    };
    map.entry(guild.to_string())
        .or_default()
        .insert(target.clone(), entry);
    match save_bans(bans.path(), &map) {
        Ok(()) => {
            // Invalidate the enforcement cache so the new file is read on the
            // next message even if the mtime resolution hides it.
            bans.invalidate();
            let msg = if reason.is_empty() {
                format!("Banned `{target}` from {cmd}.")
            } else {
                format!("Banned `{target}` from {cmd}: {reason}")
            };
            vec![reply_container(id, chat, msg), bans_report_container(id, &map)]
        }
        Err(e) => vec![reply_container(
            id,
            chat,
            format!("Could not save the ban: {e}"),
        )],
    }
}

/// Handle an `unban <user>` subcommand.
fn do_unban(
    id: &EngineIdentity,
    chat: &ChatMessage,
    bans: &mut BanStore,
    guild: &str,
    rest: &str,
    cmd: &str,
) -> Vec<ContainerForEngine> {
    let (allowed, _actor) = ban_actor(chat);
    if !allowed {
        return vec![reply_container(
            id,
            chat,
            format!("Only moderators can manage {cmd} bans."),
        )];
    }
    let target = normalize_ban_user(rest.split_whitespace().next().unwrap_or(""));
    if target.is_empty() {
        return vec![reply_container(
            id,
            chat,
            format!("Usage: `!{cmd} unban <user>`."),
        )];
    }
    let mut map = load_bans(bans.path());
    let removed = map
        .get_mut(guild)
        .and_then(|g| g.remove(&target))
        .is_some();
    if removed && map.get(guild).map(|g| g.is_empty()).unwrap_or(false) {
        map.remove(guild);
    }
    match save_bans(bans.path(), &map) {
        Ok(()) => {
            bans.invalidate();
            let msg = if removed {
                format!("Unbanned `{target}` from {cmd}.")
            } else {
                format!("`{target}` was not banned.")
            };
            let mut out = vec![reply_container(id, chat, msg)];
            if removed {
                out.push(bans_report_container(id, &map));
            }
            out
        }
        Err(e) => vec![reply_container(
            id,
            chat,
            format!("Could not save the ban list: {e}"),
        )],
    }
}

/// Handle one routed message: enforce local bans, then the command itself.
async fn handle_chat(
    id: &EngineIdentity,
    chat: &ChatMessage,
    bans: &mut BanStore,
) -> Vec<ContainerForEngine> {
    let (_, guild, _) = discord_meta(&chat.raw_data);
    let guild_key = if guild.is_empty() {
        DM_DB.to_string()
    } else {
        guild
    };
    let user = chat.user_uuid7.clone();
    let (author_id, author_name) = discord_author(&chat.raw_data);

    // Local enforcement: ignore messages from anyone banned in this guild.
    let locally_banned = {
        let map = bans.bans();
        is_banned_in_guild(
            map,
            &guild_key,
            &[user.as_str(), author_id.as_str(), author_name.as_str()],
        )
    };
    if locally_banned {
        return Vec::new();
    }

    let Some(cmd) = &chat.command else {
        return Vec::new();
    };
    let handle = chat
        .user_data
        .as_ref()
        .map(|u| u.username.clone())
        .unwrap_or_default();
    let args = strip_command(&chat.raw_message, &cmd.command_flag, &cmd.command_name);

    // `ban`/`unban` are subcommands of the module's own command.
    if cmd.command_name == ROLL_COMMAND {
        if let Some(rest) = subcommand(&args, "ban") {
            return do_ban(id, chat, bans, &guild_key, rest, ROLL_COMMAND);
        }
        if let Some(rest) = subcommand(&args, "unban") {
            return do_unban(id, chat, bans, &guild_key, rest, ROLL_COMMAND);
        }
        let reply = roll_reply(&handle, &args).unwrap_or_else(|| USAGE.to_string());
        return vec![reply_container(id, chat, reply)];
    }
    if cmd.command_name == COIN_COMMAND {
        return vec![reply_container(id, chat, coin_reply(&handle))];
    }
    Vec::new()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let module_name = client.config.module_name.clone();
    info!("roll module connected as '{}'", module_name);
    let identity: Arc<AsyncMutex<EngineIdentity>> = Arc::new(AsyncMutex::new(EngineIdentity {
        auth: client.auth_token.clone(),
        instance: client.instance_uuid7.clone(),
        module: module_name,
    }));

    let settings = ensure_defaults();
    let command_flag = settings.command_flag.clone();
    let data_dir = settings.data_dir.clone();
    let reconnect_base_secs = settings.reconnect_base_secs;
    let reconnect_max_secs = settings.reconnect_max_secs;
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);

    tokio::spawn(async move {
        let mut read = read;
        let mut bans = BanStore::new(&data_dir);
        loop {
            register_commands(&write_for_task, &identity_for_task, &command_flag).await;
            // Announce our current bans as soon as we (re)connect.
            {
                let id = identity_for_task.lock().await.clone();
                let report = {
                    let map = bans.bans();
                    bans_report_container(&id, map)
                };
                send_container(&write_for_task, report).await;
            }
            loop {
                let Some(msg) = read.next().await else { break };
                let data = match msg {
                    Ok(WsMessage::Binary(d)) => d,
                    Ok(WsMessage::Close(_)) => {
                        info!("Engine closed connection");
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("Engine WebSocket error: {}", e);
                        break;
                    }
                };
                let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                let id = identity_for_task.lock().await.clone();
                match container.payload {
                    Some(ModulePayload::AuthVerify(_)) => {
                        let reply = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                cur_auth: id.auth.clone(),
                            })),
                        };
                        send_container(&write_for_task, reply).await;
                    }
                    Some(ModulePayload::MessagePreProcess(pre)) => {
                        let MessagePreProcess {
                            message_uuid7: uuid,
                            raw_message,
                            audio,
                            audio_type,
                        } = pre;
                        if !uuid.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: uuid.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        // Handle our routed command(s).
                        if let Some(chat) = &raw_message {
                            for c in handle_chat(&id, chat, &mut bans).await {
                                send_container(&write_for_task, c).await;
                            }
                        }
                        // ALWAYS ack the pre-process stage so the pipeline advances.
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
                                message_uuid7: uuid,
                                raw_message,
                                audio,
                                audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::MessageInProcess(process)) => {
                        if !process.message_uuid7.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: process.message_uuid7.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
                                message_uuid7: process.message_uuid7,
                                raw_message: process.raw_message,
                                processed_message: process.processed_message,
                                abandon_message: process.abandon_message,
                                audio: process.audio,
                                audio_type: process.audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::DatabaseQueryResult(res))
                        if res.query_id == MODULE_BANS_QUERY =>
                    {
                        // The engine asked for our current bans. Answer with a
                        // `module_bans_report` (fresh read, so hand edits count).
                        let report = {
                            let map = bans.bans();
                            bans_report_container(&id, map)
                        };
                        send_container(&write_for_task, report).await;
                    }
                    Some(ModulePayload::Shutdown(_)) => {
                        info!("Engine requested shutdown — exiting");
                        std::process::exit(0);
                    }
                    _ => {}
                }
            }

            info!("Engine disconnected — reconnecting...");
            let mut backoff = reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
                match CockatielClient::connect("config.json").await {
                    Ok(conn) => {
                        info!("Reconnected to engine");
                        let (w, r) = conn.stream.split();
                        *write_for_task.lock().await = w;
                        *identity_for_task.lock().await = EngineIdentity {
                            auth: conn.auth_token,
                            instance: conn.instance_uuid7,
                            module: conn.config.module_name,
                        };
                        read = r;
                        break;
                    }
                    Err(e) => {
                        warn!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                        backoff = (backoff * 2).min(reconnect_max_secs);
                    }
                }
            }
        }
    });

    // Keep the process alive; the read loop owns reconnects.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_forms() {
        assert_eq!(parse_roll("d20"), Some((1, 20)));
        assert_eq!(parse_roll("20"), Some((1, 20)));
        assert_eq!(parse_roll("2d6"), Some((2, 6)));
        assert_eq!(parse_roll(" D100 "), Some((1, 100)));
    }

    #[test]
    fn parses_multiple_groups() {
        assert_eq!(parse_rolls("5d20 3d3 4d4"), Some(vec![(5, 20), (3, 3), (4, 4)]));
        assert_eq!(parse_rolls("d20"), Some(vec![(1, 20)]));
        assert_eq!(parse_rolls("  2d6   1d4 "), Some(vec![(2, 6), (1, 4)]));
        // A malformed token rejects the whole thing; an empty arg is None.
        assert_eq!(parse_rolls("5d20 nope"), None);
        assert_eq!(parse_rolls(""), None);
        // Too many groups is rejected.
        let many = std::iter::repeat_n("d6", MAX_GROUPS + 1).collect::<Vec<_>>().join(" ");
        assert_eq!(parse_rolls(&many), None);
    }

    #[test]
    fn multiple_groups_report_each_total_and_the_sum() {
        let reply = roll_reply("bob", "5d20 3d3 4d4").expect("parses");
        assert!(reply.contains("5d20") && reply.contains("3d3") && reply.contains("4d4"), "{reply}");
        assert!(reply.contains("total"), "{reply}");
        // A single group keeps the original compact form (no "total").
        let one = roll_reply("bob", "2d6").expect("parses");
        assert!(one.contains("2d6") && !one.contains("total"), "{one}");
    }

    #[test]
    fn rejects_bad_input() {
        for bad in ["", "d", "dd", "0", "d0", "abc", "-3", "d-3", "1d", "0d6"] {
            assert!(parse_roll(bad).is_none(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn rejects_absurd_counts_and_sides() {
        assert!(parse_roll("1000d6").is_none(), "too many dice");
        assert!(parse_roll("d2000000000").is_none(), "too many sides");
        assert_eq!(parse_roll(&format!("{}d6", MAX_DICE)), Some((MAX_DICE, 6)));
    }

    #[test]
    fn single_die_is_in_range() {
        for _ in 0..1000 {
            let faces = roll(1, 6);
            assert_eq!(faces.len(), 1);
            assert!((1..=6).contains(&faces[0]));
        }
    }

    #[test]
    fn many_dice_are_all_in_range() {
        let faces = roll(5, 20);
        assert_eq!(faces.len(), 5);
        assert!(faces.iter().all(|f| (1..=20).contains(f)));
    }

    #[test]
    fn reply_formats_single_and_multiple() {
        let r = roll_reply("bob", "d20").unwrap();
        assert!(r.starts_with("🎲 bob rolled d20: "), "{r}");
        let r = roll_reply("amy", "2d6").unwrap();
        assert!(r.starts_with("🎲 amy rolled 2d6: "), "{r}");
        assert!(r.contains('='), "a multi-roll shows the total: {r}");
    }

    #[test]
    fn reply_none_on_bad_args() {
        assert!(roll_reply("bob", "banana").is_none());
        assert!(roll_reply("bob", "").is_none());
    }

    #[test]
    fn coin_is_heads_or_tails() {
        for _ in 0..50 {
            let r = coin_reply("bob");
            assert!(r.contains("Heads") || r.contains("Tails"), "{r}");
        }
    }

    #[test]
    fn strip_command_keeps_args() {
        assert_eq!(strip_command("!roll d20", "!", "roll"), "d20");
        assert_eq!(strip_command("!roll", "!", "roll"), "");
    }

    // ── per-module bans ─────────────────────────────────────────────────────

    fn discord_msg(user: &str, channel: &str, guild: &str, msg_id: &str, text: &str) -> ChatMessage {
        let raw = format!(
            r#"{{"id":"{msg_id}","guild_id":"{guild}","channel_id":"{channel}","content":{}}}"#,
            serde_json::to_string(text).unwrap()
        );
        ChatMessage {
            platform: "discord".into(),
            raw_data: raw.into_bytes(),
            raw_message: text.into(),
            user_uuid7: user.into(),
            command: Some(Command {
                command_name: ROLL_COMMAND.into(),
                command_flag: "!".into(),
                command_description: String::new(),
                command_flags: vec![],
            }),
            user_data: Some(UserData {
                username: user.into(),
                ..Default::default()
            }),
            channel_id: channel.into(),
        }
    }

    fn mod_chat(user: &str, channel: &str, guild: &str, id: &str, text: &str) -> ChatMessage {
        let mut c = discord_msg(user, channel, guild, id, text);
        c.user_data = Some(UserData {
            uuid: "mod-uuid".into(),
            username: user.into(),
            is_moderator: true,
            ..Default::default()
        });
        c
    }

    fn test_identity() -> EngineIdentity {
        EngineIdentity {
            auth: "t".into(),
            instance: "inst".into(),
            module: "roll".into(),
        }
    }

    fn sends(v: &[ContainerForEngine]) -> Vec<SendToPlatforms> {
        v.iter()
            .filter_map(|c| match &c.payload {
                Some(EnginePayload::SendToPlatforms(s)) => Some(s.clone()),
                _ => None,
            })
            .collect()
    }

    fn reports(v: &[ContainerForEngine]) -> Vec<DatabaseQuery> {
        v.iter()
            .filter_map(|c| match &c.payload {
                Some(EnginePayload::DatabaseQuery(q)) if q.query_id == MODULE_BANS_REPORT => {
                    Some(q.clone())
                }
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn ban_and_unban_persist_report_and_enforce_locally() {
        let dir = std::env::temp_dir().join(format!("cockatiel-roll-{}", uuid_like()));
        let mut bans = BanStore::new(&dir.to_string_lossy());
        let id = test_identity();
        let (user, channel, guild) = ("mod1", "chan", "guild1");

        // A mod bans a user by handle.
        let out = handle_chat(
            &id,
            &mod_chat(user, channel, guild, "m1", "!roll ban @badguy spam"),
            &mut bans,
        )
        .await;
        assert!(sends(&out)[0].msg.contains("Banned `badguy`"));

        // A report to the engine rides along.
        let r = reports(&out);
        assert_eq!(r.len(), 1);
        let v: serde_json::Value = serde_json::from_str(&r[0].sql).unwrap();
        assert_eq!(v["bans"], serde_json::json!(["badguy"]));

        // Persisted with the actor and reason.
        let map = load_bans(bans.path());
        let entry = &map[guild]["badguy"];
        assert_eq!(entry.by, "mod-uuid");
        assert_eq!(entry.reason, "spam");
        assert_eq!(entry.user, "badguy");

        // The module ignores the banned user locally.
        assert!(
            handle_chat(
                &id,
                &discord_msg("badguy", channel, guild, "m2", "!roll d6"),
                &mut bans,
            )
            .await
            .is_empty(),
            "a banned user's message must be ignored"
        );

        // A non-mod cannot ban.
        let mut norm = discord_msg("someone", channel, guild, "m3", "!roll ban @other");
        norm.user_data = Some(UserData {
            uuid: "u-someone".into(),
            username: "someone".into(),
            ..Default::default()
        });
        let out = handle_chat(&id, &norm, &mut bans).await;
        assert!(sends(&out)[0].msg.contains("Only moderators"));

        // Unban clears the record and the local gate.
        let out = handle_chat(
            &id,
            &mod_chat(user, channel, guild, "m4", "!roll unban badguy"),
            &mut bans,
        )
        .await;
        assert!(sends(&out)[0].msg.contains("Unbanned `badguy`"));
        assert!(!load_bans(bans.path()).contains_key(guild));

        // The formerly banned user can interact again.
        let out = handle_chat(
            &id,
            &discord_msg("badguy", channel, guild, "m5", "!roll d6"),
            &mut bans,
        )
        .await;
        assert_eq!(sends(&out).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A tiny unique token for temp dirs (no uuid dependency in this crate).
    fn uuid_like() -> String {
        let n = now_ms();
        let p = std::process::id();
        format!("{n}-{p}-{}", rand::thread_rng().gen::<u32>())
    }
}
