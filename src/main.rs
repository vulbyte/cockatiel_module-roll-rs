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
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;

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
        "🎲 {who} rolled {}: {} (total {total})",
        spec.join(" + "),
        parts.join(" | ")
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

/// Post a chat message back to the platform the command came from.
async fn send_to_chat(
    write: &Arc<AsyncMutex<WsWriteHalf>>,
    id: &EngineIdentity,
    msg: String,
    chat: &ChatMessage,
) {
    let handle = chat
        .user_data
        .as_ref()
        .map(|u| u.username.clone())
        .unwrap_or_default();
    let container = ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(SendToPlatforms {
            msg,
            level: 0,
            module_uuid7: id.instance.clone(),
            pid: String::new(),
            platform: chat.platform.clone(),
            actor_platform: chat.platform.clone(),
            actor_handle: handle,
            actor_uuid7: chat.user_uuid7.clone(),
            channel_id: chat.channel_id.clone(),
        })),
    };
    send_container(write, container).await;
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
    let reconnect_base_secs = settings.reconnect_base_secs;
    let reconnect_max_secs = settings.reconnect_max_secs;
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);

    tokio::spawn(async move {
        let mut read = read;
        loop {
            register_commands(&write_for_task, &identity_for_task, &command_flag).await;
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
                            if let Some(cmd) = &chat.command {
                                let handle = chat
                                    .user_data
                                    .as_ref()
                                    .map(|u| u.username.clone())
                                    .unwrap_or_default();
                                if cmd.command_name == ROLL_COMMAND {
                                    let arg =
                                        strip_command(&chat.raw_message, &cmd.command_flag, &cmd.command_name);
                                    let reply = roll_reply(&handle, &arg).unwrap_or_else(|| USAGE.to_string());
                                    send_to_chat(&write_for_task, &id, reply, chat).await;
                                } else if cmd.command_name == COIN_COMMAND {
                                    send_to_chat(&write_for_task, &id, coin_reply(&handle), chat).await;
                                }
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
}
