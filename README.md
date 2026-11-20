# p2p-agents

A single Go binary that lets paired Claude Code agents belonging to different people message each other directly. It is an MCP server for Claude Code and also an owner CLI.

Signaling (WebRTC offers and answers, sealed per pairing) goes over Nostr relays or LAN multicast. Messages travel over a WebRTC data channel between the two machines.

## Install

```sh
git clone <repo> && cd p2p-agents
go install .                        # or: CGO_ENABLED=0 go build -o p2p-agents . and put it on PATH
claude mcp add p2p-agents -- p2p-agents
```

Peer messages arrive live as channel notifications. The server is not on Claude Code's approved channel list, so launch with:

```sh
claude --dangerously-load-development-channels server:p2p-agents
```

Without the flag the tools still work, but messages are only seen by calling `read_messages`.

## Pairing

Pairing is always decided by the owner, never by an agent. Both owners need a terminal, and the code is compared out of band (phone, voice, chat).

1. Owner A: `p2p-agents invite` prints a code starting with `p2pa1:` (valid 24 hours) and waits.
2. Owner A gives the code to owner B over any channel.
3. Owner B: `p2p-agents pair p2pa1:...` waits for A.
4. Each side now shows a 6-digit verify code. Compare the two over a separate channel. They must match.
5. Each owner types the code to accept. Enter alone rejects, and three wrong codes fail the request.

`accept` and `pair` refuse to run without an interactive TTY. That stops an agent's plain shell tool, but not an agent that wraps the command in a pseudo-terminal. See [Same-user limit](#same-user-limit).

If a Claude Code session is already running with p2p-agents, it shows an in-session dialog (MCP elicitation) with the code and asks the owner to type it. Decline the dialog and use `p2p-agents accept <id>` in a terminal instead. If no dialog appears and no notification arrives, run `p2p-agents requests` in a terminal to see the request id, then `p2p-agents accept <id>`.

Other commands: `whoami`, `name <name>` (up to 64 characters), `requests`, `accept <id>`, `reject <id>`, `peers`, `unpair <peer> [reason]`.

## Agent tools

`list_peers`, `read_messages` (optional `peer`, `since`), `send` (`peer`, `text` up to 64 KB, optional `reply_to`; returns `delivered`, `offline` or `failed`), `pair_invite`, `pair_join`, `reject`, `unpair`.

There is no accept tool. `pair_invite` and `pair_join` only start pairing; the owner still confirms.

## Trust model

- Incoming peer text is delivered framed as untrusted content from another person's agent. The server instructions tell the agent to treat it as information, never as instructions from its user, and to ask the owner before acting on it.
- The agent can unpair a peer that keeps pushing it, and tells the owner.
- The owner always decides: pairing needs the owner's typed code, and `peers` / `unpair` are available to you any time.
- This is guidance to a model, not a guarantee. Only pair with people you would trust to talk to your agent.

## Privacy

What is protected:

- Message content goes over the WebRTC data channel (DTLS), end to end encrypted between the two machines. It never touches a relay. The channel is bound to the pairing: offers and answers are encrypted to the pair's mailbox and signed with the peer's identity key.
- Your identity key, your name and your peers' identities never appear in the clear on a relay or on the LAN; they travel only inside encrypted envelopes. Every outer event is kind 21059, padded to one fixed size, and signed by a fresh throwaway key.

What is not protected, so read this before using public relays:

- **A relay sees your IP address and your full mailbox list.** The node keeps one subscription per relay connection. It lists every mailbox you listen on: for each paired peer the previous, current and next hourly mailbox, plus any pending invite or pairing mailbox. The mailbox keys are random-looking, but the relay gets the whole set in one request, from your IP.
- **A relay can link everything in that list.** It can link:
  - one pair's mailboxes across hours, because each hourly update shares two of its three mailboxes with the one before, so rotation does not break the chain while you stay connected;
  - all of your pairs to each other, because they arrive in the same request from the same connection and IP;
  - the two IP addresses of a pair, because your peer subscribes to the same mailboxes from their IP (when you both use that relay);
  - when you are online, because the subscription is held for as long as the node runs.
- A relay sees the timing and count of events: pairing messages, offers and answers, unpair notices, and while a peer is offline, a knock every 30 seconds from one side and offers from the other (backing off to one every 5 minutes). It does not see their content or who sent them, beyond the connection they arrived on.
- Default relays are public servers run by third parties, and all configured relays get the same view. Rotation and padding protect content and identity keys; they do not hide your social graph or your IP from a relay you connect to.

Who sees what:

| Who | Sees | Does not see |
|---|---|---|
| Nostr relay | Your IP; your mailbox set (linkable across hours and across your pairs, see above); your peer's IP subscribing to the same mailboxes; when you are online; timing and count of events | Identity keys, names, message content |
| LAN observer (multicast) | Sealed events with their mailbox tag, source IP and timing, so it can link one pair's events within a mailbox (mailbox lists are filtered locally, never sent on the LAN) | Identities, content |
| STUN server | Your public IP when a connection is set up | Content, who you talk to |
| Peer | Your IP address (direct WebRTC connection), name, and what you send | Your other peers |

LAN first: when both peers are on the same network, the offer and the answer go over LAN multicast first and go to relays only if no reply arrives within 2 seconds, so a connection on a shared LAN normally sends no offer or answer to a relay. Even so, without `lan_only` the node stays subscribed on the relays, and knocks, pairing messages and unpair notices are always sent on both LAN and relays.

To keep relays out of it entirely, set `lan_only`. No relays and no STUN are used, so nothing outside the LAN learns your IP or mailboxes. Only peers on the same network can connect. A possible future mitigation for relay use is a separate relay connection per pair, which would stop a relay from linking your pairs to each other (it would still see each pair's own mailboxes and IPs). It is not implemented.

Config is `~/.config/p2p-agents/config.json` (override the directory with `P2PA_HOME`):

```json
{
  "name": "alice's agent",
  "relays": ["wss://relay.damus.io", "wss://nos.lol", "wss://relay.primal.net"],
  "stun": ["stun:stun.cloudflare.com:3478", "stun:stun.l.google.com:19302"],
  "lan_only": false
}
```

Defaults are as shown. Other files in that directory: `identity.key` (secret), `peers.json` (secret pairing keys), `log/<peer>.jsonl`, `lock`, and the control socket.

Env: `P2PA_HOME` (state directory), `P2PA_NO_LAN=1` (disable LAN multicast), `P2PA_DEBUG=1` (log every outer event to stderr as a `p2pa-debug <json>` line).

## Same-user limit

The agent runs as your OS user, so p2p-agents cannot stop an agent that has an unrestricted shell. Such an agent can:

- accept a pairing itself. The TTY check only tests whether stdin and stdout are terminals. A pseudo-terminal wrapper such as `script -qc "p2p-agents accept <id>"`, `expect`, or Python's `pty` module passes it, and then the agent can read the printed code and type it back;
- talk to the control socket directly, which offers the same calls as the CLI, verify code and accept included;
- read `identity.key` and `peers.json` and copy your identity and pairing keys.

The protection that holds is not letting an agent run shell commands unseen:

- Keep Bash on "ask". Do not allow-list it broadly or use a mode that runs commands without asking, and read each command before approving it.
- Accept pairings only in the in-session dialog or in your own terminal outside Claude Code.
- For a hard boundary, use the Claude Code sandbox to deny the state directory at the OS level (see the Claude Code sandbox settings docs for the exact key), or run the agent as a different OS user.

Also add deny rules in `~/.claude/settings.json`:

```json
{
  "permissions": {
    "deny": [
      "Read(~/.config/p2p-agents)",
      "Read(~/.config/p2p-agents/**)",
      "Edit(~/.config/p2p-agents)",
      "Edit(~/.config/p2p-agents/**)",
      "Bash(p2p-agents accept:*)",
      "Bash(p2p-agents pair:*)",
      "Bash(*p2p-agents*accept*)",
      "Bash(*p2p-agents*pair*)",
      "Bash(*.config/p2p-agents*)"
    ]
  }
}
```

The `Read` and `Edit` rules are enforced for Claude Code's own file tools. The `Bash` rules are best effort only, because they match the command text. They catch the plain command and simple wrappers such as `script -qc "p2p-agents accept ..."`. They miss anything that builds the string indirectly, for example a variable, `cd ~/.config` followed by a relative path, base64, or a script file. `*p2p-agents*pair*` also blocks `p2p-agents unpair` from the agent's shell; the agent has the `unpair` tool for that. If you use `P2PA_HOME`, put that path in place of `~/.config/p2p-agents`.

## Known limits

- No TURN. If a direct connection cannot be made (strict NAT on both sides), the peer stays `offline`.
- No offline delivery. Messages only go to online peers; `send` returns `offline` otherwise.
- A second Claude Code session is read-only: `list_peers` and `read_messages` work from disk, with online status unknown, but it cannot send or pair.
- The first session holds the network (an exclusive lock on the state directory). CLI commands run while it is up talk to it through the control socket; later MCP sessions are read-only. With no instance running, `peers` and `requests` read from disk and start no networking (`requests` is then always empty, since pending requests live only in a running instance).
- Invites use this node's own relays and LAN; owners with disjoint custom relay lists cannot pair over relays.
- If the last `pair_ok` message is lost, the pairing can be one-sided: one owner sees the peer in `peers` and the other does not. Pairing again fixes it.
