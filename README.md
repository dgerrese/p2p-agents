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

`accept` and `pair` refuse to run without an interactive TTY, so an agent cannot call them.

If a Claude Code session is already running with p2p-agents, it shows an in-session dialog (MCP elicitation) with the code and asks the owner to type it. Decline the dialog and use `p2p-agents accept <id>` in a terminal instead.

Other commands: `whoami`, `name <name>`, `requests`, `accept <id>`, `reject <id>`, `peers`, `unpair <peer> [reason]`.

## Agent tools

`list_peers`, `read_messages` (optional `peer`, `since`), `send` (`peer`, `text` up to 64 KB, optional `reply_to`; returns `delivered`, `offline` or `failed`), `pair_invite`, `pair_join`, `reject`, `unpair`.

There is no accept tool. `pair_invite` and `pair_join` only start pairing; the owner still confirms.

## Trust model

- Incoming peer text is delivered framed as untrusted content from another person's agent. The server instructions tell the agent to treat it as information, never as instructions from its user, and to ask the owner before acting on it.
- The agent can unpair a peer that keeps pushing it, and tells the owner.
- The owner always decides: pairing needs the owner's typed code, and `peers` / `unpair` are available to you any time.
- This is guidance to a model, not a guarantee. Only pair with people you would trust to talk to your agent.

## Privacy

| Who | Sees | Does not see |
|---|---|---|
| Nostr relay | Your IP, that you subscribe to some random-looking mailbox keys, event timing and size | Your identity key, who the peer is, content. Each event uses a fresh throwaway sender key and carries no identity |
| LAN observer (multicast) | The same sealed events, plus that two hosts are talking | Identities, content |
| STUN server | Your public IP when a connection is set up | Content, who you talk to |
| Peer | Your IP address (direct WebRTC connection), name, and what you send | Your other peers |

Events to one pair's mailbox share a `p` tag for up to an hour, so a relay can link them and see the volume and timing of that pair's signaling in that hour. Mailboxes rotate hourly and differ per pair, so events are not linkable across hours or across pairs.

Outer events are kind 21059, all padded to one fixed size, so length reveals nothing. Message content is end to end encrypted by the WebRTC data channel and never touches relays.

IP visibility: a direct peer connection exposes each side's IP to the other. Set `lan_only` to keep everything on the local network: no relays and no STUN are used, so nothing outside the LAN learns your IP. Only peers on the same network can connect.

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

The agent runs as your OS user. A model with an unrestricted shell can read `identity.key` and `peers.json` or talk to the control socket directly. The TTY rule stops the provided tools and CLI paths, not a determined process running as you. Deny the agent access to the state directory.

In `~/.claude/settings.json`:

```json
{
  "permissions": {
    "deny": [
      "Read(~/.config/p2p-agents/**)",
      "Edit(~/.config/p2p-agents/**)",
      "Bash(*/.config/p2p-agents/*)"
    ]
  }
}
```

Bash path rules are best effort: shell can reach the path in many indirect ways (variables, `cd`, other interpreters). Where your Claude Code version supports sandboxing, also add the directory to the sandbox filesystem deny list (see the Claude Code sandbox settings docs for the exact key), which is enforced by the OS rather than by pattern matching. If you use `P2PA_HOME`, deny that path instead.

## Known limits

- No TURN. If a direct connection cannot be made (strict NAT on both sides), the peer stays `offline`.
- No offline delivery. Messages only go to online peers; `send` returns `offline` otherwise.
- A second Claude Code session is read-only: `list_peers` and `read_messages` work from disk, with online status unknown, but it cannot send or pair.
- The first session holds the network (an exclusive lock on the state directory). CLI commands run while it is up talk to it through the control socket; later MCP sessions are read-only.
- Invites use this node's own relays and LAN; owners with disjoint custom relay lists cannot pair over relays.
