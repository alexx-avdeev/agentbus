# agentbus

Message bus for AI agents that work on the same project from different
machines. RabbitMQ underneath, one Rust binary on top, and a Claude Code skill
that teaches an agent the protocol.

```
                       ┌──────────── agentbus (topic exchange) ────────────┐
   alice ── publish ──▶│  <project>.direct.<agent>  <project>.broadcast    │
                       │  <project>.control (hello / roster / bye)         │
                       └───┬───────────────┬───────────────┬───────────────┘
                           ▼               ▼               ▼
                  agentbus.<p>.alice  agentbus.<p>.bob   agentbus.log.<p>   (stream, replayable)
                  (quorum queue)      (quorum queue)       ▲        ▲
                                                           │        └── agentbus serve  (browser)
                                                           └─────────── agentbus log -f (terminal)
```

- Each agent has its own durable queue and pulls from it with `recv`.
- Broadcasts reach every queue; the sender's own copy is dropped client-side.
- Every message, including the handshake, lands in a per-project stream with
  7 days of retention. Agents and humans replay it with `log`.
- `hello`, `roster`, and `bye` go only to the stream, so `recv` is work-only.

## Run the broker

```bash
docker compose up -d          # RabbitMQ 4 with management UI on :15672 (guest/guest)
```

The compose file uses host networking so other machines can reach port 5672.
RabbitMQ's `guest` user only works from localhost; for remote agents create a
real user:

```bash
docker exec agentbus-rabbitmq rabbitmqctl add_user agent 'a-long-password'
docker exec agentbus-rabbitmq rabbitmqctl set_permissions agent '.*' '.*' '.*'
```

and point them at `AMQP_URL=amqp://agent:a-long-password@<host>:5672/%2f`.
Put TLS or a WireGuard/Tailscale mesh in front of it before leaving a LAN.

## Install on a machine

```bash
git clone <remote>/agentbus && cd agentbus && ./install.sh
```

That builds the binary into `~/.cargo/bin` and links the skill into
`~/.claude/skills/agentbus`. It is enough to tell a Claude Code agent on a new
machine: "clone <remote>/agentbus and run its install.sh". Needs Rust
(https://rustup.rs) on the machine.

## Protocol

```bash
# coordinator
agentbus join -p demo -a alice --role coordinator --roster alice,bob,carol
agentbus wait  -p demo --timeout 10m

# every other agent
agentbus join -p demo -a bob --role backend
agentbus wait  -p demo --timeout 10m

# then
agentbus send -p demo -a alice --to bob -k task "implement the login endpoint"
agentbus recv -p demo -a bob --wait 60s            # JSON lines
agentbus send -p demo -a bob --to alice -k result -t <thread> "done, PR 42"
agentbus leave -p demo -a bob
```

`-p` / `-a` default to `AGENTBUS_PROJECT` / `AGENTBUS_AGENT`; `--url` to `AMQP_URL`.

`wait` reads the whole retained log, takes the newest `roster`, and returns once
every listed agent has published a `hello` after that roster. Re-running
`join --roster` therefore starts a fresh session on the same project.

## Reading the log

```bash
agentbus log -p demo                    # everything retained, text
agentbus log -p demo --since 2h -f      # tail
agentbus log -p demo -t <thread> --json # one thread, JSON lines
agentbus serve -p demo -p other         # http://127.0.0.1:8080/
```

## Skill

`skill/SKILL.md` is the Claude Code skill; `install.sh` links it. Launch each agent with `AGENTBUS_PROJECT`, `AGENTBUS_AGENT` and, for the
coordinator, a prompt that says so and lists the roster.

## Message format

```json
{"id":"uuid","ts":"2026-11-05T16:15:23Z","project":"demo","from":"alice","to":"bob",
 "thread":"uuid","reply_to":"uuid","kind":"task","body":"...","data":{}}
```

`to` is absent on broadcasts. `data` carries structured payloads (the roster list).

## License

MIT, see [LICENSE](LICENSE).
