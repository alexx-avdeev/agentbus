---
name: agentbus
description: "Use this skill whenever you are told you are an agent on an agentbus project, or when the environment has AGENTBUS_PROJECT and AGENTBUS_AGENT set, or when asked to coordinate with other agents on other machines through RabbitMQ / agentbus. It covers the join handshake, waiting for the whole team, sending and receiving messages, threads, and leaving. Do NOT use for in-process subagents or for file-based agent communication."
---

# agentbus: talking to agents on other machines

`agentbus` is a CLI over RabbitMQ. Every agent on a project has its own queue.
A message goes to one agent (`--to`) or to everyone (`--broadcast`). Every
message, including the handshake, is also kept in a per-project log stream that
anyone can replay. You never see your own broadcasts.

Configuration comes from the environment. Check it first:

```bash
echo "$AGENTBUS_PROJECT $AGENTBUS_AGENT $AMQP_URL"
```

- `AGENTBUS_PROJECT`: the project (topic) you work on. Lower-case, `[a-z0-9_-]`.
- `AGENTBUS_AGENT`: your name on the bus. Other agents address you by it.
- `AMQP_URL`: broker URL. Defaults to the local broker if unset.

If any of these is missing, ask the user for it before doing anything else.
Every command below accepts `-p <project> -a <agent>` instead of the env vars.

## 1. Join

Run `join` exactly once at the start of the session. It creates the topology
if it does not exist yet, creates your queue, and publishes your `hello`.

```bash
agentbus join --role "one line: what you are responsible for"
```

**If you were told you are the coordinator**, you also announce the roster.
List every agent on the project including yourself. Run this before anyone
else joins if you can, but order does not actually matter:

```bash
agentbus join --role "coordinator, owns the plan" --roster alice,bob,carol
```

A new roster supersedes any older one, and only hellos published after it
count. Re-running `join --roster` is the way to restart a project session.

## 2. Wait for the team

Do not start work until everyone is present. `wait` returns 0 once every agent
on the latest roster has said hello, and prints the list.

```bash
agentbus wait --timeout 10m
```

If it times out it prints who is missing and exits 1. Tell the user; do not
start without them unless the user says so.

## 3. Catch up on history

Before your first message, read what has already been said on the project.
Earlier sessions, the roster, and other agents' roles are all here:

```bash
agentbus log --since 2h         # or --thread <id>, --from <agent>, --kind task
```

Use `--json` when you want to parse it.

## 4. Work loop

Pull messages addressed to you (direct or broadcast). Each line is one JSON
message. The call blocks up to `--wait` for the first message, then returns.

```bash
agentbus recv --wait 60s --max 10
```

Message fields: `id`, `ts`, `from`, `to` (absent for broadcast), `thread`,
`reply_to`, `kind`, `body`.

Send a message:

```bash
agentbus send --to bob --kind task "Implement POST /login. Spec in thread."
agentbus send --broadcast --kind note "Schema migrated; rerun your tests."
agentbus send --to alice --kind result --thread "$THREAD" --reply-to "$ID" "Done in PR 42."
agentbus send --to bob --kind question - <<'MSG'
Multi-line body goes through stdin.
MSG
```

`send` prints the `id` and `thread` it used. Keep them: replies on the same
topic must reuse the thread and should set `--reply-to`.

### Kinds

Free form, but use these so humans and other agents can filter:

| kind | meaning |
|---|---|
| `task` | asking someone to do something |
| `question` | needs an `answer` back |
| `answer` | reply to a question |
| `result` | a task is finished; say where the output is |
| `done` | you have nothing left on the project |
| `note` | FYI, no action expected |
| `block` | you are blocked; say on what and on whom |

### Loop shape

1. `recv`. If nothing arrived, continue your own work or `recv` again.
2. For each message: act, and reply **in the same thread** with the right kind.
   Answer `question`s before anything else.
3. When you hand work to someone, `send --kind task` and move on; do not poll
   for the result, it will arrive through `recv`.
4. When all your work is finished, broadcast `done` with a short summary of
   what you produced and where it lives.
5. Keep bodies concrete: paths, branch names, PR numbers, commands. The other
   agent is on another machine and cannot see your filesystem.

## 5. Leave

At the end of the session:

```bash
agentbus leave "handing off; see PR 42"
```

## Reading the log as a human

`agentbus log -f` tails the project in a terminal. `agentbus serve -p <project>`
serves a browser view on http://127.0.0.1:8080/. Mention these to the user if
they ask how to follow along.

## Troubleshooting

- `connecting to amqp://...` error: broker is down or `AMQP_URL` is wrong.
  Locally: `docker compose up -d` in the agentbus repo.
- `wait` never sees a roster: no coordinator has run `join --roster` for this
  project since the log's retention window. Ask the user who the coordinator is.
- `recv` shows nothing for a long time: that is normal when the others are busy.
  Check `agentbus log --since 10m` to see if traffic is flowing elsewhere.
