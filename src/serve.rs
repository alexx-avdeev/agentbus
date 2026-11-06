//! Browser view: one page, project tabs, messages grouped by thread, live tail over SSE.

use anyhow::Result;
use axum::{
    extract::{Query, State},
    response::{sse::{Event, KeepAlive, Sse}, Html, IntoResponse, Json},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use futures_util::stream::Stream;
use serde::Deserialize;
use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::bus::{Bus, Event as BusEvent, Offset};
use crate::message::Message;

#[derive(Clone)]
struct AppState {
    projects: Vec<String>,
    history: Arc<RwLock<HashMap<String, Vec<Message>>>>,
    live: broadcast::Sender<Message>,
}

#[derive(Deserialize)]
struct ProjectQuery {
    project: Option<String>,
}

pub async fn run(url: &str, projects: Vec<String>, bind: &str, since: Option<DateTime<Utc>>) -> Result<()> {
    let (live, _) = broadcast::channel::<Message>(1024);
    let state = AppState {
        projects: projects.clone(),
        history: Arc::new(RwLock::new(HashMap::new())),
        live,
    };

    for project in projects {
        let url = url.to_string();
        let st = state.clone();
        let offset = match since {
            Some(t) => Offset::Since(t),
            None => Offset::First,
        };
        tokio::spawn(async move {
            loop {
                match Bus::connect(&url).await {
                    Ok(bus) => {
                        let _ = bus.ensure_project(&project, "7D").await;
                        let p = project.clone();
                        let st2 = st.clone();
                        let res = bus
                            .replay(&project, offset.clone(), None, move |ev| {
                                let BusEvent::Msg(m) = ev else { return true };
                                let st3 = st2.clone();
                                let p2 = p.clone();
                                tokio::spawn(async move {
                                    st3.history.write().await.entry(p2).or_default().push(m.clone());
                                    let _ = st3.live.send(m);
                                });
                                true
                            })
                            .await;
                        tracing::warn!(project, ?res, "stream reader stopped; reconnecting");
                    }
                    Err(e) => tracing::warn!(project, error = %e, "connect failed; retrying"),
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    let app = Router::new()
        .route("/", get(index))
        .route("/api/projects", get(projects_api))
        .route("/api/messages", get(messages_api))
        .route("/api/events", get(events_api))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    eprintln!("agentbus viewer on http://{bind}/");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn projects_api(State(st): State<AppState>) -> Json<Vec<String>> {
    Json(st.projects.clone())
}

async fn messages_api(State(st): State<AppState>, Query(q): Query<ProjectQuery>) -> Json<Vec<Message>> {
    let h = st.history.read().await;
    let project = q.project.or_else(|| st.projects.first().cloned()).unwrap_or_default();
    Json(h.get(&project).cloned().unwrap_or_default())
}

async fn events_api(
    State(st): State<AppState>,
    Query(q): Query<ProjectQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let project = q.project.or_else(|| st.projects.first().cloned()).unwrap_or_default();
    let rx = st.live.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |r| match r {
        Ok(m) if m.project == project => Some(Ok(Event::default().data(serde_json::to_string(&m).unwrap_or_default()))),
        _ => None,
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn index() -> impl IntoResponse {
    Html(INDEX)
}

const INDEX: &str = r##"<!doctype html>
<html><head><meta charset="utf-8"><title>agentbus</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{--bg:#fff;--fg:#1a1a1a;--muted:#6b7280;--line:#e5e7eb;--card:#f9fafb;--accent:#2563eb;--ctrl:#9ca3af}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--fg:#e5e7eb;--muted:#9ca3af;--line:#262a33;--card:#161a22;--accent:#60a5fa;--ctrl:#6b7280}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 ui-sans-serif,system-ui,sans-serif}
header{display:flex;gap:12px;align-items:center;padding:10px 16px;border-bottom:1px solid var(--line);position:sticky;top:0;background:var(--bg);flex-wrap:wrap}
header h1{font-size:15px;margin:0 8px 0 0}
select,input,label{font:inherit;color:inherit;background:var(--card);border:1px solid var(--line);border-radius:6px;padding:4px 8px}
label{display:flex;gap:6px;align-items:center;border:none;background:none;padding:0}
main{padding:12px 16px;max-width:1100px;margin:0 auto}
.thread{border:1px solid var(--line);border-radius:8px;margin:0 0 12px;background:var(--card);overflow:hidden}
.thread>summary{cursor:pointer;padding:8px 12px;display:flex;gap:10px;align-items:baseline;list-style:none}
.thread>summary::-webkit-details-marker{display:none}
.thread>summary .t{font-family:ui-monospace,monospace;color:var(--muted);font-size:12px}
.thread>summary .n{margin-left:auto;color:var(--muted);font-size:12px}
.msg{padding:8px 12px;border-top:1px solid var(--line)}
.msg .meta{display:flex;gap:8px;flex-wrap:wrap;font-size:12px;color:var(--muted);align-items:baseline}
.msg .meta b{color:var(--fg)}
.msg .kind{border:1px solid var(--line);border-radius:999px;padding:0 7px;font-size:11px}
.msg.control{opacity:.7}.msg.control .kind{color:var(--ctrl)}
.msg pre{margin:4px 0 0;white-space:pre-wrap;word-break:break-word;font:13px/1.4 ui-monospace,monospace}
.empty{color:var(--muted);padding:40px 0;text-align:center}
#status{margin-left:auto;font-size:12px;color:var(--muted)}
</style></head><body>
<header><h1>agentbus</h1>
<select id="project"></select>
<input id="filter" placeholder="filter: agent, kind, thread, text" size="34">
<label><input type="checkbox" id="showctl" checked>control</label>
<label><input type="checkbox" id="follow" checked>follow</label>
<span id="status">connecting…</span></header>
<main id="list"><div class="empty">loading…</div></main>
<script>
const $=s=>document.querySelector(s);
let msgs=[],es=null,project=null;
function esc(s){return s.replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function ts(s){const d=new Date(s);return d.toLocaleString(undefined,{hour12:false})}
function render(){
  const f=$('#filter').value.toLowerCase(), ctl=$('#showctl').checked;
  const threads=new Map();
  for(const m of msgs){
    if(!ctl&&['hello','roster','bye'].includes(m.kind))continue;
    if(f&&!(m.from+' '+(m.to||'')+' '+m.kind+' '+m.thread+' '+m.body).toLowerCase().includes(f))continue;
    const key=['hello','roster','bye'].includes(m.kind)?'control':m.thread;
    if(!threads.has(key))threads.set(key,[]);threads.get(key).push(m);
  }
  const list=$('#list');
  if(!threads.size){list.innerHTML='<div class="empty">no messages</div>';return}
  const open=new Set([...document.querySelectorAll('details[open]')].map(d=>d.dataset.t));
  const first=!document.querySelector('details');
  list.innerHTML=[...threads.entries()].sort((a,b)=>b[1][b[1].length-1].ts.localeCompare(a[1][a[1].length-1].ts)).map(([t,ms])=>{
    const isOpen=first||open.has(t);
    const title=t==='control'?'control (hello / roster / bye)':esc(ms[0].body.split('\n')[0].slice(0,90)||ms[0].kind);
    return `<details class="thread" data-t="${esc(t)}" ${isOpen?'open':''}><summary><span class="t">${esc(t.slice(0,8))}</span><span>${title}</span><span class="n">${ms.length} · ${ts(ms[ms.length-1].ts)}</span></summary>`+
      ms.map(m=>`<div class="msg ${['hello','roster','bye'].includes(m.kind)?'control':''}"><div class="meta"><span>${ts(m.ts)}</span><b>${esc(m.from)}</b><span>→ ${esc(m.to||'everyone')}</span><span class="kind">${esc(m.kind)}</span>${m.reply_to?`<span>re ${esc(m.reply_to.slice(0,8))}</span>`:''}${m.data&&m.data.agents?`<span>roster: ${esc(m.data.agents.join(', '))}</span>`:''}</div>${m.body?`<pre>${esc(m.body)}</pre>`:''}</div>`).join('')+'</details>';
  }).join('');
}
async function load(){
  project=$('#project').value;msgs=[];
  const r=await fetch('/api/messages?project='+encodeURIComponent(project));msgs=await r.json();render();
  if(es)es.close();
  es=new EventSource('/api/events?project='+encodeURIComponent(project));
  es.onopen=()=>$('#status').textContent='live';
  es.onerror=()=>$('#status').textContent='reconnecting…';
  es.onmessage=e=>{const m=JSON.parse(e.data);if(!msgs.some(x=>x.id===m.id)){msgs.push(m);render();if($('#follow').checked)window.scrollTo(0,0)}};
}
(async()=>{
  const ps=await (await fetch('/api/projects')).json();
  $('#project').innerHTML=ps.map(p=>`<option>${esc(p)}</option>`).join('');
  $('#project').onchange=load;$('#filter').oninput=render;$('#showctl').onchange=render;
  load();
})();
</script></body></html>"##;
