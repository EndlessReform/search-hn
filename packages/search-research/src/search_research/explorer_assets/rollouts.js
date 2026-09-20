'use strict';
const $ = id => document.getElementById(id);
const esc = x => String(x ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let active = null, summary = null, page = 1, busy = false, predictionTotal = 0;
const path = suffix => '/sets/' + active + '/rollouts' + suffix;
async function api(url, method='GET', body={}) {
  const response = await fetch('/api' + url, {method, ...(method === 'GET' ? {} : {
    headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)})});
  const data = await response.json();
  if (!response.ok) throw new Error(Array.isArray(data.detail) ? data.detail.map(x=>x.msg).join('; ') : data.detail || 'Request failed');
  return data;
}
async function run(action) {
  if (busy) return;
  busy = true; $('rollout-error').hidden = true; $('rollout-status').textContent = 'WORKING...';
  const controls = [...document.querySelectorAll('button,input,select')];
  const disabled = controls.map(c=>c.disabled); controls.forEach(c=>{c.disabled=true;});
  try { const message = await action(); await refresh(); $('rollout-status').textContent = message || 'READY / SAVED'; }
  catch(e) { $('rollout-error').textContent=e.message; $('rollout-error').hidden=false; $('rollout-status').textContent='ERROR'; }
  finally { controls.forEach((c,i)=>{c.disabled=disabled[i];}); busy=false; sync(); }
}
function sync() {
  const pool = Boolean(summary?.pool);
  const running = summary?.runs.some(r=>r.status==='running');
  $('create-pool').disabled=pool || !active;
  $('pool-seed').disabled=pool; $('test-fraction').disabled=pool;
  $('sample-all').disabled=!pool; $('invalidate').disabled=!pool || running;
  const eligible=summary?.eligible_pending || 0;
  const planned=Math.min(Number($('run-limit').value),eligible);
  $('run-preview').textContent=running ? 'A batch is running. These counts update as it finishes.' :
    `Will label ${planned.toLocaleString()} comments · ${eligible.toLocaleString()} eligible pending · ${(eligible-planned).toLocaleString()} left afterward` +
    (summary?.excluded_prompt_examples ? ` · ${summary.excluded_prompt_examples} prompt examples excluded` : '');
  $('start-run').textContent=`▶ LABEL ${planned.toLocaleString()} COMMENTS`;
  $('start-run').disabled=!pool || running || !summary?.credential_available || planned===0;
  $('stop-run').disabled=!running;
  $('pred-prev').disabled=page<=1; $('pred-next').disabled=page*50>=predictionTotal;
  $('start-rank').disabled=$('rule-kind').value!=='rank';
  $('end-rank').disabled=$('rule-kind').value!=='rank';
  $('start-score').disabled=$('rule-kind').value!=='similarity';
}
async function refresh() {
  if (!active) return;
  summary=await api(path(''));
  const selected=$('run-model').value;
  $('run-model').innerHTML=summary.models.map(m=>`<option>${esc(m)}</option>`).join('');
  if(summary.models.includes(selected)) $('run-model').value=selected;
  $('pool-info').textContent=summary.pool ? `POOL ${summary.pool.id} / frozen ${summary.pool.created_at} / ${summary.pool.anchor.positive_ids.length} anchor positives` : 'No active pool.';
  if(summary.pool) {
    $('pool-seed').value=summary.pool.seed; $('test-fraction').value=summary.pool.test_fraction;
  }
  // Keep unsaved rule edits intact while a run is being polled.
  if (!document.querySelector('#rules [data-dirty]')) $('rules').innerHTML=summary.rules.map(r=>{
    const spec=r.spec;
    const fields=spec.kind==='rank' ?
      `<label>Start rank<input name="start_rank" type="number" min="1" required value="${spec.start_rank}"></label><label>End rank<input name="end_rank" type="number" min="1" value="${spec.end_rank ?? ''}" placeholder="Onward"></label>` :
      (spec.kind==='similarity' ? `<label>Start cosine<input name="start_score" type="number" min="-1" max="1" step="any" required value="${spec.start_score}"></label>` : '');
    return `<form class="rule-row" data-rule="${r.id}"><b>${spec.kind==='rank'?'Rank range':spec.kind==='random'?'Random corpus sample':'Similarity, then downward'}</b>
      <div class="two-fields">${fields}<label ${spec.kind==='rank'&&spec.end_rank!=null?'hidden':''}>Candidate count<input name="count" type="number" min="1" value="${spec.count}"></label></div>
      <p>${r.picked} comments already picked</p><button>Save + sample</button><button type="button" data-delete-rule="${r.id}">Delete rule</button></form>`;
  }).join('');
  $('counts').textContent=(summary.credential_available?'VENDOR READY':'MISSING OPENROUTER_API_KEY')+'\n'+JSON.stringify(summary.counts,null,2)+'\nACCEPTED LABELS: '+JSON.stringify(summary.label_counts || {});
  const splits=summary.label_splits || {positive:{train:0,test:0},negative:{train:0,test:0}};
  const fmt=n=>n.toLocaleString();
  const pos=splits.positive, neg=splits.negative;
  $('label-summary').innerHTML=`<div class="label-totals"><strong>${fmt(pos.train+pos.test)} POSITIVE</strong><strong>${fmt(neg.train+neg.test)} NEGATIVE</strong></div>
    <table><caption>ACCEPTED / READY TO EXPORT</caption><thead><tr><th>Split</th><th>Positive</th><th>Negative</th><th>Total</th></tr></thead>
    <tbody>${['train','test'].map(split=>`<tr><th>${split.toUpperCase()}</th><td>${fmt(pos[split])}</td><td>${fmt(neg[split])}</td><td>${fmt(pos[split]+neg[split])}</td></tr>`).join('')}</tbody></table>
    <p>Actual assigned splits across the whole active pool, regardless of filters. Pending, errors, and rejected labels are excluded.</p>`;
  $('runs').innerHTML=summary.runs.map(r=>`<div class="run-row">RUN ${r.id} / ${esc(r.model)} / ${r.status}<br>${esc(JSON.stringify(r.counts))}<br>Reported cost: ${r.reported_cost==null?'unavailable':'$'+r.reported_cost.toFixed(6)}${r.error?'<pre>'+esc(r.error)+'</pre>':''}</div>`).join('');
  $('export').href='/api'+path('/export');
  if(summary.pool) await predictions();
  else { $('predictions').innerHTML='Create and sample a pool to begin.'; $('pred-page').textContent=''; }
  sync();
}
async function predictions() {
  const expanded=new Set([...document.querySelectorAll('.prediction details[open]')].map(el=>el.closest('[data-comment]').dataset.comment));
  const data=await api(path('/predictions')+'?page='+page+'&status='+encodeURIComponent($('status-filter').value)+'&label='+$('label-filter').value);
  predictionTotal=data.total;
  $('predictions').innerHTML=data.results.map(r=>`<article class="prediction ${r.status}" data-comment="${r.comment_id}">
    <div class="comment-meta"><span>RANK ${r.rank} / COS ${r.score.toFixed(5)} / ${r.split.toUpperCase()} / RULES ${r.sources.join(', ')}</span><span>${r.status.toUpperCase()}</span></div>
    <p><a href="https://news.ycombinator.com/item?id=${r.comment_id}" target="_blank" rel="noopener">${r.comment_id} ↗</a> / ${esc(r.author)}</p>
    <details ${expanded.has(String(r.comment_id))?'open':''}><summary>${esc(r.text.slice(0,220))}${r.text.length>220?'…':''}</summary><div class="comment-text">${esc(r.text)}</div></details>
    <p><b>${r.label ? (r.label.is_positive?'POSITIVE':'NEGATIVE')+' / '+esc(r.label.taxonomy) : 'No valid prediction'}</b> / ${esc(r.model || '')} / run ${r.run_id || '—'}</p>
    ${r.error?'<pre>'+esc(r.error)+'\nModel output: '+esc(r.raw_output || '(no response)')+'</pre>':''}
    <div class="comment-actions"><button data-action="accept" ${!r.label?'disabled':''}>Accept</button><button data-action="reject">Reject</button><button data-action="retry">Queue retry</button></div></article>`).join('') || '<p>No candidates match this filter.</p>';
  $('pred-page').textContent=`Page ${page} / ${Math.max(1,Math.ceil(data.total/50))} — ${data.total} rows`;
  $('pred-prev').disabled=page<=1; $('pred-next').disabled=page*50>=data.total;
}
$('create-pool').onclick=()=>run(()=>api(path('/pool'),'POST',{seed:Number($('pool-seed').value),test_fraction:Number($('test-fraction').value)}).then(()=>null));
$('sample-all').onclick=()=>run(async()=>{
  let added=0,overlap=0;
  for(const rule of summary.rules) {const r=await api(path('/rules/'+rule.id+'/sample'),'POST');added+=r.added;overlap+=r.overlap;}
  return `SAMPLED / ${added} new / ${overlap} overlaps`;
});
$('rules').oninput=event=>{event.target.closest('[data-rule]').dataset.dirty='true';};
$('rules').onsubmit=event=>{
  event.preventDefault();
  const form=event.target.closest('[data-rule]');
  const id=Number(form.dataset.rule);
  const original=summary.rules.find(r=>r.id===id).spec;
  const data=new FormData(form);
  const body={...original,count:Number(data.get('count'))};
  if(original.kind==='rank') {body.start_rank=Number(data.get('start_rank'));body.end_rank=data.get('end_rank')?Number(data.get('end_rank')):null;}
  if(original.kind==='similarity')body.start_score=Number(data.get('start_score'));
  run(async()=>{
    await api(path('/rules/'+id),'PATCH',body);
    const result=await api(path('/rules/'+id+'/sample'),'POST');
    delete form.dataset.dirty;
    return `Saved / ${result.added} new comments / existing picks preserved`;
  });
};
$('rules').onclick=event=>{
  const button=event.target.closest('[data-delete-rule]');if(!button)return;
  run(async()=>{
    await api(path('/rules/'+button.dataset.deleteRule),'DELETE');
    button.closest('[data-rule]').remove();
    return 'Rule deleted / existing picks and predictions preserved';
  });
};
$('rule-kind').onchange=sync;
$('run-limit').onchange=sync;
$('rule-form').onsubmit=event=>{
  event.preventDefault();
  run(()=>api(path('/rules'),'POST',{kind:$('rule-kind').value,start_rank:Number($('start-rank').value),
    end_rank:$('end-rank').value?Number($('end-rank').value):null,start_score:Number($('start-score').value),count:Number($('rule-count').value)}).then(()=>null));
};
$('invalidate').onclick=()=>{ $('invalidate-confirmation').value=''; $('invalidate-dialog').showModal(); };
$('cancel-invalidate').onclick=()=>$('invalidate-dialog').close();
$('confirm-invalidate').onclick=()=>{
  const confirmation=$('invalidate-confirmation').value;
  if(confirmation!=='INVALIDATE ALL') { $('invalidate-confirmation').setCustomValidity('Type INVALIDATE ALL exactly'); $('invalidate-confirmation').reportValidity(); return; }
  $('invalidate-dialog').close();
  run(()=>api(path('/invalidate'),'POST',{confirmation}).then(()=>{page=1;}));
};
$('invalidate-confirmation').oninput=()=>$('invalidate-confirmation').setCustomValidity('');
$('start-run').onclick=()=>run(()=>api(path('/run'),'POST',{model:$('run-model').value,limit:Number($('run-limit').value),concurrency:Number($('concurrency').value)}).then(r=>'STARTED RUN '+r.run_id));
$('stop-run').onclick=()=>run(()=>api(path('/stop'),'POST').then(()=>'Stopping after in-flight calls'));
$('predictions').onclick=event=>{
  const button=event.target.closest('[data-action]');if(!button)return;
  run(()=>api(path('/picks/'+button.closest('[data-comment]').dataset.comment),'POST',{action:button.dataset.action}).then(()=>null));
};
for(const id of ['status-filter','label-filter'])$(id).onchange=()=>run(async()=>{page=1;});
$('pred-prev').onclick=()=>run(async()=>{page--;});
$('pred-next').onclick=()=>run(async()=>{page++;});
$('rollout-set').onchange=()=>run(async()=>{active=Number($('rollout-set').value);page=1;history.replaceState(null,'','/rollouts?set='+active);});
run(async()=>{
  const data=await api('/sets');
  $('rollout-set').innerHTML=data.sets.map(s=>`<option value="${s.id}">${esc(s.name)}</option>`).join('');
  const requested=Number(new URLSearchParams(location.search).get('set'))||Number(localStorage.getItem(`comment-lab:${data.corpus_id}:active`));
  active=data.sets.find(s=>s.id===requested)?.id??data.sets[0]?.id;
  if(active)$('rollout-set').value=active;
});
setInterval(async()=>{
  if(busy||!summary?.runs.some(r=>r.status==='running'))return;
  busy=true;try{await refresh();}catch(e){$('rollout-error').textContent=e.message;$('rollout-error').hidden=false;}finally{busy=false;}
},2500);
