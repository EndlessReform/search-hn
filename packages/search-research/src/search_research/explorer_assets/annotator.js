/* Frozen teacher proposals plus durable, reversible span edits. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  let queue = [], filtered = [], item = null, selection = null, busy = false;
  let noteTimer;
  const initial = new URLSearchParams(location.search);
  const base = () => `/api/entity-annotations/${$('batch').value}`;
  function error(message) { $('annotation-error').textContent = message; $('annotation-error').hidden = !message; }
  async function api(url, body) {
    const response = await fetch(url, body ? {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)} : {});
    if (!response.ok) {
      const result = await response.json();
      throw new Error(typeof result.detail === 'string' ? result.detail : JSON.stringify(result.detail));
    }
    return response.json();
  }
  async function task(fn) {
    if (busy) return;
    busy = true; error('');
    document.querySelectorAll('button,select').forEach(node => { node.disabled = true; });
    try { await saveNotes(); await fn(); } catch (e) { error(e.message); }
    finally { busy = false; controls(); }
  }
  function controls() {
    document.querySelectorAll('button,select').forEach(node => { node.disabled = busy; });
    const index = filtered.findIndex(r => r.comment_id === item?.comment_id);
    $('prev').disabled = busy || index <= 0;
    $('next').disabled = busy || index < 0 || index >= filtered.length - 1;
    $('review').disabled = busy || !item;
    $('unreview').disabled = busy || !item?.reviewed;
    $('add').disabled = busy || !selection || !item;
  }
  function applyFilters() {
    filtered = queue.filter(r =>
      ($('status-filter').value === 'all' || Boolean(r.reviewed) === ($('status-filter').value === 'reviewed')) &&
      ($('split-filter').value === 'all' || r.split === $('split-filter').value) &&
      ($('notes-filter').value === 'all' || r.has_notes) &&
      ($('source-filter').value === 'all' || r.source === $('source-filter').value));
    $('jump').replaceChildren(...filtered.map(r => new Option(`${r.reviewed ? '✓' : '○'} ${r.ordinal} · ${r.comment_id} · ${r.entities} titles`, r.comment_id)));
    const reviewed = queue.filter(r => r.reviewed).length;
    $('progress').textContent = `${reviewed} / ${queue.length} reviewed · ${filtered.length} in view`;
    $('meter').max = queue.length || 1; $('meter').value = reviewed;
    $('export').href = `${base()}/export`;
  }
  async function refreshQueue() { queue = await api(base()); applyFilters(); }
  async function show(cid) {
    selection = null; $('selection-preview').textContent = 'Select text to add a missing title.';
    item = cid ? await api(`${base()}/comments/${cid}`) : null;
    $('card').hidden = !item; $('empty').hidden = Boolean(item);
    if (!item) { $('position').textContent = ''; $('save-state').textContent = ''; return; }
    $('jump').value = item.comment_id;
    history.replaceState(null, '', `/annotator?batch=${$('batch').value}&comment=${item.comment_id}`);
    render();
  }
  function render() {
    $('review-note').value = item.note;
    $('note-state').textContent = 'Notes saved.';
    const index = filtered.findIndex(r => r.comment_id === item.comment_id);
    $('position').textContent = `${index+1} / ${filtered.length} in view`;
    $('save-state').textContent = item.reviewed ? '✓ Reviewed · saved' : 'Unreviewed · edits saved';
    const link = document.createElement('a'); link.href = `https://news.ycombinator.com/item?id=${item.comment_id}`;
    link.target = '_blank'; link.rel = 'noopener'; link.textContent = `HN ${item.comment_id} ↗`;
    $('meta').replaceChildren(link, document.createTextNode(`#${item.ordinal} · ${item.split === 'test' ? 'Random evaluation' : 'Training'} · ${item.source.replaceAll('_',' ')}`));
    // Match the playground renderer: offsets count Unicode code points, not UTF-16.
    const chars = Array.from(item.text), spans = item.entities.filter(e => e.start !== null);
    const bounds = [...new Set([0, chars.length, ...spans.flatMap(e => [e.start,e.end])])].sort((a,b) => a-b);
    $('comment').replaceChildren();
    for (let i=0; i<bounds.length-1; i++) {
      const start=bounds[i], end=bounds[i+1], covering=spans.filter(e => e.start<=start && e.end>=end);
      const node=document.createElement(covering.length ? 'mark' : 'span'); node.textContent=chars.slice(start,end).join('');
      if (covering.length) {
        node.className=covering.every(e => e.deleted) ? 'deleted' : covering.some(e => !e.deleted && e.origin==='manual') ? 'manual' : '';
        node.title=covering.map(e => `${e.title} · ${e.origin}${e.deleted ? ' · deleted' : ''}`).join('\n');
      }
      $('comment').append(node);
    }
    $('entity-count').textContent = `(${item.entities.filter(e => !e.deleted).length} active)`;
    $('entity-list').replaceChildren();
    if (!item.entities.length) $('entity-list').textContent='No predicted titles. Select text above to add a missing title, or mark reviewed to confirm none.';
    for (const entity of item.entities) {
      const row=document.createElement('div'); row.className=`entity-row ${entity.origin}${entity.deleted ? ' deleted' : ''}`;
      const title=document.createElement('span'); title.className='entity-title'; title.textContent=entity.title;
      const origin=document.createElement('span'); origin.className='entity-origin'; origin.textContent=entity.origin;
      row.append(title,origin);
      if (entity.start === null) { const warning=document.createElement('span'); warning.className='unmatched'; warning.textContent='No exact text span'; row.append(warning); }
      const button=document.createElement('button'); button.textContent=entity.deleted ? 'Restore' : 'Delete';
      button.setAttribute('aria-label', `${button.textContent} ${entity.title}`);
      button.onclick=() => task(() => edit({action:entity.deleted ? 'restore':'delete', entity_id:entity.id}));
      row.append(button); $('entity-list').append(row);
    }
    const list=document.createElement('ul');
    for (const book of item.comparison.books) { const li=document.createElement('li'); li.textContent=book.title+(book.author ? ` — ${book.author}` : ''); list.append(li); }
    $('comparison-list').replaceChildren(item.comparison.books.length ? list : document.createTextNode('Luna returned no books.'));
    $('comparison').open = item.source === 'disagreement';
  }
  async function edit(change, advance=false) {
    const index=filtered.findIndex(r => r.comment_id===item.comment_id);
    const nextId=filtered[index+1]?.comment_id;
    item=await api(`${base()}/comments/${item.comment_id}`, {revision:item.revision, ...change});
    selection=null; $('selection-preview').textContent='Select text to add a missing title.';
    await refreshQueue();
    if (advance) {
      const next=filtered.find(r => r.comment_id===nextId) || filtered.find(r => !r.reviewed && r.comment_id!==item.comment_id);
      await show(next?.comment_id || (filtered.some(r => r.comment_id===item.comment_id) ? item.comment_id : filtered[0]?.comment_id));
    } else if (filtered.some(r => r.comment_id===item.comment_id)) { $('jump').value=item.comment_id; render(); }
    else await show(filtered[0]?.comment_id);
  }
  document.addEventListener('selectionchange', () => {
    const selected=window.getSelection();
    if (busy || !item || !selected.rangeCount || selected.isCollapsed) return;
    const range=selected.getRangeAt(0), body=$('comment');
    if (!body.contains(range.startContainer) || !body.contains(range.endContainer)) return;
    const before=range.cloneRange(); before.selectNodeContents(body); before.setEnd(range.startContainer,range.startOffset);
    const start=Array.from(before.toString()).length, text=range.toString();
    selection={start,end:start+Array.from(text).length};
    $('selection-preview').textContent=`Add: “${text}”`; controls();
  });
  async function saveNotes() {
    clearTimeout(noteTimer);
    while (item && $('review-note').value !== item.note) {
      const note = $('review-note').value;
      $('note-state').textContent = 'Saving note…';
      const saved = await api(`/api/entity-annotations/${item.batch_id}/comments/${item.comment_id}`, {action:'note', revision:item.revision, note});
      item.note = saved.note; item.revision = saved.revision;
      const row = queue.find(r => r.comment_id === item.comment_id);
      if (row) row.has_notes = Boolean(note.trim());
    }
    $('note-state').textContent = 'Notes saved.';
  }
  function autosaveNote() {
    if (busy) { noteTimer = setTimeout(autosaveNote, 300); return; }
    task(async () => {});
  }
  $('review-note').oninput = () => {
    $('note-state').textContent = 'Unsaved note…';
    clearTimeout(noteTimer); noteTimer = setTimeout(autosaveNote, 600);
  };
  $('save-note').onclick = () => task(async () => {});
  window.addEventListener('beforeunload', event => {
    if (item && $('review-note').value !== item.note) { event.preventDefault(); event.returnValue = ''; }
  });
  $('add').onclick=() => task(() => edit({action:'add', ...selection}));
  $('review').onclick=() => task(() => edit({action:'review'},true));
  $('unreview').onclick=() => task(() => edit({action:'unreview'}));
  const navigate=delta => task(async () => {
    const index=filtered.findIndex(r => r.comment_id===item?.comment_id);
    if (filtered[index+delta]) await show(filtered[index+delta].comment_id);
  });
  $('prev').onclick=() => navigate(-1); $('next').onclick=() => navigate(1);
  $('jump').onchange=() => task(() => show(Number($('jump').value)));
  for (const id of ['status-filter','split-filter','source-filter','notes-filter']) $(id).onchange=() => task(async () => { applyFilters(); await show(filtered[0]?.comment_id); });
  $('batch').onchange=() => task(async () => { await refreshQueue(); await show(filtered.find(r => !r.reviewed)?.comment_id || filtered[0]?.comment_id); });
  document.addEventListener('keydown', event => {
    if (busy || event.repeat || event.isComposing || event.target.isContentEditable || /INPUT|TEXTAREA|SELECT/.test(event.target.tagName)) return;
    if ((event.ctrlKey || event.metaKey) && event.key==='Enter' && item) { event.preventDefault(); $('review').click(); }
    else if (!event.ctrlKey && !event.metaKey && !event.altKey && event.key.toLowerCase()==='a' && item) { event.preventDefault(); $('review').click(); }
    else if (!event.ctrlKey && !event.metaKey && event.key.toLowerCase()==='n') $('next').click();
    else if (!event.ctrlKey && !event.metaKey && event.key.toLowerCase()==='p') $('prev').click();
  });
  task(async () => {
    const batches=await api('/api/entity-annotations');
    $('batch').replaceChildren(...batches.map(b => new Option(b.name,b.id)));
    if (batches.some(b => String(b.id)===initial.get('batch'))) $('batch').value=initial.get('batch');
    if (!batches.length) { $('empty').hidden=false; $('empty').textContent='No annotation batches have been populated yet.'; return; }
    await refreshQueue();
    const requested=Number(initial.get('comment'));
    await show(filtered.find(r => r.comment_id===requested)?.comment_id || filtered.find(r => !r.reviewed)?.comment_id || filtered[0]?.comment_id);
  });
})();
