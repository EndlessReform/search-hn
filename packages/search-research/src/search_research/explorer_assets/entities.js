/* Ontologies persist only in this browser; predictions belong to a rendered card. */
(() => {
  'use strict';
  const el = id => document.getElementById(id);
  const key = 'comment-lab:entity-ontologies:v1';
  let sets = JSON.parse(localStorage.getItem(key) || 'null') || [{name: 'Books', labels: ['book title', 'author', 'publisher'], thresholds: {'book title': 0.5, author: 0.5, publisher: 0.5}}];
  let editing = null;
  const active = () => sets.find(s => s.name === el('ontology').value);
  function save() { localStorage.setItem(key, JSON.stringify(sets)); }
  function controls(name) {
    el('ontology').replaceChildren(...sets.map(set => new Option(set.name, set.name)));
    if (sets.some(s => s.name === name)) el('ontology').value = name;
    thresholds();
  }
  function thresholds() {
    el('entity-thresholds').replaceChildren();
    for (const label of active()?.labels || []) {
      const row = document.createElement('label');
      row.append(document.createTextNode(label + ' '));
      const slider = document.createElement('input');
      Object.assign(slider, {type: 'range', min: 0, max: 1, step: 0.01, value: active().thresholds[label]});
      const output = document.createElement('output');
      output.value = Number(slider.value).toFixed(2);
      slider.addEventListener('input', () => {
        active().thresholds[label] = Number(slider.value); output.value = Number(slider.value).toFixed(2); save();
      });
      row.append(slider, output); el('entity-thresholds').append(row);
    }
  }
  function edit(set) {
    editing = set?.name ?? null;
    el('ontology-name').value = set?.name || '';
    el('ontology-labels').value = set?.labels.join('\n') || '';
    el('delete-ontology').disabled = !set;
    el('ontology-error').textContent = '';
  }
  el('ontology').addEventListener('change', thresholds);
  el('edit-ontologies').onclick = () => { edit(active()); el('ontology-editor').showModal(); };
  el('close-ontologies').onclick = () => el('ontology-editor').close();
  el('new-ontology').onclick = () => edit(null);
  el('delete-ontology').onclick = () => {
    sets = sets.filter(s => s.name !== editing); save(); controls(); edit(active());
  };
  el('ontology-form').onsubmit = event => {
    event.preventDefault();
    const name = el('ontology-name').value.trim();
    const labels = [...new Set(el('ontology-labels').value.split('\n').map(x => x.trim()).filter(Boolean))];
    if (!name || !labels.length || sets.some(s => s.name === name && s.name !== editing)) {
      el('ontology-error').textContent = 'Use a unique set name and at least one label.'; return;
    }
    const previous = sets.find(s => s.name === editing);
    const set = {name, labels, thresholds: Object.fromEntries(labels.map(label => [label, previous?.thresholds[label] ?? 0.5]))};
    if (previous) sets[sets.indexOf(previous)] = set; else sets.push(set);
    save(); controls(name); el('ontology-editor').close();
  };

  function show(card, data, ontology) {
    card.querySelector('.entity-result')?.remove();
    const panel = document.createElement('details'); panel.className = 'entity-result'; panel.open = true;
    const summary = document.createElement('summary');
    summary.textContent = `${ontology.name} · ${data.spans.length} entities · ${data.seconds.toFixed(2)}s · ${data.device} · ${data.dtype} · ${data.windows} window(s)`;
    panel.append(summary);
    const note = document.createElement('p');
    note.textContent = `Thresholds: ${ontology.labels.map(l => `${l} ≥ ${ontology.thresholds[l].toFixed(2)}`).join(' · ')}. Hover or focus a highlighted span for scores.`;
    panel.append(note);
    const body = document.createElement('div'); body.className = 'comment-text';
    // Python offsets count Unicode code points; JS slice normally counts UTF-16 units.
    const chars = Array.from(data.text);
    const bounds = [...new Set([0, chars.length, ...data.spans.flatMap(s => [s.start, s.end])])].sort((a,b) => a-b);
    for (let i=0; i<bounds.length-1; i++) {
      const start=bounds[i], end=bounds[i+1];
      const spans=data.spans.filter(s => s.start <= start && s.end >= end);
      const node=document.createElement(spans.length ? 'mark' : 'span');
      node.textContent=chars.slice(start,end).join('');
      if (spans.length) {
        const description=spans.map(s => `${s.text} — ${s.label}: ${(s.score*100).toFixed(1)}%`).join('\n');
        node.title=description; node.tabIndex=0; node.setAttribute('aria-label',description);
      }
      body.append(node);
    }
    panel.append(body);
    if (!data.spans.length) panel.append(document.createTextNode('No entities above the selected thresholds.'));
    for (const span of data.spans) {
      const row=document.createElement('div'); row.className='entity-score';
      const label=document.createElement('span'); label.textContent=`${span.text} → ${span.label}`;
      const meter=document.createElement('meter'); Object.assign(meter,{min:0,max:1,value:span.score});
      meter.setAttribute('aria-label',`${span.label} confidence`);
      const score=document.createElement('span'); score.textContent=`${(span.score*100).toFixed(1)}%`;
      row.append(label,meter,score); panel.append(row);
    }
    card.querySelector('.comment-actions').after(panel);
  }
  el('results').addEventListener('click', async event => {
    const button=event.target.closest('.entity-run'); if (!button) return;
    const card=button.closest('[data-comment]');
    if (!active()) { edit(null); el('ontology-editor').showModal(); return; }
    const ontology=structuredClone(active()); button.disabled=true; button.textContent='Extracting… (first load may take a while)';
    card.querySelector('.entity-error')?.remove();
    try {
      const response=await fetch(`/api/comments/${card.dataset.comment}/entities`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({labels:ontology.labels,thresholds:ontology.thresholds})});
      if (!response.ok) {
        const body=await response.text();
        throw new Error(`Extraction failed (${response.status}): ${body}`);
      }
      show(card,await response.json(),ontology);
    } catch (error) {
      const message=document.createElement('p'); message.className='entity-error'; message.setAttribute('role','alert'); message.textContent=error.message; button.after(message);
    } finally { button.disabled=false; button.textContent='Extract entities'; }
  });
  controls();
})();
