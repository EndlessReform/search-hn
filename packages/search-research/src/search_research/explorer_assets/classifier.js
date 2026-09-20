'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let active = null, catalog = [], draft = null, dirty = false, busy = false, preferenceKey = '';
async function api(path, method = 'GET', body) {
  const response = await fetch('/api' + path, {method,
    ...(method === 'GET' ? {} : {headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)})});
  const data = await response.json();
  if (!response.ok) throw new Error(Array.isArray(data.detail) ? data.detail.map(d => d.msg).join('; ') : data.detail || 'Request failed');
  return data;
}
async function run(action) {
  if (busy) return;
  busy = true;
  $('workbench-error').hidden = true;
  const controls = [...document.querySelectorAll('button,input,select,textarea')];
  const disabled = controls.map(c => c.disabled);
  controls.forEach(c => { c.disabled = true; });
  try { await action(); }
  catch (error) { $('workbench-error').textContent = error.message; $('workbench-error').hidden = false; }
  finally { controls.forEach((c, i) => { c.disabled = disabled[i]; }); busy = false; }
}
function edited() {
  dirty = true;
  $('draft-status').textContent = 'UNSAVED DRAFT';
  $('preview-status').textContent = 'DRAFT CHANGED / compile to refresh preview';
  $('compiled-prompt').value = '';
  $('compiled-schema').value = '';
  $('token-count').textContent = 'o200k_base / RECOMPILE REQUIRED';
}
function renderTaxonomy() {
  $('taxonomy').innerHTML = draft.taxonomy.map((t, i) => `<div class="taxon" data-taxon="${i}">
    <div class="taxon-head"><label>Taxonomy name<input data-field="name" value="${esc(t.name)}"></label>
    <label>Label<select data-field="is_positive"><option value="true" ${t.is_positive ? 'selected' : ''}>Positive</option><option value="false" ${!t.is_positive ? 'selected' : ''}>Negative</option></select></label>
    <button data-remove-taxon="${i}" aria-label="Remove taxonomy entry ${i + 1}">Remove</button></div>
    <label>Description<textarea data-field="description" rows="3">${esc(t.description)}</textarea></label></div>`).join('');
}
function renderExamples() {
  const choices = new Map(draft.examples.map(e => [e.comment_id, e]));
  const missing = draft.examples.filter(e => !catalog.some(c => c.comment_id === e.comment_id));
  $('example-catalog').innerHTML = missing.map(e =>
    `<p>Example ${e.comment_id} is no longer labeled. <button data-remove-example="${e.comment_id}">Remove missing example</button></p>`).join('') +
    ['positive', 'negative'].map(label => {
      const rows = catalog.filter(r => r.label === label);
      return `<h3 class="example-group">${label.toUpperCase()} EXAMPLES / ${rows.length}</h3>` + rows.map(row => {
        const choice = choices.get(row.comment_id);
        return `<article class="example-option ${label}-example" data-example="${row.comment_id}">
          <label><input type="checkbox" class="include-example" ${choice ? 'checked' : ''}> Include ${label} example <a href="https://news.ycombinator.com/item?id=${row.comment_id}" target="_blank" rel="noopener">${row.comment_id} ↗</a></label>
          <details ${choice ? 'open' : ''}><summary>${esc(row.text.slice(0, 150))}${row.text.length > 150 ? '…' : ''}</summary><div class="example-text">${esc(row.text)}</div></details>
          <div class="rationale-controls" ${choice ? '' : 'hidden'}>
            <label>Rationale in prompt<select class="rationale-mode">
              <option value="omit" ${choice?.rationale === 'omit' ? 'selected' : ''}>Omit rationale</option>
              <option value="saved" ${choice?.rationale === 'saved' ? 'selected' : ''}>Use saved rationale</option>
              <option value="custom" ${choice?.rationale === 'custom' ? 'selected' : ''}>Write custom rationale</option>
            </select></label>
            <div class="saved-note" ${choice?.rationale === 'saved' ? '' : 'hidden'}>${esc(row.saved_rationale || '(No saved rationale for this example.)')}</div>
            <label class="custom-label" ${choice?.rationale === 'custom' ? '' : 'hidden'}>Custom rationale<textarea class="custom-rationale" rows="3">${esc(choice?.custom_rationale || '')}</textarea></label>
          </div></article>`;
      }).join('');
    }).join('');
  filterExamples();
  countExamples();
}
function countExamples() {
  $('example-count').textContent = `${draft.examples.length} selected / ${catalog.length} labeled examples available`;
}
function filterExamples() {
  const query = $('example-filter').value.toLowerCase();
  const matches = new Set(catalog.filter(r => `${r.comment_id} ${r.text} ${r.saved_rationale}`.toLowerCase().includes(query)).map(r => r.comment_id));
  document.querySelectorAll('[data-example]').forEach(el => { el.hidden = !matches.has(Number(el.dataset.example)); });
}
async function loadSet(id) {
  const data = await api(`/sets/${id}/classifier`);
  active = id; draft = data.draft; catalog = data.catalog;
  $('classifier-set').value = String(id);
  localStorage.setItem(preferenceKey, String(id));
  history.replaceState(null, '', `/classifier?set=${id}`);
  $('category-description').value = draft.description;
  $('example-filter').value = '';
  renderTaxonomy(); renderExamples();
  edited(); dirty = false;
  $('draft-status').textContent = 'DRAFT LOADED';
  $('preview-status').textContent = 'Save + compile to inspect the model-facing prompt.';
}
async function saveDraft() {
  await api(`/sets/${active}/classifier`, 'PUT', draft);
  dirty = false;
  $('draft-status').textContent = 'DRAFT SAVED';
}
$('classifier-set').addEventListener('change', event => {
  const next = Number(event.target.value);
  $('classifier-set').value = String(active);
  run(async () => { if (dirty) await saveDraft(); await loadSet(next); });
});
$('category-description').addEventListener('input', event => { draft.description = event.target.value; edited(); });
$('add-taxon').addEventListener('click', () => {
  draft.taxonomy.push({name: '', description: '', is_positive: true}); renderTaxonomy(); edited();
});
$('taxonomy').addEventListener('input', event => {
  const field = event.target.dataset.field;
  if (!field) return;
  const taxon = draft.taxonomy[Number(event.target.closest('[data-taxon]').dataset.taxon)];
  taxon[field] = field === 'is_positive' ? event.target.value === 'true' : event.target.value;
  edited();
});
$('taxonomy').addEventListener('click', event => {
  const button = event.target.closest('[data-remove-taxon]');
  if (!button) return;
  draft.taxonomy.splice(Number(button.dataset.removeTaxon), 1); renderTaxonomy(); edited();
});
$('example-catalog').addEventListener('click', event => {
  const button = event.target.closest('[data-remove-example]');
  if (!button) return;
  draft.examples = draft.examples.filter(e => e.comment_id !== Number(button.dataset.removeExample));
  renderExamples(); edited();
});
$('example-catalog').addEventListener('input', event => {
  const card = event.target.closest('[data-example]');
  if (!card) return;
  const id = Number(card.dataset.example);
  let choice = draft.examples.find(e => e.comment_id === id);
  if (event.target.classList.contains('include-example')) {
    if (event.target.checked) {
      const row = catalog.find(r => r.comment_id === id);
      choice = {comment_id: id, rationale: row.saved_rationale ? 'saved' : 'omit', custom_rationale: ''};
      draft.examples.push(choice);
      card.querySelector('.rationale-mode').value = choice.rationale;
      card.querySelector('details').open = true;
    } else { draft.examples = draft.examples.filter(e => e.comment_id !== id); choice = null; }
  } else if (choice) {
    choice.rationale = card.querySelector('.rationale-mode').value;
    choice.custom_rationale = card.querySelector('.custom-rationale').value;
  }
  card.querySelector('.rationale-controls').hidden = !choice;
  card.querySelector('.saved-note').hidden = choice?.rationale !== 'saved';
  card.querySelector('.custom-label').hidden = choice?.rationale !== 'custom';
  countExamples(); edited();
});
$('example-filter').addEventListener('input', filterExamples);
$('save-draft').addEventListener('click', () => run(saveDraft));
$('compile-draft').addEventListener('click', () => run(async () => {
  await saveDraft();
  const data = await api(`/sets/${active}/classifier/compile`, 'POST', draft);
  $('compiled-prompt').value = data.prompt;
  $('compiled-schema').value = data.schema_text;
  $('token-count').textContent = `o200k_base\nPROMPT: ${data.prompt_tokens.toLocaleString()} tokens\nSCHEMA TEXT: ${data.schema_tokens.toLocaleString()} tokens`;
  $('preview-status').textContent = `COMPILED / ${data.example_count} examples / current saved labels and notes`;
}));
window.addEventListener('beforeunload', event => { if (dirty) { event.preventDefault(); event.returnValue = ''; } });
run(async () => {
  const data = await api('/sets');
  preferenceKey = `comment-lab:${data.corpus_id}:active`;
  $('classifier-set').innerHTML = data.sets.map(s => `<option value="${s.id}">${esc(s.name)} (+${s.count} / −${s.negative_count})</option>`).join('');
  if (!data.sets.length) {
    document.querySelector('.workbench').hidden = true;
    $('draft-status').textContent = 'Create an example set in Corpus first.';
    $('save-draft').hidden = true; $('compile-draft').hidden = true;
    return;
  }
  const requested = Number(new URLSearchParams(location.search).get('set')) || Number(localStorage.getItem(preferenceKey));
  await loadSet(data.sets.find(s => s.id === requested)?.id ?? data.sets[0].id);
});
