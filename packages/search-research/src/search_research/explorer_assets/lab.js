/* Small single-user workstation: SQLite owns labels; the browser owns draft controls. */
'use strict';
const $ = id => document.getElementById(id);
const state = {active: null, ids: new Set(), negatives: new Map(), sets: [], applied: null, data: null, searchData: null, view: 'results', busy: false, dirty: false, preferenceKey: null};
const noteDrafts = new Map();
const noteKey = id => `${state.active}:${id}`;
const escapeHTML = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

async function api(path, method = 'GET', body) {
  const options = {method};
  if (method !== 'GET') {
    options.headers = {'Content-Type': 'application/json'};
    options.body = JSON.stringify(body ?? {});
  }
  const response = await fetch('/api' + path, options);
  const data = await response.json();
  if (!response.ok) {
    const detail = Array.isArray(data.detail) ? data.detail.map(x => x.msg).join('; ') : data.detail;
    throw new Error(detail || `HTTP ${response.status}`);
  }
  return data;
}

async function run(action) {
  if (state.busy) return;
  state.busy = true;
  $('errors').hidden = true;
  document.body.classList.add('busy');
  $('status').textContent = 'WORKING...';
  // Prevent overlapping mutations or changing the active set during a request.
  const controls = [...document.querySelectorAll('button,input,select,textarea')];
  const disabled = controls.map(x => x.disabled);
  controls.forEach(x => { x.disabled = true; });
  try {
    await action();
    $('status').textContent = noteDrafts.size ? 'READY / UNSAVED NOTE DRAFTS' : 'READY / ALL LABELS SAVED';
  } catch (error) {
    $('errors').textContent = error.message;
    $('errors').hidden = false;
    $('status').textContent = 'ERROR / REQUEST FAILED';
  } finally {
    controls.forEach((x, i) => { x.disabled = disabled[i]; });
    state.busy = false;
    document.body.classList.remove('busy');
    syncControls();
  }
}

function markDirty() {
  state.dirty = true;
  $('stale').hidden = !state.applied;
}

function syncControls() {
  const mode = $('mode').value;
  $('phrase').disabled = mode !== 'text';
  $('hide-positives').disabled = mode === 'text';
  $('positive-exclusion-help').textContent = mode === 'text' ?
    'Free-text search always shows positives. This checkbox applies only to mean queries.' :
    'On by default for mean queries. Positives disappear immediately as you label them; pages may get shorter.';
  $('phrase').classList.toggle('unused', mode !== 'text');
  $('phrase-unused').hidden = mode === 'text';
  $('phrase-help').textContent = mode === 'text' ? 'Used as the text query.' :
    'Ignored in mean modes. The phrase is not blended into the query.';
  $('background-controls').disabled = mode !== 'corrected';
  $('set-tools').hidden = !state.active;
  document.querySelectorAll('.positive-button,.negative-button,.save-note').forEach(x => { x.disabled = !state.active; });
  if (state.data) {
    $('previous').disabled = state.data.page <= 1;
    $('next').disabled = state.data.page * state.data.page_size >= state.data.total;
    $('last').disabled = $('next').disabled;
  }
}

async function loadSets() {
  const info = await api('/sets');
  state.sets = info.sets;
  state.preferenceKey = `comment-lab:${info.corpus_id}:active`;
  $('corpus-name').textContent = `${info.corpus_name} / ${info.comments.toLocaleString()} comments`;
  $('sets').innerHTML = info.sets.length ? info.sets.map(s =>
    `<button class="set-item ${s.id === state.active ? 'active' : ''}" data-set="${s.id}" aria-pressed="${s.id === state.active}">${escapeHTML(s.name)}<span>[+${s.count} / −${s.negative_count}]</span></button>`
  ).join('') : '<p class="footnote" style="padding:8px">No example sets yet.</p>';
  const selected = info.sets.find(s => s.id === state.active);
  $('set-name').value = selected ? selected.name : '';
}

function setMembership(data) {
  state.ids = new Set(data.comment_ids);
  state.negatives = new Map((data.negatives || []).map(n => [n.comment_id, n.note]));
  document.querySelectorAll('.comment-card').forEach(card => {
    const positive = state.ids.has(Number(card.dataset.comment));
    // Hide new labels immediately without compacting the applied ranking:
    // subsequent pages keep their original boundaries while collecting.
    card.hidden = state.view === 'results' && state.applied?.mode !== 'text' && Boolean(state.applied?.hide_positives) && positive;
    card.classList.toggle('positive', positive);
    const button = card.querySelector('.positive-button');
    button.textContent = positive ? '✓ POSITIVE / REMOVE' : '+ ADD POSITIVE';
    button.setAttribute('aria-pressed', String(positive));
    const negative = state.negatives.has(Number(card.dataset.comment));
    card.classList.toggle('negative', negative);
    const negativeButton = card.querySelector('.negative-button');
    if (negativeButton) {
      negativeButton.textContent = negative ? '− NEGATIVE / REMOVE' : '− ADD NEGATIVE';
      negativeButton.setAttribute('aria-pressed', String(negative));
      card.querySelector('.negative-note').hidden = !negative;
      const key = noteKey(Number(card.dataset.comment));
      card.querySelector('textarea').value = noteDrafts.get(key) ?? state.negatives.get(Number(card.dataset.comment)) ?? '';
      card.querySelector('.note-status').textContent = noteDrafts.has(key) ? 'Unsaved' : 'Saved';
    }
  });
}

async function selectSet(id) {
  state.active = id;
  const data = await api(`/sets/${id}`);
  setMembership(data);
  localStorage.setItem(state.preferenceKey, String(id));
  await loadSets();
  markDirty();
  if (state.view === 'set') render(data, true);
}

function render(data, setView = false) {
  state.data = data;
  state.view = setView ? 'set' : 'results';
  $('view-title').textContent = setView ? `02 / ${data.name} / POSITIVES + NEGATIVES` : '02 / CORPUS BROWSER';
  $('result-count').textContent = `${data.total.toLocaleString()} ${setView ? 'selected' : 'matches'}`;
  $('back-results').hidden = !setView;
  $('results').innerHTML = data.results.length ? data.results.map((row, i) => {
    const positive = state.ids.has(row.comment_id);
    const heading = setView && (i === 0 || data.results[i - 1].label !== row.label)
      ? `<h3 class="label-group">${row.label === 'positive' ? 'POSITIVES' : 'NEGATIVES'} / ${row.label === 'positive' ? data.count : data.negative_count}</h3>` : '';
    return `${heading}<article class="comment-card ${positive ? 'positive' : ''}" data-comment="${row.comment_id}">
      <div class="comment-meta"><span>#${(data.page - 1) * data.page_size + i + 1} / <a href="https://news.ycombinator.com/item?id=${row.comment_id}" target="_blank" rel="noopener">${row.comment_id}</a> / ${escapeHTML(row.author || 'unknown')} ${escapeHTML(row.comment_day || '')}</span>
      <span>${setView ? row.label.toUpperCase() : `CHUNK ${row.chunk} / <span class="score">${row.score.toFixed(5)}</span>`}</span></div>
      <div class="comment-text">${escapeHTML(row.text)}</div>
      <div class="comment-actions"><button class="positive-button" aria-pressed="${positive}">${positive ? '✓ POSITIVE / REMOVE' : '+ ADD POSITIVE'}</button>
      <button class="negative-button">− ADD NEGATIVE</button>
      <button class="parent-toggle" aria-expanded="false" aria-controls="parent-${row.comment_id}">Expand parent comment</button>
      ${row.story_id ? `<a class="story-link" href="https://news.ycombinator.com/item?id=${row.story_id}" target="_blank" rel="noopener">${escapeHTML(row.story_title || 'Open parent story')} ↗</a>` : ''}</div>
      <form class="negative-note" hidden><label for="note-${row.comment_id}">Why negative? (optional)</label>
      <textarea id="note-${row.comment_id}" rows="3" placeholder="e.g. Mentions the category without instantiating it"></textarea>
      <button class="save-note">Save note</button><span class="note-status footnote">Saved</span></form>
      <section class="parent-context" id="parent-${row.comment_id}" data-position="${(data.page - 1) * data.page_size + i + 1}" hidden></section></article>`;
  }).join('') : '<div class="empty"><div class="ascii">[ ∅ ]</div><h3>NO COMMENTS IN THIS VIEW</h3><p>Add labels from search, or adjust your query and cutoff.</p></div>';
  $('pagination').hidden = data.total === 0;
  const pages = Math.max(1, Math.ceil(data.total / data.page_size));
  $('page-number').value = data.page;
  $('page-number').max = pages;
  $('last').textContent = pages;
  setMembership({comment_ids: [...state.ids], negatives: [...state.negatives].map(([comment_id, note]) => ({comment_id, note}))});
  syncControls();
}

function draft() {
  return {mode: $('mode').value, q: $('phrase').value, set_id: state.active,
    gamma: Number($('gamma').value), baseline_size: Number($('baseline-size').value),
    seed: Number($('seed').value), min_score: Number($('min-score').value),
    page_size: Number($('page-size').value), hide_positives: $('hide-positives').checked, page: 1};
}

async function search(parameters, paging = false) {
  const data = await api('/experiment', 'POST', parameters);
  state.applied = {...parameters, query_positive_ids: data.query_positive_ids};
  state.searchData = data;
  if (!paging) state.dirty = false;
  const membership = state.active === parameters.set_id ?
    {comment_ids: data.positive_ids, negatives: data.negatives} :
    (state.active ? await api(`/sets/${state.active}`) : {comment_ids: []});
  setMembership(membership);
  render(data);
  $('stale').hidden = !state.dirty;
  const name = state.sets.find(s => s.id === parameters.set_id)?.name || 'no set';
  $('applied').textContent = parameters.mode === 'text' ? `APPLIED: text / ${parameters.q}` :
    `APPLIED: ${name} / ${parameters.mode} / γ=${data.diagnostics.gamma} / n=${parameters.baseline_size} / seed=${parameters.seed}`;
  $('diagnostics').textContent = `${data.engine}\nRANK: ${data.ranking_seconds.toFixed(3)}s / CACHE: ${data.cached ? 'HIT' : 'MISS'}\nREQUEST: ${data.request_seconds.toFixed(3)}s\nPOSITIVES: ${data.positive_ids.length}`;
  $('diagnostics').style.whiteSpace = 'pre-line';
}

async function viewSet(page = 1) {
  const data = await api(`/sets/${state.active}?page=${page}`);
  setMembership(data);
  render(data, true);
}

$('create-set').addEventListener('submit', event => {
  event.preventDefault();
  const name = $('new-name').value;
  run(async () => {
    const data = await api('/sets', 'POST', {name});
    $('new-name').value = '';
    await selectSet(data.id);
  });
});
$('sets').addEventListener('click', event => {
  const button = event.target.closest('[data-set]');
  if (button) run(() => selectSet(Number(button.dataset.set)));
});
$('rename').addEventListener('click', () => run(async () => {
  await api(`/sets/${state.active}`, 'PATCH', {name: $('set-name').value});
  await loadSets();
  if (state.view === 'set') await viewSet(state.data.page);
}));
$('delete-set').addEventListener('click', () => {
  if (!confirm(`Delete "${$('set-name').value}" and all its labels and notes?`)) return;
  run(async () => {
    await api(`/sets/${state.active}`, 'DELETE');
    state.active = null;
    setMembership({comment_ids: []});
    await loadSets();
    markDirty();
    if (state.view === 'set') {
      state.view = 'results'; state.data = null;
      $('results').innerHTML = '<div class="empty">Set deleted. Apply a query to continue.</div>';
      $('view-title').textContent = '02 / CORPUS BROWSER';
      $('result-count').textContent = '';
      $('pagination').hidden = true;
      $('back-results').hidden = true;
    }
  });
});
$('view-set').addEventListener('click', () => run(() => viewSet()));
$('back-results').addEventListener('click', () => run(async () => {
  if (state.searchData) render(state.searchData);
  else {
    state.view = 'results'; state.data = null;
    $('view-title').textContent = '02 / CORPUS BROWSER';
    $('result-count').textContent = '';
    $('results').innerHTML = '<div class="empty">Enter a phrase and apply a query to start searching.</div>';
    $('back-results').hidden = true;
    $('pagination').hidden = true;
    $('mode').value = 'text';
  }
}));
$('results').addEventListener('click', event => {
  const button = event.target.closest('.positive-button,.negative-button');
  if (!button || !state.active) return;
  const id = Number(button.closest('[data-comment]').dataset.comment);
  run(async () => {
    const negative = button.classList.contains('negative-button');
    const wasPositive = state.ids.has(id);
    const data = await api(`/sets/${state.active}/${negative ? 'negatives' : 'members'}/${id}`,
      (negative ? state.negatives.has(id) : wasPositive) ? 'DELETE' : 'PUT');
    noteDrafts.delete(noteKey(id));
    if (!negative || wasPositive) markDirty();
    setMembership(data);
    await loadSets();
    if (state.view === 'set') await viewSet(Math.max(1, Math.min(state.data.page, Math.ceil((data.count + data.negative_count) / 50))));
  });
});
$('results').addEventListener('click', event => {
  const button = event.target.closest('.parent-toggle');
  if (!button) return;
  const card = button.closest('[data-comment]');
  const context = card.querySelector('.parent-context');
  run(async () => {
    if (button.getAttribute('aria-expanded') === 'true') {
      context.hidden = true;
      button.setAttribute('aria-expanded', 'false');
      button.textContent = 'Expand parent comment';
      return;
    }
    if (!context.childElementCount) {
      const data = await api(`/comments/${card.dataset.comment}/parent`);
      const link = data.parent_id ? `<a href="https://news.ycombinator.com/item?id=${data.parent_id}" target="_blank" rel="noopener">${data.parent_id} ↗</a>` : '';
      const messages = {story: 'Top-level comment: its parent is the story.',
        outside_slice: 'Parent text is not available in this frozen slice.',
        unknown: 'This slice has no parent ID for this comment.'};
      context.innerHTML = `<div class="comment-meta"><span>#${context.dataset.position}+p / PARENT / ${link}</span><span>UNRANKED CONTEXT</span></div>` +
        (data.status === 'available' ?
          `<div class="comment-text"><small>${escapeHTML(data.parent.author || 'unknown')}</small>
${escapeHTML(data.parent.text)}</div>` :
          `<p class="comment-text">${messages[data.status]}</p>`);
    }
    context.hidden = false;
    button.setAttribute('aria-expanded', 'true');
    button.textContent = 'Collapse parent comment';
  });
});
$('results').addEventListener('input', event => {
  const form = event.target.closest('.negative-note');
  if (form) {
    noteDrafts.set(noteKey(Number(form.closest('[data-comment]').dataset.comment)), event.target.value);
    form.querySelector('.note-status').textContent = 'Unsaved';
    $('status').textContent = 'UNSAVED NOTE DRAFT';
  }
});
$('results').addEventListener('submit', event => {
  const form = event.target.closest('.negative-note');
  if (!form) return;
  event.preventDefault();
  const id = Number(form.closest('[data-comment]').dataset.comment);
  const note = form.querySelector('textarea').value;
  run(async () => {
    await api(`/sets/${state.active}/negatives/${id}`, 'PUT', {note});
    state.negatives.set(id, note);
    noteDrafts.delete(noteKey(id));
    form.querySelector('.note-status').textContent = 'Saved';
  });
});
$('query-form').addEventListener('submit', event => {
  event.preventDefault();
  const parameters = draft();
  run(() => search(parameters));
});
$('query-form').addEventListener('input', () => { markDirty(); syncControls(); });
$('mode').addEventListener('change', syncControls);
$('gamma-slider').addEventListener('input', () => { $('gamma').value = $('gamma-slider').value; });
$('gamma').addEventListener('input', () => { $('gamma-slider').value = $('gamma').value; });
$('resample').addEventListener('click', () => {
  $('seed').value = crypto.getRandomValues(new Uint32Array(1))[0]; markDirty();
});
async function goToPage(page) {
  if (state.view === 'set') await viewSet(page);
  else await search({...state.applied, page}, true);
}

$('page-jump').addEventListener('submit', event => {
  event.preventDefault();
  if (!$('page-jump').reportValidity()) return;
  const page = Number($('page-number').value);
  run(() => goToPage(page));
});

for (const [id, offset] of [['previous', -1], ['next', 1], ['last', 0]]) {
  $(id).addEventListener('click', () => run(async () => {
    const page = id === 'last' ? Math.max(1, Math.ceil(state.data.total / state.data.page_size)) : state.data.page + offset;
    await goToPage(page);
  }));
}
run(async () => {
  await loadSets();
  const saved = Number(localStorage.getItem(state.preferenceKey));
  if (state.sets.length) await selectSet(state.sets.find(s => s.id === saved)?.id ?? state.sets[0].id);
  const query = new URLSearchParams(location.search).get('q');
  if (query) { $('phrase').value = query; await search(draft()); }
});

window.addEventListener('beforeunload', event => {
  if (noteDrafts.size) { event.preventDefault(); event.returnValue = ''; }
});
