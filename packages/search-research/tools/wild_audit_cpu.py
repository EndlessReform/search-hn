# /// script
# requires-python = ">=3.13"
# dependencies = ["numpy>=2.4", "torch>=2.8", "scikit-learn>=1.7", "xgboost-cpu>=3.0"]
# [tool.uv.sources]
# torch = { index = "pytorch-cpu" }
# [[tool.uv.index]]
# name = "pytorch-cpu"
# url = "https://download.pytorch.org/whl/cpu"
# explicit = true
# ///
"""Converged L2 logistic regression on the fixed MLP experiment splits."""

import json
from pathlib import Path
import numpy as np
from xgboost import XGBClassifier
from comment_linear_probe import embeddings, readonly
root=Path('data/comment-2025'); out=Path('data/probes/books-wild-audit-10k-v1');out.mkdir(exist_ok=False)
excluded=set()
for path in [Path('data/probes/books-mlp-v1/labels.jsonl'), *Path('data/probes/books-xgb-sweep-v1').glob('*-fixture.jsonl')]:
 excluded.update(json.loads(l)['comment_id'] for l in path.read_text().splitlines())
with readonly(root/'annotations.sqlite') as db:
 excluded.update(r[0] for r in db.execute('select distinct comment_id from rollout_picks'))
 anchor=json.loads(db.execute('select anchor_json from rollout_pools where id=1').fetchone()[0])
excluded.update(anchor['excluded_ids'])
with readonly(root/'index.sqlite') as db:
 ids=np.fromiter((r[0] for r in db.execute('select comment_id from comments order by comment_id')),dtype=np.int64)
 ids=np.random.default_rng(20260922).choice(ids[~np.isin(ids,list(excluded))],10000,replace=False)
 rows=[{'comment_id':int(i),'text':db.execute('select text from comments where comment_id=?',(int(i),)).fetchone()[0]} for i in ids]
 vec=np.load(root/'vectors.npy',mmap_mode='r'); q=np.array(anchor['query'],dtype=np.float32);q/=np.linalg.norm(q)
 cs=[]
 for row in rows:
  ix=[r[0] for r in db.execute('select vector_row from inputs where comment_id=? order by chunk',(row['comment_id'],))]
  a=vec[ix].astype(np.float32);a/=np.linalg.norm(a,axis=1,keepdims=True);cs.append(float((a@q).max()))
model=XGBClassifier();model.load_model('data/probes/books-xgb-sweep-v1/depth4_child1.ubj');xp=model.predict_proba(embeddings(root,rows).numpy())[:,1]
thresholds=json.loads(Path('data/probes/books-filter-comparison-v1/metrics.json').read_text())['thresholds']
counts={}
for r,c,x in zip(rows,cs,xp):
 r.update(centroid=c,xgboost=float(x))
 for target in ['0.99','0.95']:
  cp=c>=thresholds['centroid'][target]; xx=x>=thresholds['xgboost'][target]
  group='both_pass' if cp and xx else 'centroid_only' if cp else 'xgboost_only' if xx else 'both_reject'
  r['group'+target]=group;counts.setdefault(target,{});counts[target][group]=counts[target].get(group,0)+1
(out/'predictions.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
(out/'summary.json').write_text(json.dumps({'counts':counts,'thresholds':thresholds,'seed':20260922},indent=2))
print(json.dumps(counts))
