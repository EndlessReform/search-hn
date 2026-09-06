# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.28,<1"]
# ///
"""Small deployed API check. Run with uv run smoke.py HTTPS_ORIGIN."""
import asyncio
import json
import sys

import httpx


async def main():
    origin = sys.argv[1].rstrip('/')
    body = {'model':'pplx-embed-v1-0.6b','input':['database indexes'],'encoding_format':'float'}
    async with httpx.AsyncClient(timeout=40) as client:
        for path in ['/llms.txt','/embeddings/usage.md','/embeddings/healthz','/embeddings/readyz']:
            response = await client.get(origin+path)
            response.raise_for_status()
        response = await client.post(origin+'/embeddings/v1/embeddings', json=body)
        response.raise_for_status()
        result=response.json()
        vector=result['data'][0]['embedding']
        assert len(vector)==1024 and any(vector)
        assert all(isinstance(v,int) and -128<=v<=127 for v in vector)
        assert response.headers['X-Embedding-Recipe']==result['embedding_recipe']
        raw=await client.post(origin+'/vllm/embeddings/v1/embeddings',json=body)
        raw.raise_for_status()
        assert any(v!=int(v) for v in raw.json()['data'][0]['embedding'])
        invalid=await client.post(origin+'/embeddings/v1/embeddings',json=body,
                                  headers={'X-Embedding-Workload':'invalid'})
        assert invalid.status_code==400
        # Short byte length but excessive tokens: must reject, never silently truncate.
        too_long=await client.post(origin+'/embeddings/v1/embeddings',json={**body,'input':'a '*3000})
        assert too_long.status_code==400, too_long.text
        load_body={**body,'input':['database indexing and storage '*100]*8}
        responses=await asyncio.gather(*[
            client.post(origin+'/embeddings/v1/embeddings',json=load_body)
            for _ in range(16)])
        assert all(r.status_code==200 for r in responses), [r.status_code for r in responses]
        print(json.dumps({'origin':origin,'docs_and_health':True,'proxy_integer_coordinates':True,
                          'raw_float_coordinates':True,'invalid_workload_status':invalid.status_code,
                          'over_token_limit_status':too_long.status_code,
                          'concurrent_requests':16,'successful_load_requests':len(responses),
                          'recipe':result['embedding_recipe']},indent=2))


asyncio.run(main())
