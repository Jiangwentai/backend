import httpx
from archive.questdb_source import QuestDBArchiveSource
from archive.schema import COLUMN_NAMES

def test_questdb_source_pages_and_converts_timestamps():
    requests=[]
    def handler(request):
        query=request.url.params["query"];requests.append(query);second="LIMIT 2,4" in query
        dataset=[] if second else [["2026-09-03T13:00:00.123456Z","2026-09-03T13:00:00.123456789Z"]+[None]*(len(COLUMN_NAMES)-2)]*2
        return httpx.Response(200,json={"columns":[{"name":name} for name in COLUMN_NAMES],"dataset":dataset})
    client=httpx.Client(transport=httpx.MockTransport(handler),base_url="http://questdb")
    source=QuestDBArchiveSource("http://questdb",batch_size=2,client=client);batches=list(source.iter_partition("SHFE","zn2610","20260904"));source.close()
    assert len(batches)==1 and len(batches[0])==2
    assert batches[0][0]["event_ts"]==1788440400123456 and batches[0][0]["recv_ts"]==1788440400123456789
    assert "LIMIT 0,2" in requests[0] and "LIMIT 2,4" in requests[1]
