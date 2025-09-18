import jsonlines
from pathlib import Path
from oslm_crawler.pipeline.writers import JsonlineWriter
from oslm_crawler.pipeline.base import PipelineData

def test_jsonline_writer():
    tmp_path = Path(__file__).parent / 'tmp.jsonl'
    writer = JsonlineWriter(tmp_path, drop_keys=['repo_org_mapper'])
    inp = PipelineData({
        "abc": 123,
        "def": 567,
        "repo_org_mapper": {
            "123": "abc",
            "567": "def"
        }
    }, None, None)
    writer.parse_input(inp)
    next(writer.run())
    with jsonlines.open(tmp_path, 'r') as f:
        res = f.read()
        assert 'abc' in res.keys()
        assert 'def' in res.keys()
        assert 'repo_org_mapper' not in res.keys()
    tmp_path.unlink()
    