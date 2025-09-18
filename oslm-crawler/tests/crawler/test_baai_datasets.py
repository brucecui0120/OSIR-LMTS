from oslm_crawler.crawler.baai_data import BAAIDataPage, BAAIDataInfo


def test_baai_data_page():
    page = BAAIDataPage()
    res = page.scrape()
    assert isinstance(res, list)
    assert len(res) > 0
    print(len(res))
    assert isinstance(res[0], BAAIDataInfo)
