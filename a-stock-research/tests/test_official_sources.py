from __future__ import annotations

from typing import Any

from investresearch.data_layer.official_sources import (
    CnipaPatentSearchAdapter,
    CreditChinaSearchAdapter,
    NationalEnterpriseCreditSearchAdapter,
    OfficialSourceRegistry,
)


class _FakeResponse:
    def __init__(self, *, text: str = "", payload: Any = None) -> None:
        self.text = text
        self._payload = payload

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


def test_credit_china_adapter_parses_html_compliance_results() -> None:
    html = """
    <div class="result">
      <a href="/detail/1">示例科技股份有限公司行政处罚决定书</a>
      <p>2025年03月18日 因信息披露违规受到行政处罚。</p>
    </div>
    """

    def fake_request(method: str, url: str, **kwargs: Any) -> _FakeResponse:
        del method, url, kwargs
        return _FakeResponse(text=html)

    adapter = CreditChinaSearchAdapter(fake_request)
    records = adapter.search_company_events(company_name="示例科技", limit=3)

    assert len(records) == 1
    assert records[0]["source"] == "credit_china"
    assert records[0]["event_type"] == "administrative_penalty"
    assert records[0]["published_at"] == "2025-03-18"


def test_national_enterprise_credit_adapter_parses_business_abnormality() -> None:
    html = """
    <table>
      <tr>
        <td><a href="/corp/abnormal">示例制造有限公司经营异常名录信息</a></td>
        <td>2024年11月02日 因公示信息隐瞒真实情况被列入经营异常名录</td>
      </tr>
    </table>
    """

    def fake_request(method: str, url: str, **kwargs: Any) -> _FakeResponse:
        del method, url, kwargs
        return _FakeResponse(text=html)

    adapter = NationalEnterpriseCreditSearchAdapter(fake_request)
    records = adapter.search_company_events(company_name="示例制造", limit=3)

    assert len(records) == 1
    assert records[0]["source"] == "national_enterprise_credit"
    assert records[0]["event_type"] == "business_abnormality"


def test_cnipa_adapter_parses_json_patent_results() -> None:
    payload = {
        "data": [
            {
                "title": "示例科技一种高性能设备发明专利",
                "summary": "申请号: CN202410000001.1 专利号: ZL202410000001.1 发明专利 授权 申请人: 示例科技股份有限公司",
                "url": "https://ggfw.cnipa.gov.cn/detail/1",
                "published_at": "2025-01-08",
            }
        ]
    }

    def fake_request(method: str, url: str, **kwargs: Any) -> _FakeResponse:
        del method, url, kwargs
        return _FakeResponse(payload=payload, text="")

    adapter = CnipaPatentSearchAdapter(fake_request)
    records = adapter.search_company_events(company_name="示例科技", limit=3)

    assert len(records) == 1
    assert records[0]["source"] == "cnipa"
    assert records[0]["patent_type"] == "发明专利"
    assert records[0]["status"] == "授权"
    assert records[0]["application_no"] == "CN202410000001.1"
    assert records[0]["patent_no"] == "ZL202410000001.1"
    assert records[0]["assignee"] == "示例科技股份有限公司"


def test_official_registry_keeps_other_compliance_sources_when_one_source_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INVESTRESEARCH_CSRC_BASE_URL", "https://www.csrc.gov.cn/search/mock")

    def fake_request(method: str, url: str, **kwargs: Any) -> _FakeResponse:
        del method, kwargs
        if "creditchina" in url:
            raise RuntimeError("credit china unavailable")
        if "csrc.gov.cn" in url:
            return _FakeResponse(
                text="""
                <ul class="result-list">
                  <li>
                    <a href="/csrc/penalty">示例制造股份有限公司行政处罚决定书</a>
                    <span>2026-04-08</span>
                    <p>因信息披露违规收到行政处罚。</p>
                  </li>
                </ul>
                """
            )
        return _FakeResponse(
            text="""
            <table>
              <tr>
                <td><a href="/corp/abnormal">示例制造股份有限公司经营异常名录信息</a></td>
                <td>2026年03月02日 因公示信息隐瞒真实情况被列入经营异常名录</td>
              </tr>
            </table>
            """
        )

    registry = OfficialSourceRegistry(fake_request)
    records = registry.search_company_compliance_events(company_name="示例制造", limit=5)
    sources = {item["source"] for item in records}

    assert "credit_china" not in sources
    assert {"csrc", "national_enterprise_credit"} <= sources
