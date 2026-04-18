"""CrossVerificationEngine 单元测试."""

from investresearch.data_layer.cross_verify import CrossVerificationEngine
from investresearch.core.models import (
    CrossVerifiedMetric,
    IndustryCrossVerification,
    IndustryDataPoint,
    PeerCompany,
)


# ============================================================
# 工厂函数
# ============================================================


def _make_data_point(
    metric_name: str = "market_size",
    metric_value: float = 500.0,
    source_company: str = "公司A",
    source_company_code: str = "000001",
    source_type: str = "annual_report",
    consulting_firm: str = "",
) -> IndustryDataPoint:
    return IndustryDataPoint(
        metric_name=metric_name,
        metric_value=metric_value,
        metric_unit="亿元" if metric_name == "market_size" else "%",
        source_company=source_company,
        source_company_code=source_company_code,
        source_type=source_type,
        consulting_firm=consulting_firm,
        excerpt=f"行业规模约{metric_value}亿元",
    )


def _make_peer(code: str = "000001", name: str = "同业A") -> PeerCompany:
    return PeerCompany(
        stock_code=code,
        stock_name=name,
        industry_sw="白酒",
        industry_level="二级行业",
        market_cap=1e10,
        rank_in_industry=1,
    )


# ============================================================
# 测试 verify()
# ============================================================


class TestVerify:
    """CrossVerificationEngine.verify() 测试."""

    def test_empty_input_returns_empty(self) -> None:
        engine = CrossVerificationEngine()
        result = engine.verify([])
        assert result == []

    def test_single_source_insufficient(self) -> None:
        engine = CrossVerificationEngine()
        points = [_make_data_point(source_company="公司A")]
        result = engine.verify(points)
        assert len(result) == 1
        assert result[0].consistency_flag == "insufficient"
        assert result[0].source_count == 1

    def test_consistent_sources(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=480, source_company="公司A", source_company_code="001"),
            _make_data_point(metric_value=500, source_company="公司B", source_company_code="002"),
            _make_data_point(metric_value=520, source_company="公司C", source_company_code="003"),
        ]
        result = engine.verify(points)
        assert len(result) == 1
        metric = result[0]
        assert metric.consistency_flag == "consistent"
        assert metric.source_count == 3
        assert metric.mean_value == 500.0
        assert abs(metric.recommended_value - 500.0) < 1.0

    def test_divergent_sources(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=100, source_company="公司A", source_company_code="001"),
            _make_data_point(metric_value=500, source_company="公司B", source_company_code="002"),
            _make_data_point(metric_value=900, source_company="公司C", source_company_code="003"),
        ]
        result = engine.verify(points)
        assert len(result) == 1
        assert result[0].consistency_flag == "divergent"

    def test_consulting_source_preferred(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=500, source_company="公司A", source_company_code="001"),
            _make_data_point(
                metric_value=550,
                source_company="公司B",
                source_company_code="002",
                source_type="consulting",
                consulting_firm="IDC",
            ),
        ]
        result = engine.verify(points)
        assert len(result) == 1
        assert result[0].recommended_value == 550.0
        assert "IDC" in result[0].consulting_sources

    def test_deduplicate_same_company(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=500, source_company="公司A", source_company_code="001"),
            _make_data_point(metric_value=520, source_company="公司A", source_company_code="001"),
        ]
        result = engine.verify(points)
        assert len(result) == 1
        assert result[0].source_count == 1

    def test_multiple_metrics_grouped(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_name="market_size", metric_value=5000),
            _make_data_point(metric_name="cagr", metric_value=15.0),
            _make_data_point(metric_name="cr5", metric_value=45.0),
        ]
        result = engine.verify(points)
        assert len(result) == 3

    def test_metric_name_normalization(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            IndustryDataPoint(
                metric_name="市场规模",
                metric_value=500,
                metric_unit="亿元",
                source_company="公司A",
                source_company_code="001",
            ),
            IndustryDataPoint(
                metric_name="行业规模",
                metric_value=520,
                metric_unit="亿元",
                source_company="公司B",
                source_company_code="002",
            ),
        ]
        result = engine.verify(points)
        assert len(result) == 1
        assert result[0].metric_name == "market_size"
        assert result[0].source_count == 2


# ============================================================
# 测试 build_result()
# ============================================================


class TestBuildResult:
    """CrossVerificationEngine.build_result() 测试."""

    def test_full_result_structure(self) -> None:
        engine = CrossVerificationEngine()
        peers = [_make_peer(code=f"00000{i}") for i in range(3)]
        points = [
            _make_data_point(source_company=f"公司{i}", source_company_code=f"00000{i}")
            for i in range(3)
        ]
        result = engine.build_result(
            data_points=points,
            peers=peers,
            industry_name="白酒",
            industry_level="二级行业",
            target_stock_code="600519",
        )
        assert isinstance(result, IndustryCrossVerification)
        assert result.industry_name == "白酒"
        assert result.peer_count == 3
        assert result.target_stock_code == "600519"
        assert len(result.verified_metrics) >= 1
        assert 0 <= result.overall_confidence <= 1

    def test_overall_confidence_with_no_metrics(self) -> None:
        engine = CrossVerificationEngine()
        result = engine.build_result(
            data_points=[],
            peers=[],
            industry_name="空行业",
        )
        assert result.overall_confidence == 0.0


# ============================================================
# 测试统计量计算
# ============================================================


class TestStatistics:
    """统计量计算测试."""

    def test_mean_and_median(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=100, source_company="A", source_company_code="001"),
            _make_data_point(metric_value=200, source_company="B", source_company_code="002"),
            _make_data_point(metric_value=300, source_company="C", source_company_code="003"),
        ]
        result = engine.verify(points)
        assert len(result) == 1
        assert result[0].mean_value == 200.0
        assert result[0].median_value == 200.0
        assert result[0].min_value == 100.0
        assert result[0].max_value == 300.0

    def test_std_dev_two_sources(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=100, source_company="A", source_company_code="001"),
            _make_data_point(metric_value=200, source_company="B", source_company_code="002"),
        ]
        result = engine.verify(points)
        assert result[0].std_dev is not None
        assert result[0].std_dev > 0

    def test_confidence_increases_with_more_sources(self) -> None:
        engine = CrossVerificationEngine()
        # 2 sources
        points_2 = [
            _make_data_point(metric_value=500, source_company=f"C{i}", source_company_code=f"00{i}")
            for i in range(2)
        ]
        result_2 = engine.verify(points_2)

        # 5 sources
        points_5 = [
            _make_data_point(
                metric_value=500 + i * 2,
                source_company=f"C{i}",
                source_company_code=f"00{i}",
            )
            for i in range(5)
        ]
        result_5 = engine.verify(points_5)

        assert result_5[0].confidence_score >= result_2[0].confidence_score


# ============================================================
# 测试推荐值选择
# ============================================================


class TestRecommendedValue:
    """推荐值选择优先级测试."""

    def test_consulting_value_preferred(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=500, source_company="A", source_company_code="001"),
            _make_data_point(metric_value=600, source_company="B", source_company_code="002"),
            _make_data_point(
                metric_value=550,
                source_company="C",
                source_company_code="003",
                consulting_firm="Frost & Sullivan",
                source_type="consulting",
            ),
        ]
        result = engine.verify(points)
        assert result[0].recommended_value == 550.0

    def test_median_with_three_sources(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=100, source_company="A", source_company_code="001"),
            _make_data_point(metric_value=200, source_company="B", source_company_code="002"),
            _make_data_point(metric_value=300, source_company="C", source_company_code="003"),
        ]
        result = engine.verify(points)
        assert result[0].recommended_value == 200.0

    def test_mean_with_two_sources(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(metric_value=100, source_company="A", source_company_code="001"),
            _make_data_point(metric_value=200, source_company="B", source_company_code="002"),
        ]
        result = engine.verify(points)
        assert result[0].recommended_value == 150.0

    def test_single_value(self) -> None:
        engine = CrossVerificationEngine()
        points = [_make_data_point(metric_value=500, source_company="A", source_company_code="001")]
        result = engine.verify(points)
        assert result[0].recommended_value == 500.0


# ============================================================
# 测试 evidence_refs
# ============================================================


class TestEvidenceRefs:
    """证据引用构建测试."""

    def test_evidence_refs_built_from_points(self) -> None:
        engine = CrossVerificationEngine()
        points = [
            _make_data_point(
                metric_value=500,
                source_company="公司A",
                source_company_code="001",
            ),
            _make_data_point(
                metric_value=520,
                source_company="公司B",
                source_company_code="002",
                consulting_firm="IDC",
            ),
        ]
        result = engine.verify(points)
        assert len(result[0].evidence_refs) == 2
        assert result[0].evidence_refs[0].source == "001"
