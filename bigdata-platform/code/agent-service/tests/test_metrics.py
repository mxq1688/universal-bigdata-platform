"""
Agent Service — Prometheus 指标 单元测试
覆盖: Counter / Gauge / Histogram / generate_metrics / 装饰器
"""
import sys
import time
import pytest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ====================== Counter 测试 ======================

class TestCounter:

    def test_counter_inc(self):
        from agent.metrics import _Counter
        c = _Counter("test_counter", "Test counter")
        c.inc()
        c.inc()
        c.inc(3)
        output = c.collect()
        assert "test_counter 5" in output

    def test_counter_with_labels(self):
        from agent.metrics import _Counter
        c = _Counter("requests_total", "Requests", labels=["status"])
        c.inc(status="success")
        c.inc(status="success")
        c.inc(status="error")
        output = c.collect()
        assert 'status="success"' in output
        assert 'status="error"' in output

    def test_counter_collect_format(self):
        from agent.metrics import _Counter
        c = _Counter("my_counter", "My help text")
        c.inc()
        output = c.collect()
        assert "# HELP my_counter My help text" in output
        assert "# TYPE my_counter counter" in output

    def test_counter_empty(self):
        from agent.metrics import _Counter
        c = _Counter("empty_counter", "Empty")
        output = c.collect()
        assert "empty_counter 0" in output


# ====================== Gauge 测试 ======================

class TestGauge:

    def test_gauge_set(self):
        from agent.metrics import _Gauge
        g = _Gauge("test_gauge", "Test gauge")
        g.set(42)
        output = g.collect()
        assert "42" in output

    def test_gauge_inc_dec(self):
        from agent.metrics import _Gauge
        g = _Gauge("active", "Active")
        g.inc()
        g.inc()
        g.dec()
        output = g.collect()
        assert "active 1" in output

    def test_gauge_with_labels(self):
        from agent.metrics import _Gauge
        g = _Gauge("info", "Info", labels=["version", "env"])
        g.set(1, version="1.0", env="prod")
        output = g.collect()
        assert 'version="1.0"' in output
        assert 'env="prod"' in output

    def test_gauge_collect_format(self):
        from agent.metrics import _Gauge
        g = _Gauge("my_gauge", "My gauge help")
        g.set(100)
        output = g.collect()
        assert "# HELP my_gauge My gauge help" in output
        assert "# TYPE my_gauge gauge" in output


# ====================== Histogram 测试 ======================

class TestHistogram:

    def test_histogram_observe(self):
        from agent.metrics import _Histogram
        h = _Histogram("duration", "Duration", buckets=(1, 5, 10, float("inf")))
        h.observe(0.5)
        h.observe(3)
        h.observe(7)
        h.observe(15)

        output = h.collect()
        assert "duration_sum" in output
        assert "duration_count 4" in output

    def test_histogram_buckets_cumulative(self):
        from agent.metrics import _Histogram
        h = _Histogram("latency", "Latency", buckets=(1, 5, 10, float("inf")))
        h.observe(0.5)  # fits in 1, 5, 10, inf
        h.observe(3)    # fits in 5, 10, inf
        h.observe(7)    # fits in 10, inf

        output = h.collect()
        # bucket le=1: 1 (only 0.5 <= 1)
        assert 'le="1"} 1' in output
        # The histogram uses cumulative counts, so each observe increments
        # all matching buckets. After 3 observations, total count should be 3
        assert "latency_count 3" in output
        # Sum should be 10.5
        assert "latency_sum 10.5" in output

    def test_histogram_sum(self):
        from agent.metrics import _Histogram
        h = _Histogram("test", "Test", buckets=(10, float("inf")))
        h.observe(1)
        h.observe(2)
        h.observe(3)
        output = h.collect()
        assert "test_sum 6" in output

    def test_histogram_collect_format(self):
        from agent.metrics import _Histogram
        h = _Histogram("req_duration", "Request duration")
        output = h.collect()
        assert "# HELP req_duration Request duration" in output
        assert "# TYPE req_duration histogram" in output


# ====================== generate_metrics 测试 ======================

class TestGenerateMetrics:

    def test_generate_metrics_text(self):
        from agent.metrics import generate_metrics
        output = generate_metrics()
        assert isinstance(output, str)
        assert "agent_analysis_requests_total" in output
        assert "agent_active_analyses" in output
        assert "agent_analysis_duration_seconds" in output

    def test_generate_metrics_has_all_metrics(self):
        from agent.metrics import generate_metrics, ALL_METRICS
        output = generate_metrics()
        for m in ALL_METRICS:
            assert m.name in output, f"Missing metric: {m.name}"


# ====================== 装饰器测试 ======================

class TestDecorators:

    def test_track_analysis_success(self):
        from agent.metrics import track_analysis, ANALYSIS_REQUESTS, ACTIVE_ANALYSES

        @track_analysis
        def dummy_analysis():
            return MagicMock(success=True)

        result = dummy_analysis()
        # 不应抛异常
        assert result is not None

    def test_track_analysis_error(self):
        from agent.metrics import track_analysis, ANALYSIS_ERRORS

        @track_analysis
        def failing_analysis():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            failing_analysis()

    def test_track_sql_success(self):
        from agent.metrics import track_sql

        @track_sql
        def dummy_sql():
            return [{"id": 1}]

        result = dummy_sql()
        assert result == [{"id": 1}]

    def test_track_sql_error(self):
        from agent.metrics import track_sql

        @track_sql
        def failing_sql():
            raise ConnectionError("DB down")

        with pytest.raises(ConnectionError):
            failing_sql()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
