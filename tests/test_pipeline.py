"""
Unit tests for pipeline items 31-34.

No external services required — Flink state, DB, and Kafka calls are all mocked.

Run with:
        pytest tests/test_pipeline.py -v

Item mapping:
    31  test_broadcast_state_refresh_applies_new_scores
    32  test_cep_home_loan_fires_on_3_category_txns_over_2100
    33  test_state_ttl_dedup_suppresses_second_offer_within_24h
    34  test_late_data_routed_to_side_output_and_flagged
"""

import json
import os
import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 0.  Stub pyflink BEFORE anything imports intent_detector.
#     The module-level Flink topology (KafkaSource, env.execute …) will
#     call mock objects and complete without starting a cluster.
# ---------------------------------------------------------------------------
class _Fluent:
    """Chainable object used to stub Flink builder/stream APIs."""

    def __getattr__(self, _name):
        return self

    def __call__(self, *args, **kwargs):
        return self


class _StateDesc:
    def __init__(self, *args, **kwargs):
        pass

    def enable_time_to_live(self, *args, **kwargs):
        pass


class _StateTtlBuilder(_Fluent):
    def build(self):
        return object()


class _StateTtlConfig:
    class UpdateType:
        OnCreateAndWrite = "OnCreateAndWrite"

    class StateVisibility:
        NeverReturnExpired = "NeverReturnExpired"

    @staticmethod
    def new_builder(*args, **kwargs):
        return _StateTtlBuilder()


def _module(name: str):
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


_module("pyflink")
common_mod = _module("pyflink.common")
serialization_mod = _module("pyflink.common.serialization")
datastream_mod = _module("pyflink.datastream")
_module("pyflink.datastream.connectors")
kafka_mod = _module("pyflink.datastream.connectors.kafka")
functions_mod = _module("pyflink.datastream.functions")
state_mod = _module("pyflink.datastream.state")
window_mod = _module("pyflink.datastream.window")

common_mod.Duration = _Fluent()
common_mod.Time = _Fluent()
common_mod.Types = _Fluent()
common_mod.WatermarkStrategy = _Fluent()
serialization_mod.SimpleStringSchema = _Fluent()

datastream_mod.OutputTag = lambda *args, **kwargs: object()


class _Env:
    @staticmethod
    def get_execution_environment():
        return _Fluent()


datastream_mod.StreamExecutionEnvironment = _Env

kafka_mod.KafkaOffsetsInitializer = _Fluent()
kafka_mod.KafkaRecordSerializationSchema = _Fluent()
kafka_mod.KafkaSink = _Fluent()
kafka_mod.KafkaSource = _Fluent()

functions_mod.KeyedBroadcastProcessFunction = object
functions_mod.KeyedProcessFunction = object


class _ProcessWindowFunction:
    class Context:
        pass


functions_mod.ProcessWindowFunction = _ProcessWindowFunction
functions_mod.RuntimeContext = object

state_mod.MapStateDescriptor = _StateDesc
state_mod.ValueStateDescriptor = _StateDesc
state_mod.StateTtlConfig = _StateTtlConfig

window_mod.SlidingEventTimeWindows = _Fluent()

# Stub psycopg2 so intent_detector imports cleanly.
if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = MagicMock()

# ---------------------------------------------------------------------------
# 1.  Add project directories to sys.path and import modules under test.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _d in (
    os.path.join(_PROJECT_ROOT, "Flinkjobs"),
    _PROJECT_ROOT,
):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import intent_detector as _id  # noqa: E402  (pyflink mocked above)

# ---------------------------------------------------------------------------
# 2.  Lightweight Flink state / context mocks.
# ---------------------------------------------------------------------------


class _ValueState:
    def __init__(self):
        self._v = None

    def value(self):
        return self._v

    def update(self, v):
        self._v = v

    def clear(self):
        self._v = None


class _MapState:
    def __init__(self):
        self._d: dict = {}

    def get(self, k):
        return self._d.get(k)

    def put(self, k, v):
        self._d[k] = v

    def contains(self, k):
        return k in self._d

    def remove(self, k):
        self._d.pop(k, None)


class _TimerService:
    def __init__(self, watermark: int):
        self._wm = watermark
        self.registered: list = []
        self.deleted: list = []

    def current_watermark(self):
        return self._wm

    def register_event_time_timer(self, t):
        self.registered.append(t)

    def delete_event_time_timer(self, t):
        self.deleted.append(t)


class _BroadcastState:
    def __init__(self):
        self._d: dict = {}

    def get(self, k):
        return self._d.get(k)

    def put(self, k, v):
        self._d[k] = v


class _ProcessCtx:
    """Minimal context passed to process_element / process_broadcast_element."""

    def __init__(self, watermark: int = -(2**63)):
        self.broadcast_state = _BroadcastState()
        self._timer_svc = _TimerService(watermark)
        self.side_outputs: list = []
        self._key: str = ""

    def get_broadcast_state(self, _desc):
        return self.broadcast_state

    def timer_service(self):
        return self._timer_svc

    def output(self, _tag, value):
        self.side_outputs.append(value)

    def get_current_key(self):
        return self._key


def _make_runtime_ctx() -> MagicMock:
    """RuntimeContext mock: returns a fresh _ValueState per get_state() call."""
    ctx = MagicMock()
    ctx.get_index_of_this_subtask.return_value = 0
    ctx.get_map_state.return_value = _MapState()
    ctx.get_state.side_effect = lambda _desc: _ValueState()
    return ctx


# ---------------------------------------------------------------------------
# 3.  Shared fixture: IntentOfferFunction with DB calls mocked.
# ---------------------------------------------------------------------------


@pytest.fixture()
def intent_func():
    """Return a fully-opened IntentOfferFunction whose DB calls hit MagicMocks."""
    mock_conn = MagicMock()
    # MagicMock supports the context-manager protocol by default, so
    # `with self.conn.cursor() as cur: cur.execute(...)` works fine.
    with patch("psycopg2.connect", return_value=mock_conn):
        func = _id.IntentOfferFunction()
        func.open(_make_runtime_ctx())
        yield func


# ---------------------------------------------------------------------------
# 4.  Tiny helpers
# ---------------------------------------------------------------------------


def _txn(customer_id: str, ts_ms: int, category: str, amount: float) -> dict:
    return {
        "kind": "txn",
        "customer_id": customer_id,
        "event_ts_ms": ts_ms,
        "merchant_category": category,
        "transaction_type": "debit",
        "amount": amount,
        "has_credit_card": 0,
        "has_fixed_deposit": 0,
    }


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# TEST 31 — Broadcast State refresh: updated scores applied without restart
# ---------------------------------------------------------------------------


def test_broadcast_state_refresh_applies_new_scores(intent_func):
    """process_broadcast_element() writes the score into broadcast state.
    A subsequent transaction event for that customer uses the new base score
    (not the default 45.0), proving scores update within the same job run."""
    ctx = _ProcessCtx()
    now_ms = _now_ms()

    # Publish a fresh score payload — simulates broadcast_publisher.py writing
    # to the propensity_scores.broadcast Kafka topic and Flink consuming it.
    score_payload = {
        "customer_id": "CUST_BCAST",
        "scores": {
            "home_loan": 68.0,
            "credit_card": 55.0,
            "fixed_deposit": 50.0,
            "personal_loan": 48.0,
        },
        "score_ts": "2026-04-18T11:00:00Z",
    }
    intent_func.process_broadcast_element(score_payload, ctx)

    # --- Verify broadcast state was written correctly ---
    raw = ctx.broadcast_state.get("CUST_BCAST")
    assert raw is not None, "Broadcast state was not updated"
    stored = json.loads(raw)
    assert stored["scores"]["home_loan"] == 68.0, "home_loan score mismatch"
    assert stored["scores"]["credit_card"] == 55.0, "credit_card score mismatch"

    # --- Verify the next offer for that customer uses the broadcast base score ---
    # 3 home-category txns to trigger the CEP session ($2,100 total)
    ctx._key = "CUST_BCAST"
    emitted = []
    for cat, amt in [
        ("home_improvement",   700.0),
        ("furniture",          700.0),
        ("kitchen_appliances", 700.0),
    ]:
        emitted.extend(list(intent_func.process_element(_txn("CUST_BCAST", now_ms, cat, amt), ctx)))

    home_loan_offers = [
        json.loads(r) for r in emitted if json.loads(r).get("product") == "home_loan"
    ]
    assert home_loan_offers, "No home_loan offer after broadcast state was populated"

    offer = home_loan_offers[0]
    assert offer["base_score"] == 68.0, (
        f"Offer used base_score={offer['base_score']} instead of broadcast value 68.0 — "
        "broadcast state was not applied"
    )
    # composite = base(68) + boost(28) = 96, well above threshold
    assert offer["final_score"] == 96.0


# ---------------------------------------------------------------------------
# TEST 32 — CEP home loan intent fires on 3 home-category txns > $2,100
# ---------------------------------------------------------------------------


def test_cep_home_loan_fires_on_3_category_txns_over_2100(intent_func):
    """3 transactions in the home-improvement/furniture/kitchen_appliances
    categories totalling $2,100 within a 7-day session window must trigger
    a home_loan RealTimeOffer.  The '3-second' latency is a streaming runtime
    guarantee; this unit test validates the CEP logic fires correctly."""
    now_ms = _now_ms()
    # Watermark just before events → none are late
    ctx = _ProcessCtx(watermark=now_ms - 10_000)
    ctx._key = "CUST_CEP"
    # Seed broadcast: base(60) + boost(28) = 88 > threshold(75) → offer will emit
    ctx.broadcast_state.put(
        "CUST_CEP", json.dumps({"scores": {"home_loan": 60.0}})
    )

    emitted = []
    # Inject the three transactions that build the $2,100 session
    for cat, amt in [
        ("home_improvement",   700.0),   # session spend = $700
        ("furniture",          700.0),   # session spend = $1,400
        ("kitchen_appliances", 700.0),   # session spend = $2,100 → fires
    ]:
        results = list(
            intent_func.process_element(_txn("CUST_CEP", now_ms, cat, amt), ctx)
        )
        emitted.extend(results)

    home_loan_offers = [
        json.loads(r) for r in emitted if json.loads(r).get("product") == "home_loan"
    ]
    assert home_loan_offers, (
        "No home_loan offer emitted after 3 home-category txns totalling $2,100. "
        "CEP session window did not fire — check HOME_LOAN_CATEGORIES set and "
        "_update_home_session spend threshold."
    )
    offer = home_loan_offers[0]
    assert offer["final_score"] > _id.THRESHOLD, (
        f"Offer final_score {offer['final_score']} did not exceed THRESHOLD {_id.THRESHOLD}"
    )
    assert offer["trigger_event"] == "home_improvement_session_spend_gt_2000"


# ---------------------------------------------------------------------------
# TEST 33 — State TTL deduplication: second offer within 24h is suppressed
# ---------------------------------------------------------------------------


def test_state_ttl_dedup_suppresses_second_offer_within_24h(intent_func):
    """Sending a second home-improvement session for the same customer within
    the 24-hour deduplication window must produce zero additional offers.
    The MapState TTL (30 days) prevents re-offers; the 24-hour guard inside
    process_element is the first line of defence validated here."""
    now_ms = _now_ms()
    ctx = _ProcessCtx(watermark=now_ms - 10_000)
    ctx._key = "CUST_DEDUP"
    ctx.broadcast_state.put(
        "CUST_DEDUP", json.dumps({"scores": {"home_loan": 60.0}})
    )

    def _fire_home_session() -> list:
        """Reset session state and send 3 txns that cross $2,100; return emitted offers."""
        intent_func._reset_home_session()
        emitted = []
        for cat, amt in [
            ("home_improvement",   800.0),
            ("furniture",          700.0),
            ("kitchen_appliances", 700.0),
        ]:
            emitted.extend(
                list(intent_func.process_element(_txn("CUST_DEDUP", now_ms, cat, amt), ctx))
            )
        return [json.loads(r) for r in emitted if json.loads(r).get("product") == "home_loan"]

    first_offers = _fire_home_session()
    assert first_offers, "First session produced no offer — test precondition failed"

    second_offers = _fire_home_session()
    assert not second_offers, (
        f"State TTL dedup failed — a second home_loan offer was emitted "
        f"within the 24h dedup window: {second_offers}"
    )


# ---------------------------------------------------------------------------
# TEST 34 — Late data: event 15 min past watermark → side output, not CEP
# ---------------------------------------------------------------------------


def test_late_data_routed_to_side_output_and_flagged(intent_func):
    """A transaction whose event_ts is 15 minutes older than the current
    watermark (i.e., beyond the 10-minute allowed lateness window) must:
      1. Be emitted to the LATE_EVENT_TAG side output for manual review.
      2. Have is_late=True on any resulting intent event (audit stamp).
    This ensures the realtime_late_events table captures them for review."""
    now_ms = _now_ms()
    fifteen_min_ms = 15 * 60 * 1000
    late_ts_ms = now_ms - fifteen_min_ms

    # Watermark = now — the late event (15 min ago) is behind it
    ctx = _ProcessCtx(watermark=now_ms)
    ctx._key = "CUST_LATE"
    ctx.broadcast_state.put(
        "CUST_LATE", json.dumps({"scores": {"home_loan": 60.0}})
    )

    # A single large txn that would trigger a signal if not late
    late_txn = _txn("CUST_LATE", late_ts_ms, "home_improvement", 3000.0)
    list(intent_func.process_element(late_txn, ctx))

    # 1. Side output must contain the late event
    assert ctx.side_outputs, (
        "Late event (15 min behind watermark) was NOT routed to side output. "
        "Check is_late detection in process_element."
    )
    side_event = json.loads(ctx.side_outputs[0])
    assert side_event["customer_id"] == "CUST_LATE"
    assert side_event["event_ts_ms"] == late_ts_ms

    # 2. If an intent event was upserted, it must carry is_late=True
    # (checked indirectly: any offer yielded from this call is stamped)
    # We verify via the conn mock — the INSERT payload includes is_late
    insert_calls = intent_func.conn.cursor.return_value.__enter__.return_value.execute.call_args_list
    for call_obj in insert_calls:
        args = call_obj.args
        if args and len(args) > 1:
            params = args[1]
            # params[13] is is_late (14th positional arg in _upsert_intent_event)
            if isinstance(params, (list, tuple)) and len(params) >= 14:
                assert params[13] is True, (
                    f"Intent event from late txn was NOT stamped is_late=True. "
                    f"params[13]={params[13]}"
                )


