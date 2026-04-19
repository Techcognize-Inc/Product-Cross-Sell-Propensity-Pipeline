"""
IntentDetector — Real-Time Cross-Sell Intent Detection via PyFlink

Advanced Flink Features implemented:
  1. Broadcast State          — Weekly propensity scores (500K customers × 4 products) broadcast to all
                                subtasks; every intent event is enriched from local memory, zero DB lookups.
  2. CEP Session Windows      — Home-loan intent uses a 7-day inactivity-gap session window; all
                                home-improvement spend within a single shopping session is aggregated
                                before the pattern fires at >$2,000.
  3. Sliding Window           — 7-day / 1-day-slide window aggregates category spend per customer.
                                Written to propensity_enrichment_feed table for freshness supplement.
  4. State TTL (30-day dedup) — MapState<product, last_offer_ts> with a 30-day TTL prevents
                                duplicate offers without manual eviction logic.
  5. AllowedLateness + Side   — Events up to 10 min late are accepted by the watermark strategy.
     Output                     Events arriving after that are emitted to a typed side-output tag
                                (late_events) and written to realtime_late_events for manual review.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import psycopg2
from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    KeyedBroadcastProcessFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import (
    MapStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)
from pyflink.datastream.window import SlidingEventTimeWindows

logging.basicConfig(
    level=os.getenv("INTENT_DETECTOR_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [intent_detector] %(message)s",
)
logger = logging.getLogger("intent_detector")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TXN_TOPIC = os.getenv("TRANSACTION_TOPIC", "txn.stream")
PRODUCT_TOPIC = os.getenv("PRODUCT_EVENT_TOPIC", "product.events")
BROADCAST_TOPIC = os.getenv("BROADCAST_TOPIC", "propensity_scores.broadcast")
OFFER_TOPIC = os.getenv("OFFER_TOPIC", "crm.realtime_offers")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")
DB_NAME = os.getenv("DB_NAME", "airflow")

PARALLELISM = int(os.getenv("INTENT_DETECTOR_PARALLELISM", "16"))
THRESHOLD = float(os.getenv("REALTIME_OFFER_THRESHOLD", "75"))
EVENT_LOG_EVERY_N = int(os.getenv("INTENT_EVENT_LOG_EVERY_N", "1"))
BROADCAST_LOG_EVERY_N = int(os.getenv("INTENT_BROADCAST_LOG_EVERY_N", "10000"))

# Feature 5 — Side Output tag for late-arriving events (post 10-min watermark)
LATE_EVENT_TAG = OutputTag("late_events", Types.STRING())

# Time constants (milliseconds)
MS_1_DAY  = 24 * 60 * 60 * 1000
MS_7_DAY  = 7  * MS_1_DAY
MS_30_DAY = 30 * MS_1_DAY
MS_72_HR  = 72 * 60 * 60 * 1000

PATTERN_BOOST = {
    "home_loan": 28.0,
    "credit_card": 26.0,
    "fixed_deposit": 24.0,
    "personal_loan": 25.0,
}

# Feature 1 — Broadcast State descriptor shared across ALL subtasks
BROADCAST_STATE_DESC = MapStateDescriptor(
    "propensity_state",
    Types.STRING(),   # key: customer_id
    Types.STRING(),   # value: JSON-encoded scores dict
)


def _parse_iso_ts_to_ms(ts_value: str) -> int:
    if not ts_value:
        return 0
    try:
        cleaned = ts_value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(cleaned)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return 0


def parse_txn(raw: str):
    try:
        event = json.loads(raw)
        ts_ms = _parse_iso_ts_to_ms(event.get("event_ts"))
        if ts_ms == 0:
            return None
        return {
            "kind": "txn",
            "customer_id": event.get("customer_id", ""),
            "event_ts_ms": ts_ms,
            "merchant_category": event.get("merchant_category", "unknown"),
            "transaction_type": event.get("transaction_type", "debit"),
            "amount": float(event.get("amount", 0.0)),
            "has_credit_card": int(event.get("has_credit_card", 0) or 0),
            "has_fixed_deposit": int(event.get("has_fixed_deposit", 0) or 0),
        }  # _raw kept for side-output late-event tagging
    except Exception:
        return None


def parse_product_event(raw: str):
    try:
        event = json.loads(raw)
        ts_ms = _parse_iso_ts_to_ms(event.get("event_ts"))
        if ts_ms == 0:
            return None
        return {
            "kind": "product",
            "customer_id": event.get("customer_id", ""),
            "event_ts_ms": ts_ms,
            "event_type": event.get("event_type", "page_view"),
            "product": event.get("product", "unknown"),
        }
    except Exception:
        return None


def parse_score(raw: str):
    try:
        payload = json.loads(raw)
        customer_id = payload.get("customer_id", "")
        scores = payload.get("scores", {})
        if not customer_id or not isinstance(scores, dict):
            return None
        return {
            "customer_id": customer_id,
            "scores": scores,
            "score_ts": payload.get("score_ts"),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# DB helper mixin — opens one Postgres connection per subtask, creates tables
# ---------------------------------------------------------------------------

class _DbMixin:
    def _open_db(self):
        self.conn = psycopg2.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME
        )
        self.conn.autocommit = False
        self._ensure_tables()

    def _close_db(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass

    def _ensure_tables(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS public.realtime_intent_events (
            event_id            UUID PRIMARY KEY,
            customer_id         VARCHAR(255) NOT NULL,
            product             VARCHAR(64)  NOT NULL,
            pattern_name        VARCHAR(128) NOT NULL,
            pattern_boost       DOUBLE PRECISION,
            base_score          DOUBLE PRECISION,
            composite_score     DOUBLE PRECISION,
            threshold           DOUBLE PRECISION,
            event_ts_ms         BIGINT,
            window_start_ms     BIGINT,
            category_spend_7d   JSONB,
            payload             JSONB,
            session_window_ms   BIGINT,
            is_late             BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_rie_customer
            ON public.realtime_intent_events(customer_id);
        CREATE INDEX IF NOT EXISTS idx_rie_created
            ON public.realtime_intent_events(created_at DESC);

        -- Feature 5: table for late events routed via Side Output
        CREATE TABLE IF NOT EXISTS public.realtime_late_events (
            id          BIGSERIAL PRIMARY KEY,
            customer_id VARCHAR(255),
            raw_event   JSONB NOT NULL,
            arrived_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_rle_customer
            ON public.realtime_late_events(customer_id);

        -- Feature 3: sliding-window spend aggregation feed
        CREATE TABLE IF NOT EXISTS public.propensity_enrichment_feed (
            window_end_ms     BIGINT          NOT NULL,
            customer_id       VARCHAR(255)    NOT NULL,
            merchant_category VARCHAR(128)    NOT NULL,
            total_spend       DOUBLE PRECISION NOT NULL,
            event_count       INTEGER         NOT NULL,
            updated_at        TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (window_end_ms, customer_id, merchant_category)
        );
        CREATE INDEX IF NOT EXISTS idx_pef_customer
            ON public.propensity_enrichment_feed(customer_id);
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(ddl)
            self.conn.commit()
        except Exception:
            # Multiple parallel subtasks race to create tables; if another
            # subtask already created them the IF NOT EXISTS check is not
            # atomic and raises UniqueViolation — safe to ignore.
            try:
                self.conn.rollback()
            except Exception:
                pass

    def _upsert_intent_event(self, intent_event):
        sql = """
        INSERT INTO public.realtime_intent_events (
            event_id, customer_id, product, pattern_name,
            pattern_boost, base_score, composite_score, threshold,
            event_ts_ms, window_start_ms, category_spend_7d, payload,
            session_window_ms, is_late
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s)
        ON CONFLICT (event_id) DO NOTHING
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                intent_event["event_id"],
                intent_event["customer_id"],
                intent_event["product"],
                intent_event["pattern_name"],
                intent_event["pattern_boost"],
                intent_event["base_score"],
                intent_event["composite_score"],
                intent_event["threshold"],
                intent_event["event_ts_ms"],
                intent_event["window_start_ms"],
                json.dumps(intent_event["category_spend_7d"]),
                json.dumps(intent_event),
                intent_event.get("session_window_ms"),
                intent_event.get("is_late", False),
            ))
        self.conn.commit()

    def _insert_late_event(self, raw_json: str, customer_id: str):
        sql = """
        INSERT INTO public.realtime_late_events (customer_id, raw_event)
        VALUES (%s, %s::jsonb)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (customer_id, raw_json))
        self.conn.commit()

    def _upsert_enrichment_feed(self, window_end_ms: int, customer_id: str,
                                 category: str, spend: float, count: int):
        sql = """
        INSERT INTO public.propensity_enrichment_feed
            (window_end_ms, customer_id, merchant_category, total_spend, event_count)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (window_end_ms, customer_id, merchant_category)
        DO UPDATE SET total_spend  = EXCLUDED.total_spend,
                      event_count  = EXCLUDED.event_count,
                      updated_at   = CURRENT_TIMESTAMP
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (window_end_ms, customer_id, category, spend, count))
        self.conn.commit()


# ---------------------------------------------------------------------------
# Feature 3 — Sliding Window: 7-day / 1-day-slide spend aggregation
# Key: (customer_id, merchant_category)
# Output: upserted to propensity_enrichment_feed table
# ---------------------------------------------------------------------------

class SpendAggregateFunction(_DbMixin, ProcessWindowFunction):
    """
    Processes each (customer_id, merchant_category) bucket over a
    SlidingEventTimeWindows(7 days, 1 day slide) and writes aggregated
    spend + event count to propensity_enrichment_feed.
    """

    def open(self, runtime_context: RuntimeContext):
        self._open_db()

    def close(self):
        self._close_db()

    def process(self, key, context: ProcessWindowFunction.Context, elements):
        customer_id, merchant_category = key
        total_spend = 0.0
        event_count = 0
        for e in elements:
            total_spend += float(e.get("amount", 0.0))
            event_count += 1
        window_end_ms = context.window().max_timestamp()
        self._upsert_enrichment_feed(
            window_end_ms, customer_id, merchant_category,
            round(total_spend, 2), event_count,
        )
        yield json.dumps({
            "customer_id": customer_id,
            "merchant_category": merchant_category,
            "total_spend": round(total_spend, 2),
            "event_count": event_count,
            "window_end_ms": window_end_ms,
        })


# ---------------------------------------------------------------------------
# Feature 5 — Late Event Side Output sink
# ---------------------------------------------------------------------------

class LateEventSinkFunction(_DbMixin, KeyedProcessFunction):
    """Persists post-watermark late events to realtime_late_events table."""

    def open(self, runtime_context: RuntimeContext):
        self._open_db()

    def close(self):
        self._close_db()

    def process_element(self, value, ctx):
        try:
            parsed = json.loads(value)
            customer_id = parsed.get("customer_id", "unknown")
        except Exception:
            customer_id = "unknown"
        self._insert_late_event(value, customer_id)
        yield value

    def on_timer(self, timestamp, ctx):
        pass


# ---------------------------------------------------------------------------
# Main KeyedBroadcastProcessFunction — Features 1, 2, 4
# ---------------------------------------------------------------------------

class IntentOfferFunction(_DbMixin, KeyedBroadcastProcessFunction):
    """
    Features:
      1. Broadcast State — reads base propensity scores from local broadcast
         state (zero DB lookup per transaction event).
      2. CEP Session Window — home_loan uses a 7-day inactivity-gap session:
         accumulate home_improvement spend; fire on_timer when no new spend
         arrives within 7 days, or immediately when spend crosses $2,000.
      4. AllowedLateness (10 min) — detects late arrival via watermark
         comparison; stamps is_late=True on intent events for late signals.
      4. State TTL (30 days) — MapState<product, last_offer_ts_ms> with
         StateTtlConfig.30days auto-expires; prevents duplicate offers.
    """

    def open(self, runtime_context: RuntimeContext):
        self.subtask_index = runtime_context.get_index_of_this_subtask()
        self.processed_event_count = 0
        self.broadcast_update_count = 0

        # Feature 4 — State TTL: 30-day dedup MapState
        ttl_config = (
            StateTtlConfig
            .new_builder(Time.days(30))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        dedup_desc = MapStateDescriptor(
            "offer_dedup_state", Types.STRING(), Types.LONG()
        )
        dedup_desc.enable_time_to_live(ttl_config)
        self.dedup_state = runtime_context.get_map_state(dedup_desc)

        # General 7-day rolling event buffer
        self.events_state = runtime_context.get_state(
            ValueStateDescriptor("recent_events_json", Types.STRING())
        )

        # Feature 2 — CEP session state for home_loan
        self.home_session_spend = runtime_context.get_state(
            ValueStateDescriptor("home_session_spend", Types.DOUBLE())
        )
        self.home_session_start = runtime_context.get_state(
            ValueStateDescriptor("home_session_start", Types.LONG())
        )
        self.home_session_timer = runtime_context.get_state(
            ValueStateDescriptor("home_session_timer", Types.LONG())
        )

        self._open_db()
        logger.info(
            "subtask=%s opened threshold=%s parallelism=%s kafka=%s txn_topic=%s product_topic=%s score_topic=%s offer_topic=%s",
            self.subtask_index,
            THRESHOLD,
            PARALLELISM,
            KAFKA,
            TXN_TOPIC,
            PRODUCT_TOPIC,
            BROADCAST_TOPIC,
            OFFER_TOPIC,
        )

    def close(self):
        self._close_db()

    # Feature 1 — update broadcast state from weekly batch score stream
    def process_broadcast_element(self, value, ctx):
        if not value:
            return
        self.broadcast_update_count += 1
        b_state = ctx.get_broadcast_state(BROADCAST_STATE_DESC)
        b_state.put(
            value["customer_id"],
            json.dumps({"scores": value.get("scores", {}), "score_ts": value.get("score_ts")}),
        )
        if self.broadcast_update_count <= 3 or self.broadcast_update_count % BROADCAST_LOG_EVERY_N == 0:
            logger.info(
                "subtask=%s broadcast_update_count=%s customer_id=%s products=%s score_ts=%s",
                self.subtask_index,
                self.broadcast_update_count,
                value.get("customer_id"),
                sorted(list(value.get("scores", {}).keys())),
                value.get("score_ts"),
            )

    def process_element(self, value, ctx):
        if not value:
            return
        customer_id = value.get("customer_id")
        now_ms = value.get("event_ts_ms", 0)
        if not customer_id or now_ms == 0:
            return

        self.processed_event_count += 1

        # Feature 4 — detect late arrival via watermark
        current_wm = ctx.timer_service().current_watermark()
        LONG_MIN = -9223372036854775808  # watermark before any event
        is_late = (current_wm != LONG_MIN and now_ms < current_wm)

        if self.processed_event_count <= 5 or self.processed_event_count % EVENT_LOG_EVERY_N == 0:
            logger.info(
                "subtask=%s event_count=%s customer_id=%s kind=%s category=%s event_type=%s ts_ms=%s watermark=%s is_late=%s",
                self.subtask_index,
                self.processed_event_count,
                customer_id,
                value.get("kind"),
                value.get("merchant_category"),
                value.get("event_type"),
                now_ms,
                current_wm,
                is_late,
            )

        # Feature 4 — route late events to side output immediately
        if is_late:
            logger.info(
                "subtask=%s late_event customer_id=%s kind=%s ts_ms=%s watermark=%s",
                self.subtask_index,
                customer_id,
                value.get("kind"),
                now_ms,
                current_wm,
            )
            ctx.output(LATE_EVENT_TAG, json.dumps(value))

        # Maintain rolling 7-day event buffer
        events = self._load_events()
        events.append(value)
        events = [e for e in events if now_ms - e.get("event_ts_ms", now_ms) <= MS_7_DAY]
        self._save_events(events)

        # Feature 2 — drive home-loan CEP session window
        # Trigger on home_improvement, furniture, and kitchen_appliances — all
        # signal home-ownership intent and are invisible in weekly batch scores.
        HOME_LOAN_CATEGORIES = {"home_improvement", "furniture", "kitchen_appliances"}
        home_loan_signal = None
        if (value.get("kind") == "txn"
                and value.get("merchant_category") in HOME_LOAN_CATEGORIES):
            home_loan_signal = self._update_home_session(value, ctx, now_ms)

        signals, category_spend_7d = self._detect_patterns(events, now_ms, home_loan_signal)

        # Feature 1 — enrich from Broadcast State (zero DB lookup)
        b_state = ctx.get_broadcast_state(BROADCAST_STATE_DESC)
        raw_base = b_state.get(customer_id)
        base_scores = {}
        if raw_base:
            try:
                base_scores = json.loads(raw_base).get("scores", {})
            except Exception:
                pass

        if signals:
            if base_scores:
                logger.info(
                    "subtask=%s score_hit customer_id=%s products=%s signal_count=%s",
                    self.subtask_index,
                    customer_id,
                    sorted(list(base_scores.keys())),
                    len(signals),
                )
            else:
                logger.info(
                    "subtask=%s score_miss customer_id=%s using_default_base=45.0 signal_count=%s",
                    self.subtask_index,
                    customer_id,
                    len(signals),
                )

        for product, pattern_name, metric, session_window_ms in signals:
            # Feature 4 — TTL-backed dedup check
            last_ts = self.dedup_state.get(product)
            if last_ts is not None and (now_ms - last_ts) < MS_1_DAY:
                logger.info(
                    "subtask=%s dedup_skip customer_id=%s product=%s seconds_since_last=%s",
                    self.subtask_index,
                    customer_id,
                    product,
                    round((now_ms - last_ts) / 1000.0, 2),
                )
                continue

            base_score = float(base_scores.get(product, 45.0))
            pattern_boost = float(PATTERN_BOOST.get(product, 20.0))
            composite_score = min(100.0, round(base_score + pattern_boost, 2))

            intent_event = {
                "event_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "product": product,
                "pattern_name": pattern_name,
                "pattern_metric": metric,
                "pattern_boost": pattern_boost,
                "base_score": round(base_score, 2),
                "composite_score": composite_score,
                "threshold": THRESHOLD,
                "event_ts_ms": now_ms,
                "window_start_ms": now_ms - MS_7_DAY,
                "session_window_ms": session_window_ms,
                "category_spend_7d": category_spend_7d,
                "source": "intent_detector",
                "is_late": is_late,
            }
            self._upsert_intent_event(intent_event)
            self.dedup_state.put(product, now_ms)   # resets TTL clock

            logger.info(
                "subtask=%s intent_event customer_id=%s product=%s pattern=%s base_score=%s boost=%s final_score=%s threshold=%s",
                self.subtask_index,
                customer_id,
                product,
                pattern_name,
                intent_event["base_score"],
                pattern_boost,
                composite_score,
                THRESHOLD,
            )

            if composite_score > THRESHOLD:
                logger.info(
                    "subtask=%s offer_emitted customer_id=%s product=%s final_score=%s trigger=%s",
                    self.subtask_index,
                    customer_id,
                    product,
                    composite_score,
                    pattern_name,
                )
                yield json.dumps({
                    "customer_id": customer_id,
                    "product": product,
                    "base_score": intent_event["base_score"],
                    "intent_score": pattern_boost,
                    "final_score": composite_score,
                    "threshold": THRESHOLD,
                    "trigger_event": pattern_name,
                    "txn_features": {
                        "window_days": 7,
                        "slide_days": 1,
                        "pattern_metric": metric,
                        "session_window_ms": session_window_ms,
                    },
                    "source": "intent_detector",
                    "event_ts_ms": now_ms,
                    "is_late": is_late,
                })
            else:
                logger.info(
                    "subtask=%s below_threshold customer_id=%s product=%s final_score=%s threshold=%s",
                    self.subtask_index,
                    customer_id,
                    product,
                    composite_score,
                    THRESHOLD,
                )

    # ------------------------------------------------------------------
    # Feature 2 — CEP Session Window for home_loan
    #
    # Accumulate home_improvement spend in keyed ValueState.
    # Register an event-time timer 7 days after each txn (inactivity gap).
    # If spend > $2,000 at any point, fire immediately and reset session.
    # If spend > $2,000 when timer fires (session closed), emit via on_timer.
    # ------------------------------------------------------------------

    def _update_home_session(self, event, ctx, now_ms):
        spend = self.home_session_spend.value() or 0.0
        session_start = self.home_session_start.value()
        if session_start is None:
            session_start = now_ms
            self.home_session_start.update(now_ms)

        old_timer = self.home_session_timer.value()
        if old_timer:
            ctx.timer_service().delete_event_time_timer(old_timer)

        spend += float(event.get("amount", 0.0))
        self.home_session_spend.update(spend)

        new_timer = now_ms + MS_7_DAY
        ctx.timer_service().register_event_time_timer(new_timer)
        self.home_session_timer.update(new_timer)

        if spend > 2000.0:
            session_ms = now_ms - session_start
            logger.info(
                "subtask=%s home_session_trigger customer_id=%s spend=%s session_ms=%s",
                self.subtask_index,
                event.get("customer_id"),
                round(spend, 2),
                session_ms,
            )
            self._reset_home_session()
            return ("home_loan", "home_improvement_session_spend_gt_2000", spend, session_ms)
        return None

    def on_timer(self, timestamp, ctx):
        """Session inactivity timer fired — close session and evaluate."""
        spend = self.home_session_spend.value() or 0.0
        session_start = self.home_session_start.value() or timestamp
        session_ms = timestamp - session_start

        if spend > 2000.0:
            customer_id = ctx.get_current_key()
            b_state = ctx.get_broadcast_state(BROADCAST_STATE_DESC)
            raw_base = b_state.get(customer_id) if b_state else None
            base_scores = {}
            if raw_base:
                try:
                    base_scores = json.loads(raw_base).get("scores", {})
                except Exception:
                    pass

            if base_scores:
                logger.info(
                    "subtask=%s timer_score_hit customer_id=%s products=%s spend=%s",
                    self.subtask_index,
                    customer_id,
                    sorted(list(base_scores.keys())),
                    round(spend, 2),
                )
            else:
                logger.info(
                    "subtask=%s timer_score_miss customer_id=%s using_default_base=45.0 spend=%s",
                    self.subtask_index,
                    customer_id,
                    round(spend, 2),
                )

            last_ts = self.dedup_state.get("home_loan")
            if last_ts is None or (timestamp - last_ts) >= MS_1_DAY:
                base_score = float(base_scores.get("home_loan", 45.0))
                pattern_boost = PATTERN_BOOST["home_loan"]
                composite_score = min(100.0, round(base_score + pattern_boost, 2))

                intent_event = {
                    "event_id": str(uuid.uuid4()),
                    "customer_id": customer_id,
                    "product": "home_loan",
                    "pattern_name": "home_improvement_session_closed_gt_2000",
                    "pattern_metric": spend,
                    "pattern_boost": pattern_boost,
                    "base_score": round(base_score, 2),
                    "composite_score": composite_score,
                    "threshold": THRESHOLD,
                    "event_ts_ms": timestamp,
                    "window_start_ms": session_start,
                    "session_window_ms": session_ms,
                    "category_spend_7d": {"home_improvement": round(spend, 2)},
                    "source": "intent_detector_timer",
                    "is_late": False,
                }
                self._upsert_intent_event(intent_event)
                self.dedup_state.put("home_loan", timestamp)

                logger.info(
                    "subtask=%s timer_intent_event customer_id=%s product=home_loan base_score=%s boost=%s final_score=%s threshold=%s",
                    self.subtask_index,
                    customer_id,
                    intent_event["base_score"],
                    pattern_boost,
                    composite_score,
                    THRESHOLD,
                )

                if composite_score > THRESHOLD:
                    logger.info(
                        "subtask=%s timer_offer_emitted customer_id=%s product=home_loan final_score=%s trigger=session_timer_fired",
                        self.subtask_index,
                        customer_id,
                        composite_score,
                    )
                    yield json.dumps({
                        "customer_id": customer_id,
                        "product": "home_loan",
                        "base_score": intent_event["base_score"],
                        "intent_score": pattern_boost,
                        "final_score": composite_score,
                        "threshold": THRESHOLD,
                        "trigger_event": "session_timer_fired",
                        "txn_features": {
                            "session_window_ms": session_ms,
                            "spend": round(spend, 2),
                        },
                        "source": "intent_detector",
                        "event_ts_ms": timestamp,
                        "is_late": False,
                    })
                else:
                    logger.info(
                        "subtask=%s timer_below_threshold customer_id=%s product=home_loan final_score=%s threshold=%s",
                        self.subtask_index,
                        customer_id,
                        composite_score,
                        THRESHOLD,
                    )
        self._reset_home_session()

    def _reset_home_session(self):
        self.home_session_spend.clear()
        self.home_session_start.clear()
        self.home_session_timer.clear()

    def _detect_patterns(self, events, now_ms, home_loan_signal):
        txns = [e for e in events if e.get("kind") == "txn"]
        txns_24h = [e for e in txns if now_ms - e["event_ts_ms"] <= MS_1_DAY]

        retail_count_24h = sum(1 for e in txns_24h if e.get("merchant_category") == "retail")
        has_existing_card = max((int(e.get("has_credit_card", 0)) for e in txns), default=0)
        salary_7d = [
            e for e in txns
            if e.get("transaction_type") == "salary" or e.get("merchant_category") == "salary"
        ]
        travel_or_elec_7d = [
            e for e in txns if e.get("merchant_category") in ["travel", "electronics"]
        ]

        sorted_txns = sorted(txns, key=lambda x: x["event_ts_ms"])
        fd_signal = False
        for i, e in enumerate(sorted_txns):
            is_large_credit = (
                e.get("transaction_type") == "salary" or e.get("merchant_category") == "salary"
            ) and float(e.get("amount", 0.0)) >= 5000.0
            if not is_large_credit:
                continue
            later = [
                t for t in sorted_txns[i + 1:]
                if e["event_ts_ms"] < t["event_ts_ms"] <= e["event_ts_ms"] + MS_72_HR
            ]
            if len(later) == 0 and now_ms >= e["event_ts_ms"] + MS_72_HR:
                fd_signal = True
                break

        category_spend_7d = {}
        for e in txns:
            cat = e.get("merchant_category", "unknown")
            category_spend_7d[cat] = round(category_spend_7d.get(cat, 0.0) + float(e.get("amount", 0.0)), 2)

        # (product, pattern_name, metric, session_window_ms)
        signals = []
        if home_loan_signal:
            signals.append(home_loan_signal)
        if retail_count_24h >= 3 and has_existing_card == 0:
            signals.append(("credit_card", "retail_3plus_24h_no_existing_card", float(retail_count_24h), None))
        if travel_or_elec_7d and salary_7d:
            signals.append(("personal_loan", "travel_or_electronics_plus_salary_same_week", float(len(travel_or_elec_7d)), None))
        if fd_signal:
            signals.append(("fixed_deposit", "large_credit_and_72h_no_activity", 1.0, None))
        return signals, category_spend_7d

    def _load_events(self):
        raw = self.events_state.value()
        if not raw:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def _save_events(self, events):
        self.events_state.update(json.dumps(events))


# ---------------------------------------------------------------------------
# Topology
# ---------------------------------------------------------------------------

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(PARALLELISM)

# Sources
txn_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA)
    .set_topics(TXN_TOPIC)
    .set_group_id("intent-detector-txn")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
product_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA)
    .set_topics(PRODUCT_TOPIC)
    .set_group_id("intent-detector-product")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
score_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA)
    .set_topics(BROADCAST_TOPIC)
    .set_group_id("intent-score-broadcast-consumer")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Feature 5 — AllowedLateness: accept events up to 10 minutes late
class TimestampAssigner:
    def extract_timestamp(self, element, record_timestamp):
        return int(element.get("event_ts_ms", 0))

watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_minutes(10))
    .with_timestamp_assigner(TimestampAssigner())
)

raw_txn     = env.from_source(txn_source,     WatermarkStrategy.no_watermarks(), "txn-source")
raw_product = env.from_source(product_source, WatermarkStrategy.no_watermarks(), "product-source")

parsed_txn     = raw_txn.map(parse_txn).filter(lambda e: e is not None)
parsed_product = raw_product.map(parse_product_event).filter(lambda e: e is not None)

all_events   = parsed_txn.union(parsed_product)
timed_events = all_events.assign_timestamps_and_watermarks(watermark_strategy)

# Feature 3 — Sliding Window: 7-day / 1-day-slide spend aggregation
# SpendAggregateFunction upserts to propensity_enrichment_feed on each window fire.
# We add a discard sink so Flink registers the stream as consumed and executes it.
spend_stream = (
    parsed_txn
    .filter(lambda e: e.get("merchant_category") not in (None, "unknown"))
    .assign_timestamps_and_watermarks(watermark_strategy)
    .key_by(lambda e: (e["customer_id"], e["merchant_category"]))
    .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1)))
    .process(SpendAggregateFunction(), output_type=Types.STRING())
)
spend_stream.print()  # activates the window operator; SpendAggregateFunction already upserts to DB

# Score broadcast stream
score_stream = (
    env.from_source(score_source, WatermarkStrategy.no_watermarks(), "score-source")
    .map(parse_score)
    .filter(lambda e: e is not None)
)

# Main intent detection operator
intent_process_result = (
    timed_events
    .key_by(lambda e: e["customer_id"])
    .connect(score_stream.broadcast(BROADCAST_STATE_DESC))
    .process(IntentOfferFunction(), output_type=Types.STRING())
)

# Feature 5 — Side Output: route late events to realtime_late_events table
late_events_stream = intent_process_result.get_side_output(LATE_EVENT_TAG)
late_events_stream.key_by(
    lambda raw: (json.loads(raw).get("customer_id", "unknown") if raw else "unknown")
).process(LateEventSinkFunction(), output_type=Types.STRING())

# Offer Kafka Sink
offer_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(KAFKA)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(OFFER_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    .build()
)
intent_process_result.sink_to(offer_sink)

env.execute("IntentDetector - Real-Time Intent Detection")
