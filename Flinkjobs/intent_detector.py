import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import BroadcastProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.common import Types, Time, Duration, WatermarkStrategy

from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema

from pyflink.cep import CEP
from pyflink.cep.pattern import Pattern
from pyflink.cep.functions import PatternSelectFunction


# =========================
# ENV
# =========================
KAFKA = "kafka:9092"

TXN_TOPIC = "txn.stream"
BROADCAST_TOPIC = "propensity_scores.broadcast"
OUTPUT_TOPIC = "crm.realtime_offers"


# =========================
# ENVIRONMENT
# =========================
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(16)


# =========================
# SOURCE (TRANSACTIONS)
# =========================
txn_source = KafkaSource.builder() \
    .set_bootstrap_servers(KAFKA) \
    .set_topics(TXN_TOPIC) \
    .set_group_id("intent-detector") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()


def parse(x):
    return json.loads(x)


watermark = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
    .with_timestamp_assigner(lambda e, ts: int(e["event_ts"]))


txn = env.from_source(txn_source, watermark, "txn").map(parse)


# =========================
# BROADCAST STATE (PROPENSITY SCORES)
# =========================
propensity_state_desc = MapStateDescriptor(
    "propensity_state",
    Types.STRING(),
    Types.MAP(Types.STRING(), Types.FLOAT())
)


# =========================
# SLIDING WINDOW FEATURES
# =========================
def aggregate(a, b):
    return {
        "customer_id": a["customer_id"],
        "home_improvement_spend": float(a.get("home_improvement_spend", 0)) + float(b.get("amount", 0)),
        "txn_count": a.get("txn_count", 1) + 1
    }


windowed = txn \
    .key_by(lambda x: x["customer_id"]) \
    .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1))) \
    .reduce(aggregate)


# =========================
# CEP (HOME LOAN INTENT)
# =========================
pattern = Pattern.begin("start") \
    .where(lambda e: e["merchant_category"] == "home_improvement") \
    .times_or_more(2) \
    .within(Time.days(7))


class HomeIntentSelect(PatternSelectFunction):
    def select(self, pattern):
        events = pattern["start"]
        total = sum(float(e["amount"]) for e in events)

        if total > 2000:
            return {
                "customer_id": events[0]["customer_id"],
                "intent": "home_loan",
                "boost": 40
            }


cep_stream = CEP.pattern(txn.key_by(lambda x: x["customer_id"]), pattern)

cep_result = cep_stream.select(HomeIntentSelect())


# =========================
# DEDUP STATE (30 DAYS)
# =========================
class DedupFilter(KeyedProcessFunction):

    def open(self, runtime_context):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("last_offer_ts", Types.LONG())
        )

    def process_element(self, value, ctx):
        last = self.state.value()
        now = int(value["event_ts"])

        if last and (now - last < 30 * 24 * 60 * 60):
            return

        self.state.update(now)
        yield value


# =========================
# SCORING ENGINE (BROADCAST + CEP + WINDOW)
# =========================
class IntentScorer(BroadcastProcessFunction):

    def process_element(self, event, ctx, out):

        state = ctx.get_broadcast_state(propensity_state_desc)

        customer = event["customer_id"]
        product = event.get("product", "home_loan")

        base_scores = state.get(customer) if state.contains(customer) else {}

        base = base_scores.get(product, 40)

        boost = 0
        if event.get("merchant_category") == "home_improvement":
            boost = 40
        if event.get("merchant_category") in ["electronics", "travel"]:
            boost = 30

        final_score = min(100, base + boost)

        if final_score > 75:
            out.collect(json.dumps({
                "customer_id": customer,
                "product": product,
                "score": final_score,
                "source": "flink_intent_engine"
            }))

    def process_broadcast_element(self, value, ctx, out):
        state = ctx.get_broadcast_state(propensity_state_desc)
        state.put(value["customer_id"], value["scores"])


# =========================
# CONNECT STREAMS
# =========================
result = txn.connect(
    txn.broadcast(propensity_state_desc)
).process(
    IntentScorer(),
    output_type=Types.STRING()
)


# =========================
# KAFKA SINK
# =========================
sink = KafkaSink.builder() \
    .set_bootstrap_servers(KAFKA) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(OUTPUT_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
    .build()


result.sink_to(sink)


# =========================
# EXECUTE
# =========================
env.execute("Intent Detector - Full Enterprise Pipeline")