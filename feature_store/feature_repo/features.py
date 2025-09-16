from feast import Entity, FeatureView, Field, FeatureService
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Float32, Int32, String, UnixTimestamp
from feast.value_type import ValueType
from datetime import timedelta

# project = Project(name="feature_store", description="A project for driver statistics")

# primary key
order_id = Entity(name='Order_ID', join_keys=['Order ID'], value_type=ValueType.STRING)

# offline store, superstore_feature
superstore_feature_source = PostgreSQLSource(
    name='superstore_feature_source',
    query='select * from superstore_feature',
    table='superstore_feature',
    timestamp_field='Order Date',
    created_timestamp_column='created'
)

# get these columns for training (from offline store)
# and for serving (sync to online store when materialize)
superstore_feature_fv = FeatureView(
    name="superstore_feature_fv",
    source=superstore_feature_source,
    schema=[
        Field(name="Order ID", dtype=String),
        Field(name="Order Date", dtype=UnixTimestamp),
        Field(name="Ship Date", dtype=UnixTimestamp),
        Field(name="Ship Mode", dtype=String),
        Field(name="Segment", dtype=String),
        Field(name="Country", dtype=String),
        Field(name="City", dtype=String),
        Field(name="State", dtype=String),
        Field(name="Region", dtype=String),
        Field(name="Postal Code", dtype=Int32),
        Field(name="Category", dtype=String),
        Field(name="Sub-Category", dtype=String),
        Field(name="Product Name", dtype=String),
        Field(name="Sales", dtype=Float32),
        Field(name="Quantity", dtype=Int32),
        Field(name="Discount", dtype=Float32),

        # they are ID features
        # Field(name="Customer ID", dtype=String),
        # Field(name="Customer Name", dtype=String),
        # Field(name="Product ID", dtype=String),
        # Field(name="Profit", dtype=Float32),
    ],
    entities=[order_id],
    ttl=timedelta(hours=3),
    online=True,
    offline=True
)

# offline store, superstore_target
superstore_target_source = PostgreSQLSource(
    name='superstore_target_source',
    query='select * from superstore_target',
    table='superstore_target',
    timestamp_field='Order Date',
    created_timestamp_column='created'
)

# get these columns for training (from offline store)
# and for serving (sync to online store when materialize)
superstore_target_fv = FeatureView(
    name='superstore_target_fv',
    source=superstore_target_source,
    schema=[
        Field(name="Order ID", dtype=String),
        Field(name="Order Date", dtype=UnixTimestamp),
        Field(name="Profit", dtype=Float32)
    ],
    entities=[order_id],
    ttl=timedelta(days=1),
    online=True,
    offline=True
)

# in production, it is recommended to use feature service
# instead of feature view
superstore_feature_fs = FeatureService(
    name='superstore_feature_fs',
    features=[superstore_feature_fv]
)
superstore_target_fs = FeatureService(
    name='superstore_target_fs',
    features=[superstore_target_fv]
)

# -----------------------------
# feast_driver_hourly_stats

driver_id = Entity(name='driver_id', join_keys=['driver_id'], value_type=ValueType.INT32)

driver_source = PostgreSQLSource(
    name='driver_source',
    query='select * from feast_driver_hourly_stats',
    table='feast_driver_hourly_stats',
    timestamp_field='event_timestamp',
    created_timestamp_column='created',
)

driver_fv = FeatureView(
    name='driver_fv',
    source=driver_source,
    schema=[
        Field(name="driver_id", dtype=Int32),
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int32),
        Field(name="created", dtype=UnixTimestamp),
    ],
    entities=[driver_id],
    ttl=timedelta(days=1),
    online=True,
    offline=True
)

driver_fs = FeatureService(
    name='driver_fs',
    features=[driver_fv]
)
