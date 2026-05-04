LIST_COLS = [
    "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "rate_code_id", "store_and_fwd_flag", "pickup_longitude",
    "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "pu_location_id",
    "do_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
    "surcharge", "improvement_surcharge", "tip_amount", "tolls_amount",
    "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
]

LIST_DTYPES = {
    "vendor_id":             "object",
    "pickup_datetime":       "datetime64[ns]",
    "dropoff_datetime":      "datetime64[ns]",
    "passenger_count":       "float64",
    "trip_distance":         "float64",
    "rate_code_id":          "float64",
    "store_and_fwd_flag":    "object",
    "pickup_longitude":      "float64",
    "pickup_latitude":       "float64",
    "dropoff_longitude":     "float64",
    "dropoff_latitude":      "float64",
    "pu_location_id":        "object",
    "do_location_id":        "object",
    "payment_type":          "object",
    "fare_amount":           "float64",
    "extra":                 "float64",
    "mta_tax":               "float64",
    "surcharge":             "float64",
    "improvement_surcharge": "float64",
    "tip_amount":            "float64",
    "tolls_amount":          "float64",
    "total_amount":          "float64",
    "congestion_surcharge":  "float64",
    "airport_fee":           "float64",
    "cbd_congestion_fee":    "float64",
}

map1 = {
    "columns": 'VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee',
    'mapping': {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
    }
}

map2 = {
    'columns':  'VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee',
    'mapping': {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "Airport_fee": "airport_fee",
    }
}

map3 = {
    'columns': 'vendor_name|Trip_Pickup_DateTime|Trip_Dropoff_DateTime|Passenger_Count|Trip_Distance|Start_Lon|Start_Lat|Rate_Code|store_and_forward|End_Lon|End_Lat|Payment_Type|Fare_Amt|surcharge|mta_tax|Tip_Amt|Tolls_Amt|Total_Amt',
    'mapping': {
        "vendor_name": "vendor_id",
        "Trip_Pickup_DateTime": "pickup_datetime",
        "Trip_Dropoff_DateTime": "dropoff_datetime",
        "Passenger_Count": "passenger_count",
        "Trip_Distance": "trip_distance",
        "Start_Lon": "pickup_longitude",
        "Start_Lat": "pickup_latitude",
        "Rate_Code": "rate_code_id",
        "store_and_forward": "store_and_fwd_flag",
        "End_Lon": "dropoff_longitude",
        "End_Lat": "dropoff_latitude",
        "Payment_Type": "payment_type",
        "Fare_Amt": "fare_amount",
        "Tip_Amt": "tip_amount",
        "Tolls_Amt": "tolls_amount",
        "Total_Amt": "total_amount",
    }
}

map4 = {
    'columns': 'VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee',
    'mapping': {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "Airport_fee": "airport_fee",
    }
}

map5 = {
    'columns':  'vendor_id|pickup_datetime|dropoff_datetime|passenger_count|trip_distance|pickup_longitude|pickup_latitude|rate_code|store_and_fwd_flag|dropoff_longitude|dropoff_latitude|payment_type|fare_amount|surcharge|mta_tax|tip_amount|tolls_amount|total_amount',
    'mapping': {
        "rate_code": "rate_code_id",
    }
}

map6 = {
    'columns':  'vendorID|tpepPickupDateTime|tpepDropoffDateTime|passengerCount|tripDistance|puLocationId|doLocationId|startLon|startLat|endLon|endLat|rateCodeId|storeAndFwdFlag|paymentType|fareAmount|extra|mtaTax|improvementSurcharge|tipAmount|tollsAmount|totalAmount',
    'mapping': {
        "vendorID": "vendor_id",
        "tpepPickupDateTime": "pickup_datetime",
        "tpepDropoffDateTime": "dropoff_datetime",
        "passengerCount": "passenger_count",
        "tripDistance": "trip_distance",
        "puLocationId": "pu_location_id",
        "doLocationId": "do_location_id",
        "startLon": "pickup_longitude",
        "startLat": "pickup_latitude",
        "endLon": "dropoff_longitude",
        "endLat": "dropoff_latitude",
        "rateCodeId": "rate_code_id",
        "storeAndFwdFlag": "store_and_fwd_flag",
        "paymentType": "payment_type",
        "fareAmount": "fare_amount",
        "mtaTax": "mta_tax",
        "improvementSurcharge": "improvement_surcharge",
        "tipAmount": "tip_amount",
        "tollsAmount": "tolls_amount",
        "totalAmount": "total_amount",
    }
}

SCHEMA_MAPPINGS = [map1, map2, map3, map4, map5, map6]
