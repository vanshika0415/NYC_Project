import io
import pandas as pd
import requests

def extract_data(url: str) -> pd.DataFrame:
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text), sep=',')
    return df

def transform_data(df: pd.DataFrame) -> tuple:
    # Convert to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Drop duplicates and assign trip_id
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    # --- DIMENSIONS ---
    # 1. Datetime Dimension
    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].copy()
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    # 2. Passenger Count Dimension
    passenger_count_dim = df[['passenger_count']].copy()
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    # 3. Trip Distance Dimension
    trip_distance_dim = df[['trip_distance']].copy()
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    # 4. Rate Code Dimension
    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
    rate_code_dim = df[['RatecodeID']].copy()
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    # 5. Pickup Location Dimension
    pickup_location_dim = df[['pickup_latitude', 'pickup_longitude']].copy()
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

    # 6. Dropoff Location Dimension
    dropoff_location_dim = df[['dropoff_latitude', 'dropoff_longitude']].copy()
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

    # 7. Payment Type Dimension
    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    payment_type_dim = df[['payment_type']].copy()
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    # --- FACT TABLE ---
    fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
                   .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
                   .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
                   .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
                   .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id') \
                   .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
                   .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
                   [['trip_id', 'VendorID', 'datetime_id', 'passenger_count_id',
                     'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag',
                     'pickup_location_id', 'dropoff_location_id', 'payment_type_id',
                     'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                     'improvement_surcharge', 'total_amount']]

    return {
        "fact_table": fact_table,
        "datetime_dim": datetime_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pickup_location_dim": pickup_location_dim,
        "dropoff_location_dim": dropoff_location_dim,
        "payment_type_dim": payment_type_dim
    }

# --- RUN SCRIPT ---
if __name__ == "__main__":
    url = "https://storage.googleapis.com/uber-data-engineering-project/uber_data.csv"
    raw_df = extract_data(url)
    transformed = transform_data(raw_df)

    # For example, preview the fact table
    print(transformed["fact_table"].head())
