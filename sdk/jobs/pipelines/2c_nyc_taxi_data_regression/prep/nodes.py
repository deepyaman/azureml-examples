from typing import Tuple

import pandas as pd

# These functions ensure that null data is removed from the dataset,
# which will help increase machine learning model accuracy.


def get_dict(dict_str):
    pairs = dict_str.strip("{}").split(";")
    new_dict = {}
    for pair in pairs:
        print(pair)
        key, value = pair.strip().split(":")
        new_dict[key.strip().strip("'")] = value.strip().strip("'")
    return new_dict


def cleanseData(data, columns, useful_columns):
    useful_columns = [
        s.strip().strip("'") for s in useful_columns.strip("[]").split(";")
    ]
    new_columns = get_dict(columns)

    new_df = (data.dropna(how="all").rename(columns=new_columns))[useful_columns]

    new_df.reset_index(inplace=True, drop=True)
    return new_df


def prep(
    green_data: pd.DataFrame, yellow_data: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Define useful columns needed for the Azure Machine Learning NYC Taxi tutorial

    useful_columns = str(
        [
            "cost",
            "distance",
            "dropoff_datetime",
            "dropoff_latitude",
            "dropoff_longitude",
            "passengers",
            "pickup_datetime",
            "pickup_latitude",
            "pickup_longitude",
            "store_forward",
            "vendor",
        ]
    ).replace(",", ";")
    print(useful_columns)

    # Rename columns as per Azure Machine Learning NYC Taxi tutorial
    green_columns = str(
        {
            "vendorID": "vendor",
            "lpepPickupDatetime": "pickup_datetime",
            "lpepDropoffDatetime": "dropoff_datetime",
            "storeAndFwdFlag": "store_forward",
            "pickupLongitude": "pickup_longitude",
            "pickupLatitude": "pickup_latitude",
            "dropoffLongitude": "dropoff_longitude",
            "dropoffLatitude": "dropoff_latitude",
            "passengerCount": "passengers",
            "fareAmount": "cost",
            "tripDistance": "distance",
        }
    ).replace(",", ";")

    yellow_columns = str(
        {
            "vendorID": "vendor",
            "tpepPickupDateTime": "pickup_datetime",
            "tpepDropoffDateTime": "dropoff_datetime",
            "storeAndFwdFlag": "store_forward",
            "startLon": "pickup_longitude",
            "startLat": "pickup_latitude",
            "endLon": "dropoff_longitude",
            "endLat": "dropoff_latitude",
            "passengerCount": "passengers",
            "fareAmount": "cost",
            "tripDistance": "distance",
        }
    ).replace(",", ";")

    print("green_columns: " + green_columns)
    print("yellow_columns: " + yellow_columns)

    green_data_clean = cleanseData(green_data, green_columns, useful_columns)
    yellow_data_clean = cleanseData(yellow_data, yellow_columns, useful_columns)

    # Append yellow data to green data
    combined_df = green_data_clean.append(yellow_data_clean, ignore_index=True)
    combined_df.reset_index(inplace=True, drop=True)

    return green_data_clean, yellow_data_clean, combined_df
