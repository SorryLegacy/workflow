from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import PolynomialFeatures
import numpy as np

def predict_bitcoin_price():
    try:
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow', connect_args={"options": "-csearch_path=_raw_data"})
        with engine.connect() as con:
            query = text("""SELECT time, usd FROM btc_price""")
            data = pd.read_sql(query, con)

        data['date'] = pd.to_datetime(data['time'], unit="s")
        data['date_ordinal'] = data['date'].map(pd.Timestamp.toordinal)
        data['year'] = data['date'].dt.year
        data['month'] = data['date'].dt.month
        data['day'] = data['date'].dt.day
        data['day_of_week'] = data['date'].dt.dayofweek

        data['usd_lag1'] = data['usd'].shift(1)

        data.dropna(inplace=True)

        features = ['date_ordinal', 'year', 'month', 'day', 'day_of_week', 'usd_lag1']
        X = data[features]
        y = data['usd']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        poly = PolynomialFeatures(degree=2)
        X_train_poly = poly.fit_transform(X_train)
        X_test_poly = poly.transform(X_test)

        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train_poly, y_train)

        predictions = model.predict(X_test_poly)
        mse = np.mean((predictions - y_test) ** 2)
        print(f'Mean Squared Error: {mse}')

        cv_scores = cross_val_score(model, X_train_poly, y_train, cv=5, scoring='neg_mean_squared_error')
        avg_cv_mse = -np.mean(cv_scores)
        print(f'Cross-validated Mean Squared Error: {avg_cv_mse}')
    except Exception as e:
        print(f"Error in task: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 21),
    'retries': 1,
}

with DAG(
    'bitcoin_price_prediction',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    predict_task = PythonOperator(
        task_id='predict_bitcoin_price',
        python_callable=predict_bitcoin_price,
    )

    predict_task
