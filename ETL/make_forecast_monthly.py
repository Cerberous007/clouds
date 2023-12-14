import numpy as np
import pandas as pd
from neuralprophet import NeuralProphet
from datetime import timedelta, datetime

from ETL.connectors import LookupConnector, PrestoConnector
from ETL.dbcreds import LOOKUP_CREDS, PRESTO_CREDS

from queries.queries_for_forecast import noncoive_query, voice_query
import warnings
warnings.filterwarnings('ignore')


lookup = LookupConnector(
    creds=LOOKUP_CREDS
)


def batch_insert_data(data: pd.DataFrame, schema: str, table: str):
    """
    Функция для пакетной вставки данных в таблицу.

    Аргументы:
    - data: DataFrame с данными для вставки.
    - schema: схема таблицы.
    - table: имя таблицы.

    Возвращает:
    - None.
    """
    overall_time = 0
    for i in range(0, len(data), 5000):
        start = datetime.now()
        lookup.insert(schema=schema, table=table, data=data[i:(i+5000)])
        time_elapsed = (datetime.now() - start).total_seconds()
        print(f'Time elapsed for batch: {time_elapsed}')
        overall_time += time_elapsed
    print(f'Elapsed in total: {overall_time}s')


def data_preparing_chats() -> pd.DataFrame:
    """
    Функция для подготовки данных о чатах.

    Аргументы:
    - None.

    Возвращает:
    - data_chats: DataFrame с данными о чатах.
    """
    data = pd.DataFrame(lookup.query(query=noncoive_query)['results'])
    data.columns = ['ds', 'y', 'channel_id', 'is_agent']
    chats_channels = [19904, 19906, 21290]
    
    chats_with_oper = data[(data['channel_id'].isin(chats_channels)) & (data['is_agent']==1)]
    chats_with_oper = chats_with_oper.iloc[:,:2]
    chats_with_oper.index = chats_with_oper.ds
    
    resampled_chats = chats_with_oper.resample('H').nunique()
    resampled_chats['ds'] = resampled_chats.index
    
    data_chats = resampled_chats.copy()

    data_chats.y = np.log(data_chats.y + 1) # for night forecast only

    data_chats['I'] = np.append(0, data_chats["y"].values[1:] \
                                            - data_chats["y"].values[:-1])
    return data_chats


def data_preparing_calls() -> pd.DataFrame:
    """
    Функция для подготовки данных о звонках.

    Аргументы:
    - None.

    Возвращает:
    - data_calls: DataFrame с данными о звонках.
    """
    calls_with_oper = pd.DataFrame(lookup.query(query=voice_query)['results'])
    calls_with_oper.columns = ['ds', 'y']
    calls_with_oper.index = calls_with_oper.ds

    resampled_calls = calls_with_oper.resample('H').nunique()
    resampled_calls['ds'] = resampled_calls.index
    data_calls = resampled_calls.copy()
    
    data_calls.y = np.log(data_calls.y + 1) # for night forecast only
    data_calls['I'] = np.append(0, data_calls["y"].values[1:] \
                                            - data_calls["y"].values[:-1])
    return data_calls


def get_fitted_model_30_d(data_for_fit: pd.DataFrame):
    """
    Функция для инииалзиации и обучения модели.

    Аргументы:
    - data_for_fit: DataFrame с данными для обучения модели.

    Возвращает:
    - model: обученная модель.
    """
    model = NeuralProphet(
        n_lags = 24*35,
        n_forecasts = 24*30,
    )
    model = model.add_lagged_regressor("I", normalize="standardize")
#     m.add_country_holidays('Russia')
    df_train, df_test = model.split_df(data_for_fit, freq='H', valid_p = 1.0/10)
    metrics = model.fit(df_train, freq='H', validation_df=df_test)
    return model


def get_predictions(fitted_model, data_for_fit):
    """
    Функция для получения прогнозов.

    Аргументы:
    - fitted_model: обученная модель.
    - data_for_fit: DataFrame с данными для обучения модели.

    Возвращает:
    - result1: DataFrame с прогнозами.
    """
    future3 = fitted_model.make_future_dataframe(df=data_for_fit, periods=24*30)
    forecast3 = fitted_model.predict(df=future3, raw=True, decompose=False)

    result1 = forecast3.T[1:]
    result1.rename(columns={0:'y'}, inplace=True)
    result1['ds'] = pd.date_range(data_for_fit.ds.iloc[-1] + timedelta(hours=1), periods=24*30, freq='H')
#     result1.index=test_chats.index
    
    result1.y = np.exp(result1.y)-1
    return result1


def make_chats_predictions():
    """
    Функция для запуска всего пайплайна прогнозоривания для чатов.

    Аргументы:
    - None.

    Возвращает:
    - None.
    """
    # Собираем данные, обучаем модель и делаем прогноз для чатов
    data_chats = data_preparing_chats()
    model_chats = get_fitted_model_30_d(data_for_fit=data_chats)
    
    predictions = get_predictions(fitted_model=model_chats, data_for_fit=data_chats)
    predictions.rename(columns={'y':'chats_count'}, inplace=True)
    predictions['last_update_date'] = datetime.now().date()
    
    batch_insert_data(data=predictions, schema='dashboards', table='contact_center_monthly_predictions_chats_updated')
    return
    
    
def make_calls_predictions():
    """
    Функция для запуска всего пайплайна прогнозоривания для звонков.

    Аргументы:
    - None.

    Возвращает:
    - None.
    """
    data_calls = data_preparing_calls()
    model_calls = get_fitted_model_30_d(data_for_fit=data_calls)
    
    predictions = get_predictions(fitted_model=model_calls, data_for_fit=data_calls)
    predictions.rename(columns={'y':'calls_count'}, inplace=True)
    predictions['last_update_date'] = datetime.now().date()
    
    batch_insert_data(data=predictions, schema='dashboards', table='contact_center_monthly_predictions_calls_updated')
    return
    
    
def main():
    """
    Основная функция для запуска всего пайплайна.
    Весь скрипт делится на 2 части: работа с чатами и звонками.
    В каждой части происходит выгрузка данных и подготовка данных, дообучение модели, 
    создание прогноза и загрузка его в БД.
    По итогу получаем почасовой прогноз на следующие 30 дней. В базу сохраняется история всех прогнозов.
    Отследить историю прогнозов можно по полю last_update_date в таблицах. 

    Аргументы:
    - None.

    Возвращает:
    - None.
    """
    make_chats_predictions()
    make_calls_predictions()
    return


if __name__ == "__main__":
    main()

