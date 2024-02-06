# create a streamlit app to visualize the data
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.connector import get_connector
import datetime

data_table = 'maxa_snbx_clt_fresh.snbx_data_mart_emile_dimas.base'

# set streamlit wide mode
st.set_page_config(layout="wide")

def main():
    st.title('Data Visualization')
    # get data
    connector = get_connector()
    connector.cursor().execute('USE ROLE MAXA_SNBX_CLT_FRESH')

    mapping_query = f"""
    select  distinct classification_code, customer_code, category from {data_table}
    """
    mapping = pd.read_sql_query(mapping_query, connector)

    cols = st.columns(6)
    classification_code = cols[0].selectbox(
        'Classification Code', 
        mapping['CLASSIFICATION_CODE'].unique(),
        # key='key_classification_code'
        )
    mapping_1 = (
        mapping
        [lambda d: d['CLASSIFICATION_CODE'] == classification_code]
    )

    customer_code = cols[1].selectbox(
        'Customer Code', 
        mapping_1['CUSTOMER_CODE'].unique(),
        # key='key_customer_code'
        )
    mapping_2 = (
        mapping_1
        [lambda d: d['CUSTOMER_CODE'] == customer_code]
    )
    category = cols[2].selectbox(
        'Category', 
        mapping_2['CATEGORY'].unique()
        )
    time_granularitties = ['Daily', 'Weekly', 'Monthly', 'Quartely', 'Yearly']
    time_granularity = cols[3].selectbox('Granularity', time_granularitties)
    # as_of_date = .date_input('As of Date')
    

    query = (
        f"select * from {data_table} where"
        f" classification_code = '{classification_code}'"
        f" and customer_code = '{customer_code}'"
        f" and category = '{category}'"
    )
    df = pd.read_sql_query(query, connector)
    aggregation = cols[4].selectbox('Aggregation', ['sum', 'mean'])

    selected_date = cols[5].slider("Select a date:", df.DATE_TRANSACTION.min(), datetime.datetime.now().date())
    # # display data
    df = add_features(df, time_granularity)
    cols = st.columns(2)
    cols[0].write(df)
    cols[1].plotly_chart(plot_ts(df=df))

    df_daily = aggregate_period(df, 'Daily', aggregation=aggregation)
    df_weekly = aggregate_period(df, 'Weekly', aggregation=aggregation)
    df_monthly = aggregate_period(df, 'Monthly', aggregation=aggregation)
    df_quartely = aggregate_period(df, 'Quartely', aggregation=aggregation)
    df_yearly = aggregate_period(df, 'Yearly', aggregation=aggregation)

    cols = st.columns(2)
    cols[0].write(df_daily)
    cols[1].plotly_chart(plot_ts(df=df_daily, date_col='date'))

    cols = st.columns(2)
    cols[0].write(df_weekly)
    cols[1].plotly_chart(plot_ts(df=df_weekly, date_col='date'))

    cols = st.columns(2)
    cols[0].write(df_monthly)
    cols[1].plotly_chart(plot_ts(df=df_monthly, date_col='date'))

    cols = st.columns(2)
    cols[0].write(df_quartely)
    cols[1].plotly_chart(plot_ts(df=df_quartely, date_col='date'))

    cols = st.columns(2)
    cols[0].write(df_yearly)
    cols[1].plotly_chart(plot_ts(df=df_yearly, date_col='date'))



# add missing dates to the data
def add_missing_dates(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    # get the min and max date
    import datetime
    min_date = df[date_col].min()
    max_date = datetime.datetime.now().strftime('%Y-%m-%d')
    # create a date range
    date_range = pd.date_range(min_date, max_date)
    # create a dataframe with the date range
    date_df = pd.DataFrame(date_range, columns=[date_col])
    # merge the dataframes
    df = df.merge(date_df, on=date_col, how='right')
    # forward fill the missing values for columns 'classification_code'
    df['CLASSIFICATION_CODE'] = df['CLASSIFICATION_CODE'].ffill()
    df['CUSTOMER_CODE'] = df['CUSTOMER_CODE'].ffill()
    df['CUSTOMER_NAME'] = df['CUSTOMER_NAME'].ffill()
    df['CATEGORY'] = df['CATEGORY'].ffill()
    df['ACCOUNT_OPENING_DATE'] = df['ACCOUNT_OPENING_DATE'].ffill()
    df['UNIT_MEASURE'] = df['UNIT_MEASURE'].ffill()
    df = df.fillna(0)

    return df


def filter_date(df: pd.DataFrame, limit_date: str) -> pd.DataFrame:
    df['DATE_TRANSACTION'] = pd.to_datetime(df['DATE_TRANSACTION'])
    df = df[df['DATE_TRANSACTION'] <= limit_date]
    return df


def add_features(df: pd.DataFrame, periodicity: str) -> pd.DataFrame:
    df['DATE_TRANSACTION'] = pd.to_datetime(df['DATE_TRANSACTION'])
    df = add_missing_dates(df, 'DATE_TRANSACTION')
    df['day_of_week'] = df['DATE_TRANSACTION'].dt.day_of_week
    df['month'] = df['DATE_TRANSACTION'].dt.month
    df['year'] = df['DATE_TRANSACTION'].dt.year
    df['week'] = df['DATE_TRANSACTION'].dt.isocalendar().week
    df['quarter'] = df['DATE_TRANSACTION'].dt.quarter
    df['date_weekly'] = df['DATE_TRANSACTION'].dt.to_period('W').dt.start_time
    df['date_monthly'] = df['DATE_TRANSACTION'].dt.to_period('M').dt.start_time
    df['date_quartely'] = df['DATE_TRANSACTION'].dt.to_period('Q').dt.start_time
    df['date_yearly'] = df['DATE_TRANSACTION'].dt.to_period('Y').dt.start_time

    return df


def aggregate_period(df: pd.DataFrame, periodicity=None, aggregation: str = 'sum'):
    dim_cols = ['CLASSIFICATION_CODE', 'ACCOUNT_OPENING_DATE', 'CUSTOMER_CODE', 'CUSTOMER_NAME', 'CATEGORY', 'UNIT_MEASURE']

    agg_dict = {
        'TOTAL_AMOUNT': aggregation,
        'TOTAL_INVOICES': aggregation,
        'TOTAL_QUANTITY_VOLUME': aggregation
    }
    if periodicity == 'Daily':
        return df.rename(columns={'DATE_TRANSACTION': 'date'})

    if periodicity == 'Weekly':
        return df.groupby(['date_weekly'] + dim_cols).agg(
            agg_dict).reset_index().rename(columns={'date_weekly': 'date'})

    if periodicity == 'Monthly':
        return df.groupby(['date_monthly'] + dim_cols).agg(
            agg_dict).reset_index().rename(columns={'date_monthly': 'date'})

    if periodicity == 'Quartely':
        return df.groupby(['date_quartely'] + dim_cols).agg(
            agg_dict).reset_index().rename(columns={'date_quartely': 'date'})

    if periodicity == 'Yearly':
        return df.groupby(['date_yearly'] + dim_cols).agg(
            agg_dict).reset_index().rename(columns={'date_yearly': 'date'})

    return df.rename(columns={'DATE_TRANSACTION': 'date'})


def plot_ts(df: pd.DataFrame, date_col: str = 'DATE_TRANSACTION'):
    unit = df['UNIT_MEASURE'].unique()[0]

    traces = [
        go.Scatter(
            x=df[date_col],
            y=df['TOTAL_AMOUNT'],
            mode='lines',
            name='Amount ($)'
        ),
        # go.Scatter(
        #     x=df['DATE_TRANSACTION'],
        #     y=df['TOTAL_INVOICES'],
        #     mode='lines',
        #     name='invoice #'
        # ),
        # go.Scatter(
        #     x=df['DATE_TRANSACTION'],
        #     y=df['TOTAL_QUANTITY_VOLUME'],
        #     mode='lines',
        #     name=unit
        # )
    ]
    return go.Figure(data=traces)


if __name__ == '__main__':
    main()
