import streamlit as st
import pandas as pd
import mysql.connector
import datetime

st.set_page_config(layout="wide")

# Константа для смещения времени
TIME_OFFSET = datetime.timedelta(hours=3)

def init_connection():
    return mysql.connector.connect(
        host="185.120.57.125",
        user="admin",
        password="v8S7b$82j51d1",
        database="crypto"
    )

def get_connection():
    if 'conn' not in st.session_state:
        st.session_state.conn = init_connection()
    return st.session_state.conn

@st.cache_data(ttl=5*60)
def fetch_data(_conn, date_from=None, date_to=None):
    query = """
    SELECT 
        *
    FROM ray_solana_parser
    """

    if date_from and date_to:
        query += " WHERE DATE BETWEEN %s AND %s"
        query += " ORDER BY DATE desc"
        df = pd.read_sql(query, _conn, params=[date_from, date_to])
    else:
        query += " ORDER BY DATE desc"
        df = pd.read_sql(query, _conn)
    
    return df

def create_summary_table(df):
    buys = df[['received_currency', 'wallet_address', 'swapped_value_USD']].rename(columns={
        'received_currency': 'coin',
        'swapped_value_USD': 'volume'
    })
    buys['transaction_type'] = 'buy'

    sells = df[['swapped_currency', 'wallet_address', 'swapped_value_USD']].rename(columns={
        'swapped_currency': 'coin',
        'swapped_value_USD': 'volume'
    })
    sells['transaction_type'] = 'sell'

    combined = pd.concat([buys, sells])

    summary = combined.groupby(['coin', 'transaction_type']).agg({
        'wallet_address': 'nunique',
        'volume': 'sum'
    }).reset_index()

    summary_pivot = summary.pivot(index='coin', columns='transaction_type', 
                                  values=['wallet_address', 'volume'])
    
    summary_pivot.columns = [f'{col[1]}_{col[0]}' for col in summary_pivot.columns]
    summary_pivot = summary_pivot.reset_index()
    
    column_mapping = {
        'buy_wallet_address': 'buy_wallets',
        'sell_wallet_address': 'sell_wallets',
        'buy_volume': 'buy_volume',
        'sell_volume': 'sell_volume'
    }
    summary_pivot = summary_pivot.rename(columns=column_mapping)
    
    summary_pivot = summary_pivot.fillna(0)
    
    summary_pivot = summary_pivot.sort_values('buy_wallets', ascending=False)
    
    return summary_pivot

def create_wallet_summary(df, selected_coins):
    filtered_df = df[(df['swapped_currency'].isin(selected_coins)) | (df['received_currency'].isin(selected_coins))]
    
    buys = filtered_df[filtered_df['received_currency'].isin(selected_coins)]
    sells = filtered_df[filtered_df['swapped_currency'].isin(selected_coins)]
    
    buy_summary = buys.groupby('wallet_address').agg({
        'received_currency': 'count',
        'swapped_value_USD': 'sum'
    }).rename(columns={'received_currency': 'unique_buy_transactions', 'swapped_value_USD': 'buy_volume'})
    
    sell_summary = sells.groupby('wallet_address').agg({
        'swapped_currency': 'count',
        'swapped_value_USD': 'sum'
    }).rename(columns={'swapped_currency': 'unique_sell_transactions', 'swapped_value_USD': 'sell_volume'})
    
    wallet_summary = pd.merge(buy_summary, sell_summary, on='wallet_address', how='outer').fillna(0)
    wallet_summary = wallet_summary.reset_index()
    
    # Добавление ссылки на dexcheck.ai
    wallet_summary['wallet_link'] = wallet_summary['wallet_address'].apply(lambda x: f"https://dexcheck.ai/app/wallet-analyzer/{x}")
    
    return wallet_summary

def update_date_range(start_date, end_date):
    st.session_state.date_range = [start_date, end_date]

def get_current_time_with_offset():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) + TIME_OFFSET

def get_last_2_hours_range():
    end_date = get_current_time_with_offset()
    start_date = end_date - datetime.timedelta(hours=2)
    return start_date, end_date

def main():
    st.title("Solana Parser Dashboard")

    # Инициализация date_range последними 2 часами по умолчанию
    if 'date_range' not in st.session_state:
        st.session_state.date_range = get_last_2_hours_range()

    st.sidebar.subheader("Быстрый выбор дат")
    if st.sidebar.button("Последние 2 часа"):
        start_date, end_date = get_last_2_hours_range()
        update_date_range(start_date, end_date)
        #st.rerun()
        
    if st.sidebar.button("Последние 6 часов"):
        end_date = get_current_time_with_offset()
        start_date = end_date - datetime.timedelta(hours=6)
        update_date_range(start_date, end_date)
        #st.rerun()
    if st.sidebar.button("Последние 24 часа"):
        end_date = get_current_time_with_offset()
        start_date = end_date - datetime.timedelta(hours=25)
        update_date_range(start_date, end_date)
    if st.sidebar.button("Последние 3 дня"):
        end_date = get_current_time_with_offset()
        start_date = end_date - datetime.timedelta(days=3, hours=1)
        update_date_range(start_date, end_date)
    if st.sidebar.button("Последние 7 дней"):
        end_date = get_current_time_with_offset()
        start_date = end_date - datetime.timedelta(days=7, hours=1)
        update_date_range(start_date, end_date)
    if st.sidebar.button("Текущий месяц"):
        end_date = get_current_time_with_offset()
        start_date = end_date.replace(day=1, hour=0, minute=0, second=0) - datetime.timedelta(hours=1)
        update_date_range(start_date, end_date)
    if st.sidebar.button("Все время"):
        end_date = get_current_time_with_offset()
        start_date = datetime.datetime(2000, 1, 1)
        update_date_range(start_date, end_date)

    date_from = st.sidebar.date_input("Начальная дата", st.session_state.date_range[0])
    date_to = st.sidebar.date_input("Конечная дата", st.session_state.date_range[1])

    time_from = st.sidebar.time_input("Время начала", st.session_state.date_range[0].time())
    time_to = st.sidebar.time_input("Время окончания", st.session_state.date_range[1].time())

    date_from = datetime.datetime.combine(date_from, time_from)
    date_to = datetime.datetime.combine(date_to, time_to)

    if date_from != st.session_state.date_range[0] or date_to != st.session_state.date_range[1]:
        st.session_state.date_range = [date_from, date_to]

    if date_from and date_to:
        conn = get_connection()
        df = fetch_data(conn, date_from, date_to)

        st.subheader(f"Сводная информация по монетам с {date_from} по {date_to}")
        summary_df = create_summary_table(df)
        
        summary_df.insert(0, 'Select', False)
        
        edited_df = st.data_editor(
            summary_df,
            column_config={
                "Select": st.column_config.CheckboxColumn(label="Выбрать"),
                "coin": "Монета",
                "buy_wallets": "Кошельки (покупка)",
                "sell_wallets": "Кошельки (продажа)",
                "buy_volume": "Объем покупок",
                "sell_volume": "Объем продаж"
            },
            disabled=["coin", "buy_wallets", "sell_wallets", "buy_volume", "sell_volume"],
            hide_index=True,
            use_container_width=True
        )

        selected_coins = edited_df[edited_df['Select']]['coin'].tolist()

        if selected_coins:
            filtered_df = df[(df['swapped_currency'].isin(selected_coins)) | (df['received_currency'].isin(selected_coins))]
            
            st.subheader(f"Сводная информация по кошелькам для выбранных монет")
            wallet_summary_df = create_wallet_summary(filtered_df, selected_coins)
            
            st.data_editor(
                wallet_summary_df,
                column_config={
                    "wallet_address": "Кошелек",
                    "wallet_link": st.column_config.LinkColumn(
                    label="Анализ кошелька", 
                    display_text="Link",),
                    "unique_buy_transactions": "Уникальные покупки",
                    "buy_volume": "Объем покупок",
                    "unique_sell_transactions": "Уникальные продажи",
                    "sell_volume": "Объем продаж"
                },
                hide_index=True,
                use_container_width=True
            )

            st.subheader(f"Детальные данные с {date_from} по {date_to}")
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.warning("Пожалуйста, выберите хотя бы одну монету для отображения детальной информации.")

    else:
        st.error("Пожалуйста, выберите диапазон дат.")

if __name__ == "__main__":
    main()
