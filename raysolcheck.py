import streamlit as st
import pandas as pd
import mysql.connector
import datetime
from sqlalchemy import create_engine

st.set_page_config(layout="wide")

# Константа для смещения времени
TIME_OFFSET = datetime.timedelta(hours=3)

def init_connection():
    # Создаем SQLAlchemy engine вместо прямого подключения
    return create_engine(
        'mysql+mysqlconnector://admin:v8S7b$82j51d1@185.120.57.125/crypto'
    )

def get_connection():
    if 'conn' not in st.session_state:
        st.session_state.conn = init_connection()
    return st.session_state.conn

@st.cache_data(ttl=5*60)
def fetch_data(_conn, date_from=None, date_to=None):
    try:
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
        
        # Заменяем None на пустую строку в столбце name_wallet
        df['name_wallet'] = df['name_wallet'].fillna('')
        return df
    except Exception as e:
        st.error(f"Ошибка при получении данных: {str(e)}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame в случае ошибки

def create_summary_table(df):
    if df.empty:
        return pd.DataFrame(columns=['coin', 'buy_wallets', 'sell_wallets', 'buy_volume', 'sell_volume'])
        
    try:
        buys = df[['received_currency', 'name_wallet', 'swapped_value_USD']].rename(columns={
            'received_currency': 'coin',
            'swapped_value_USD': 'volume'
        })
        buys['transaction_type'] = 'buy'

        sells = df[['swapped_currency', 'name_wallet', 'swapped_value_USD']].rename(columns={
            'swapped_currency': 'coin',
            'swapped_value_USD': 'volume'
        })
        sells['transaction_type'] = 'sell'

        combined = pd.concat([buys, sells])

        summary = combined.groupby(['coin', 'transaction_type']).agg({
            'name_wallet': 'nunique',
            'volume': 'sum'
        }).reset_index()

        summary_pivot = summary.pivot(index='coin', columns='transaction_type', 
                                    values=['name_wallet', 'volume'])
        
        summary_pivot.columns = [f'{col[1]}_{col[0]}' for col in summary_pivot.columns]
        summary_pivot = summary_pivot.reset_index()
        
        # Убедимся, что все нужные столбцы существуют
        required_columns = ['coin', 'buy_name_wallet', 'sell_name_wallet', 'buy_volume', 'sell_volume']
        for col in required_columns:
            if col not in summary_pivot.columns:
                if col == 'coin':
                    summary_pivot[col] = ''
                else:
                    summary_pivot[col] = 0
        
        column_mapping = {
            'buy_name_wallet': 'buy_wallets',
            'sell_name_wallet': 'sell_wallets',
            'buy_volume': 'buy_volume',
            'sell_volume': 'sell_volume'
        }
        summary_pivot = summary_pivot.rename(columns=column_mapping)
        
        summary_pivot = summary_pivot.fillna(0)
        
        # Безопасная сортировка
        if 'buy_wallets' in summary_pivot.columns:
            summary_pivot = summary_pivot.sort_values('buy_wallets', ascending=False)
        
        return summary_pivot
    except Exception as e:
        st.error(f"Ошибка при создании сводной таблицы: {str(e)}")
        return pd.DataFrame(columns=['coin', 'buy_wallets', 'sell_wallets', 'buy_volume', 'sell_volume'])

def create_wallet_summary(df, selected_coins):
    if df.empty or not selected_coins:
        return pd.DataFrame(columns=['wallet_address', 'unique_buy_transactions', 'buy_volume', 
                                   'unique_sell_transactions', 'sell_volume', 'wallet_link'])
    
    try:
        filtered_df = df[(df['swapped_currency'].isin(selected_coins)) | 
                        (df['received_currency'].isin(selected_coins))]
        
        buys = filtered_df[filtered_df['received_currency'].isin(selected_coins)]
        sells = filtered_df[filtered_df['swapped_currency'].isin(selected_coins)]
        
        buy_summary = buys.groupby('wallet_address').agg({
            'received_currency': 'count',
            'swapped_value_USD': 'sum'
        }).rename(columns={'received_currency': 'unique_buy_transactions', 
                         'swapped_value_USD': 'buy_volume'})
        
        sell_summary = sells.groupby('wallet_address').agg({
            'swapped_currency': 'count',
            'swapped_value_USD': 'sum'
        }).rename(columns={'swapped_currency': 'unique_sell_transactions', 
                         'swapped_value_USD': 'sell_volume'})
        
        wallet_summary = pd.merge(buy_summary, sell_summary, 
                                on='wallet_address', how='outer').fillna(0)
        wallet_summary = wallet_summary.reset_index()
        
        wallet_summary['wallet_link'] = wallet_summary['wallet_address'].apply(
            lambda x: f"https://dexcheck.ai/app/wallet-analyzer/{x}" if pd.notna(x) else ""
        )
        
        return wallet_summary
    except Exception as e:
        st.error(f"Ошибка при создании сводки по кошелькам: {str(e)}")
        return pd.DataFrame(columns=['wallet_address', 'unique_buy_transactions', 'buy_volume', 
                                   'unique_sell_transactions', 'sell_volume', 'wallet_link'])

def update_date_range(start_date, end_date):
    st.session_state.date_range = [start_date, end_date]

def get_current_time_with_offset():
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) + TIME_OFFSET

def get_last_2_hours_range():
    end_date = get_current_time_with_offset()
    start_date = end_date - datetime.timedelta(hours=2)
    return start_date, end_date

def filter_by_wallets(df, selected_wallets):
    if df.empty:
        return df
    if selected_wallets:
        return df[df['name_wallet'].isin(selected_wallets)]
    return df

def main():
    try:
        st.title("Ray_Sol Parser Dashboard")

        if 'date_range' not in st.session_state:
            st.session_state.date_range = get_last_2_hours_range()

        st.sidebar.subheader("Быстрый выбор дат")
        if st.sidebar.button("Последние 2 часа"):
            start_date, end_date = get_last_2_hours_range()
            update_date_range(start_date, end_date)
            
        if st.sidebar.button("Последние 6 часов"):
            end_date = get_current_time_with_offset()
            start_date = end_date - datetime.timedelta(hours=6)
            update_date_range(start_date, end_date)

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
            
            if not df.empty:
                st.sidebar.subheader("Фильтр по кошелькам")
                # Фильтруем None и пустые строки перед сортировкой
                valid_wallets = [w for w in df['name_wallet'].unique() if w]
                all_wallets = sorted(valid_wallets)
                selected_wallets = st.sidebar.multiselect(
                    "Выберите кошельки",
                    options=all_wallets,
                    default=[]
                )
                
                df = filter_by_wallets(df, selected_wallets)

                st.subheader(f"Сводная информация по монетам с {date_from} по {date_to}")
                summary_df = create_summary_table(df)
                
                if not summary_df.empty:
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
                        filtered_df = df[(df['swapped_currency'].isin(selected_coins)) | 
                                      (df['received_currency'].isin(selected_coins))]
                        
                        st.subheader(f"Сводная информация по кошелькам для выбранных монет")
                        wallet_summary_df = create_wallet_summary(filtered_df, selected_coins)
                        
                        if not wallet_summary_df.empty:
                            st.data_editor(
                                wallet_summary_df,
                                column_config={
                                    "wallet_address": "Кошелек",
                                    "wallet_link": st.column_config.LinkColumn(
                                        label="Анализ кошелька", 
                                        display_text="Link"
                                    ),
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
                st.warning("Нет данных для выбранного периода.")
        else:
            st.error("Пожалуйста, выберите диапазон дат.")
    except Exception as e:
        st.error(f"Произошла ошибка: {str(e)}")

if __name__ == "__main__":
    main()
