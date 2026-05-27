import os
import io
import logging
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from clickhouse_driver import Client
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    MessageHandler, filters, ContextTypes
)
import psycopg2
from textwrap import shorten

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CH_HOST   = os.getenv("CH_HOST", "clickhouse_user")
CH_PORT   = int(os.getenv("CH_PORT", "9000"))

PG_CONN = {
    "host":     os.getenv("PG_HOST", "postgres_user"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB", "sales_db"),
    "user":     os.getenv("PG_USER", "user"),
    "password": os.getenv("PG_PASSWORD", "user"),
}

def get_pg_conn():
    return psycopg2.connect(**PG_CONN)

def ch_client():
    return Client(host=CH_HOST, port=CH_PORT)


def get_keyboard():
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton("📊 Количество строк в БД"),
            ],
            [
                KeyboardButton("📈 Топ-10 по чеку (тек. мес.)"),
                KeyboardButton("💰 Топ-10 по прибыли (тек. мес.)"),
            ],
            [
                KeyboardButton("💰 Топ-10 по прибыли (пред. мес.)"),
                KeyboardButton("📉 Топ-10 по чеку (пред. мес.)"),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Sales Pipeline Monitor*\n\nВыбери действие:",
        parse_mode="Markdown",
        reply_markup=get_keyboard()
    )


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "📊 Количество строк в БД":
        await handle_db_row_counts(update)
    elif text == "📈 Топ-10 по чеку (тек. мес.)":
        await handle_chart_top10_receipt_cur(update)
    elif text == "💰 Топ-10 по прибыли (тек. мес.)":
        await handle_chart_top10_amount_cur(update)
    elif text == "💰 Топ-10 по прибыли (пред. мес.)":
        await handle_chart_top10_amount_prev(update)
    elif text == "📉 Топ-10 по чеку (пред. мес.)":
        await handle_chart_top10_receipt_prev(update)


async def handle_db_row_counts(update: Update):
    await update.message.reply_text("⏳ Считаю строки в базах...")

    ch_msg = ""
    pg_msg = ""

    # ClickHouse
    try:
        client = ch_client()
        ch_tables = client.execute("""
            SELECT
                name,
                total_rows
            FROM system.tables
            WHERE database = currentDatabase()
              AND engine NOT IN ('View', 'MaterializedView', 'Dictionary')
            ORDER BY name
        """)
        ch_total = sum((cnt or 0) for _, cnt in ch_tables)
        ch_lines = "\n".join(
            f"  • `{name}`: `{(cnt or 0):,}`"
            for name, cnt in ch_tables
        ) or "  • (нет таблиц)"

        ch_msg = (
            f"🟡 *ClickHouse* (итого строк: `{ch_total:,}`):\n"
            f"{ch_lines}"
        )
    except Exception as e:
        ch_msg = f"❌ ClickHouse: `{shorten(str(e), width=180)}`"

    # PostgreSQL
    try:
        pg_conn = get_pg_conn()
        try:
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT relname AS table_name,
                           n_live_tup AS row_count
                    FROM pg_stat_user_tables
                    ORDER BY relname
                """)
                rows = cur.fetchall()

            pg_tables = [(name, int(cnt)) for name, cnt in rows]
            pg_total = sum(cnt for _, cnt in pg_tables)
            pg_lines = "\n".join(
                f"  • `{name}`: `{cnt:,}`"
                for name, cnt in pg_tables
            ) or "  • (нет таблиц)"

            pg_msg = (
                f"🐘 *PostgreSQL* (итого строк: `{pg_total:,}`):\n"
                f"{pg_lines}"
            )
        finally:
            pg_conn.close()
    except Exception as e:
        pg_msg = f"❌ PostgreSQL: `{shorten(str(e), width=180)}`"

    msg = (
        "📊 *Количество строк в текущих БД*\n\n"
        f"{ch_msg}\n\n"
        f"{pg_msg}"
    )

    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=get_keyboard()
    )


def _build_barh_chart(df, x_col, y_col, title, xlabel):
    n = len(df)
    fig_height = max(6, n * 0.7)
    df[y_col] = df[y_col].astype(str)
    fig, ax = plt.subplots(figsize=(10, fig_height))
    bars = ax.barh(df[y_col][::-1], df[x_col][::-1], color="#4C72B0", height=0.6)
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Product ID")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x:,.0f}")
    )
    max_val = df[x_col].max() if len(df) > 0 else 1
    ax.set_xlim(0, max_val * 1.18)
    for bar in bars:
        w = bar.get_width()
        label = f"{w/1e6:.2f}M" if w >= 1e6 else f"{w:,.1f}"
        ax.text(w + max_val * 0.01, bar.get_y() + bar.get_height() / 2,
                label, va="center", fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130)
    buf.seek(0)
    plt.close()
    return buf


async def handle_chart_top10_receipt_cur(update: Update):
    await update.message.reply_text("⏳ Строю график...")
    try:
        client = ch_client()
        rows = client.execute("""
            select distinct
                product_id,
                avg_receipt_product_id as avg_receipt
            from sales_aggregated
            where sale_month = monthName(today())
                and sale_year = toYear(today())
            order by avg_receipt desc
            limit 10;
        """)
        if not rows:
            await update.message.reply_text(
                "ℹ️ Нет данных за текущий месяц",
                reply_markup=get_keyboard()
            )
            return
        df = pd.DataFrame(rows, columns=["product_id", "avg_receipt"])
        buf = _build_barh_chart(
            df, "avg_receipt", "product_id",
            "Топ-10 продуктов по среднему чеку\n(текущий месяц)",
            "Средний чек"
        )
        await update.message.reply_photo(
            photo=buf,
            caption="📈 Топ-10 по среднему чеку — текущий месяц",
            reply_markup=get_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка:\n`{e}`", parse_mode="Markdown", reply_markup=get_keyboard())


async def handle_chart_top10_amount_cur(update: Update):
    await update.message.reply_text("⏳ Строю график...")
    try:
        client = ch_client()
        rows = client.execute("""
            select distinct
                product_id,
                sum_amount_product_id as amount
            from sales_aggregated
            where sale_month = monthName(today())
                and sale_year = toYear(today())
            order by amount desc
            limit 10;
        """)
        if not rows:
            await update.message.reply_text(
                "ℹ️ Нет данных за текущий месяц",
                reply_markup=get_keyboard()
            )
            return
        df = pd.DataFrame(rows, columns=["product_id", "amount"])
        buf = _build_barh_chart(
            df, "amount", "product_id",
            "Топ-10 продуктов по прибыли\n(текущий месяц)",
            "Сумма продаж"
        )
        await update.message.reply_photo(
            photo=buf,
            caption="💰 Топ-10 по прибыли — текущий месяц",
            reply_markup=get_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка:\n`{e}`", parse_mode="Markdown", reply_markup=get_keyboard())


async def handle_chart_top10_amount_prev(update: Update):
    await update.message.reply_text("⏳ Строю график...")
    try:
        client = ch_client()
        rows = client.execute("""
            select distinct
                product_id,
                sum_amount_product_id as amount
            from sales_aggregated
            where sale_month = monthName(today() - interval 1 month)
                and sale_year = toYear(today())
            order by amount desc
            limit 10;
        """)
        if not rows:
            await update.message.reply_text(
                "ℹ️ Нет данных за предыдущий месяц",
                reply_markup=get_keyboard()
            )
            return
        df = pd.DataFrame(rows, columns=["product_id", "amount"])
        buf = _build_barh_chart(
            df, "amount", "product_id",
            "Топ-10 продуктов по прибыли\n(предыдущий месяц)",
            "Сумма продаж"
        )
        await update.message.reply_photo(
            photo=buf,
            caption="💰 Топ-10 по прибыли — предыдущий месяц",
            reply_markup=get_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка:\n`{e}`", parse_mode="Markdown", reply_markup=get_keyboard())


async def handle_chart_top10_receipt_prev(update: Update):
    await update.message.reply_text("⏳ Строю график...")
    try:
        client = ch_client()
        rows = client.execute("""
            select distinct
                product_id,
                avg_receipt_product_id as avg_receipt
            from sales_aggregated
            where sale_month = monthName(today() - interval 1 month)
                and sale_year = toYear(today())
            order by avg_receipt desc
            limit 10;
        """)
        if not rows:
            await update.message.reply_text(
                "ℹ️ Нет данных за предыдущий месяц",
                reply_markup=get_keyboard()
            )
            return
        df = pd.DataFrame(rows, columns=["product_id", "avg_receipt"])
        buf = _build_barh_chart(
            df, "avg_receipt", "product_id",
            "Топ-10 продуктов по среднему чеку\n(предыдущий месяц)",
            "Средний чек"
        )
        await update.message.reply_photo(
            photo=buf,
            caption="📉 Топ-10 по среднему чеку — предыдущий месяц",
            reply_markup=get_keyboard()
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка:\n`{e}`", parse_mode="Markdown", reply_markup=get_keyboard())


def main():
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN не задан в .env!")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    logger.info("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
