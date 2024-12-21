#!/usr/bin/env python

"""
    script to download trades.
    set the absoluate path destination folder for STORE_DIRECTORY, and run

    e.g. STORE_DIRECTORY=/data/ ./download-trade.py

"""

import os
import sys
from datetime import *
import pandas as pd
from enums import *
from utility import (
    check_link_in_html,
    download_file,
    find_file_in_s3_bucket,
    get_all_symbols,
    get_html_page,
    get_parser,
    get_start_end_date_objects,
    convert_to_date_object,
    get_path,
    list_objects_in_s3_bucket,
)

import pickle
import concurrent.futures
from datetime import datetime


def download_monthly_files(
    symbol,
    year,
    month,
    trading_type,
    date_range,
    folder,
    checksum,
    start_date,
    end_date,
):
    current_date = convert_to_date_object("{}-{}-01".format(year, month))
    if current_date >= start_date and current_date <= end_date:
        path = get_path(trading_type, "trades", "monthly", symbol)
        file_name = "{}-trades-{}-{}.zip".format(
            symbol.upper(), year, "{:02d}".format(month)
        )
        download_file(path, file_name, date_range, folder, print_progress=False)

        if checksum == 1:
            checksum_path = get_path(trading_type, "trades", "monthly", symbol)
            checksum_file_name = "{}-trades-{}-{}.zip.CHECKSUM".format(
                symbol.upper(), year, "{:02d}".format(month)
            )
            download_file(
                checksum_path,
                checksum_file_name,
                date_range,
                folder,
                print_progress=False,
            )


def download_monthly_trades(
    trading_type,
    symbols,
    num_symbols,
    years,
    months,
    start_date,
    end_date,
    folder,
    checksum,
):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print(
            "[{}/{}] - start download monthly {} trades ".format(
                current + 1, num_symbols, symbol
            )
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            print(f"Using max {executor._max_workers} workers")
            futures = []
            for year in years:
                for month in months:
                    futures.append(
                        executor.submit(
                            download_monthly_files,
                            symbol,
                            year,
                            month,
                            trading_type,
                            date_range,
                            folder,
                            checksum,
                            start_date,
                            end_date,
                        )
                    )
            concurrent.futures.wait(futures)
        current += 1


def download_daily_trades(
    trading_type, symbols, num_symbols, dates, start_date, end_date, folder, checksum
):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    print("Found {} symbols".format(num_symbols))
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:

        for symbol in symbols:
            print(
                "[{}/{}] - start download daily {} trades ".format(
                    current + 1, num_symbols, symbol
                )
            )
            task_count = 0
            progress_filename = f"{symbol}.prg"
            file_path = f"{folder}/{progress_filename}"
            files_processed = load_progress(file_path)
            bucket_file_list = None
            futures = []
            not_existing_files = []
            for date in dates:
                if date not in files_processed:
                    if bucket_file_list is None:
                        bucket_file_list = list_objects_in_s3_bucket(
                            "data.binance.vision",
                            get_path(trading_type, "trades", "daily", symbol),
                        )
                    file_name = get_daily_filename(symbol, date)
                    is_in_html = find_file_in_s3_bucket(bucket_file_list, file_name)
                    if is_in_html:
                        futures.append(
                            executor.submit(
                                download_daily_files,
                                trading_type,
                                start_date,
                                end_date,
                                folder,
                                checksum,
                                date_range,
                                symbol,
                                date,
                            )
                        )
                    else:
                        not_existing_files.append(date)
            for future in concurrent.futures.as_completed(futures):
                files_processed.append(future.result())

            # print(f"Tasks scheduled: {task_count} Tasks completed: {len(files_processed)}")
            files_processed.extend(not_existing_files)
            serialize_progress(file_path, files_processed)
            current += 1


def serialize_progress(filename, data):
    with open(filename, "wb") as file:
        pickle.dump(data, file)


def deserialize_progress(filename):
    with open(filename, "rb") as file:
        return pickle.load(file)


def load_progress(filename):
    if os.path.exists(filename):
        try:
            return deserialize_progress(filename)
        except pickle.UnpicklingError as e:
            print(f"Error deserializing data from file: {e}")
            return None
    else:
        return []


def download_daily_files(
    trading_type, start_date, end_date, folder, checksum, date_range, symbol, date
):
    current_date = convert_to_date_object(date)
    if current_date >= start_date and current_date <= end_date:
        path = get_path(trading_type, "trades", "daily", symbol)
        file_name = get_daily_filename(symbol, date)
        download_file(path, file_name, date_range, folder, print_progress=False)

        if checksum == 1:
            checksum_path = get_path(trading_type, "trades", "daily", symbol)
            checksum_file_name = "{}-trades-{}.zip.CHECKSUM".format(
                symbol.upper(), date
            )
            download_file(checksum_path, checksum_file_name, date_range, folder)
    return date


def get_daily_filename(symbol, date):
    file_name = "{}-trades-{}.zip".format(symbol.upper(), date)
    return file_name


if __name__ == "__main__":
    parser = get_parser("trades")

    if len(sys.argv) == 1:
        sys.argv.append("-folder")
        sys.argv.append("D:\\BigData\\BinanceDaily\\")
        sys.argv.append("-skip-monthly")
        sys.argv.append("1")
        sys.argv.append("-t")
        sys.argv.append("spot")
    args = parser.parse_args(sys.argv[1:])

    if not args.symbols:
        print("fetching all symbols from exchange")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)
        print("fetching {} symbols from exchange".format(num_symbols))

    if args.dates:
        dates = args.dates
    else:
        period = convert_to_date_object(
            datetime.today().strftime("%Y-%m-%d")
        ) - convert_to_date_object(PERIOD_START_DATE)
        dates = (
            pd.date_range(
                end=datetime.today(),
                periods=period.days + 1,
            )
            .to_pydatetime()
            .tolist()
        )
        # Don't try do download data for today
        dates = dates[:-1]
        dates = [date.strftime("%Y-%m-%d") for date in dates]
        if args.skip_monthly == 0:
            download_monthly_trades(
                args.type,
                symbols,
                num_symbols,
                args.years,
                args.months,
                args.startDate,
                args.endDate,
                args.folder,
                args.checksum,
            )
    if args.skip_daily == 0:
        download_daily_trades(
            args.type,
            symbols,
            num_symbols,
            dates,
            args.startDate,
            args.endDate,
            args.folder,
            args.checksum,
        )
