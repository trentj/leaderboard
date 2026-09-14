import argparse
import pathlib
import sqlite3

from db_utils import convert_workbook, create_db
from python_calamine import CalamineWorkbook


if __name__ == "__main__":
    parser = argparse.ArgumentParser("create_db",
                                     description="Convert a game statistics spreadsheet into a SQLite3 database")
    parser.add_argument("-c", "--create", nargs='?', const='schema.sql', type=pathlib.Path,
                        help="Create a new database using the schema file")
    parser.add_argument("-o", "--out", default='results.db', type=pathlib.Path,
                        help="Choose the output filename")
    parser.add_argument("workbook", type=pathlib.Path, help="The workbook (.xlsx or .ods) to be converted")
    args = parser.parse_args()

    wb = CalamineWorkbook.from_path(args.workbook)
    db = sqlite3.connect(args.out)
    if args.create:
        create_db(db)
    convert_workbook(db, wb)
