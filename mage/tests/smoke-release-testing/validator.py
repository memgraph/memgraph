import argparse
import csv
import sys


def read_all_csv_from_stdin():
    return list(csv.DictReader(sys.stdin))


def validate_first_as_int(data, field, expected_value):
    assert len(data) == 1
    assert int(data[0][field]) == int(expected_value), f"Got {data[0][field]}, expected {expected_value}."
    print(f"Validation of the first {field} is OK.")


def get_main_parser(data):
    main_addr = None
    for instance in data:
        if instance["role"] == '"main"':
            main_addr = instance["management_server"][1:-1].split(".")[0]
    print(main_addr)


def validate_is_main(data):
    assert len(data) == 1
    assert data[0]["replication role"] == '"main"'


def validate_number_of_results(data, expected):
    assert len(data) == int(expected)


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Smoke Tests Validator",
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="action", required=True)

    first_as_int_args_parser = subparsers.add_parser("first_as_int", help="Validate first return value as integer")
    first_as_int_args_parser.add_argument("-f", "--field", help="Name of the field to test", required=True)
    first_as_int_args_parser.add_argument("-e", "--expected", help="Expected value", required=True)

    subparsers.add_parser("get_main_parser", help="Get main parser")
    subparsers.add_parser("validate_is_main", help="Validate if data instance is main")

    validate_number_of_results_parser = subparsers.add_parser(
        "validate_number_of_results", help="Get the number of results"
    )
    validate_number_of_results_parser.add_argument("-e", "--expected", help="Expected value", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = get_arguments()
    data = read_all_csv_from_stdin()

    if args.action == "first_as_int":
        validate_first_as_int(data, args.field, args.expected)
    if args.action == "get_main_parser":
        get_main_parser(data)
    if args.action == "validate_is_main":
        validate_is_main(data)
    if args.action == "validate_number_of_results":
        validate_number_of_results(data, args.expected)
