from behave.__main__ import main as behave_main
from behave import configuration
from argparse import ArgumentParser
import os

def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--root", default="tests/openCypher_M05/tck/features", 
            help="Path to folder where tests are located, default is openCypher_M05/tck/features.")
    argp.add_argument("--graphs-root", default="tests/openCypher_M05/tck/graphs", 
            help="Path to folder where files with graphs queries are located, default is openCypher_M05/tck/graphs.")
    argp.add_argument("--stop", action="store_true", help="Stop testing after first fail.")
    argp.add_argument("--no-side-effects", action="store_true", help="Check for side effects in tests.")
    argp.add_argument("--database", default="neo4j", choices=["neo4j", "memgraph"], help="Default is neo4j.")
    argp.add_argument("--database-username", default="neo4j", help="Default is neo4j.")
    argp.add_argument("--database-password", default="memgraph", help="Default is memgraph.")
    argp.add_argument("--database-uri", default="bolt://localhost:7687", help="Default is bolt://localhost:7687.")
    argp.add_argument("--output-folder", default="results/", help="Test result output folder, default is results/.")
    argp.add_argument("--logging", default="DEBUG", choices=["INFO", "DEBUG"], help="Logging level, default is DEBUG.")
    return argp.parse_args()

def main():
    """
    Script used to run behave tests with given options. List of 
    options is available when running python test_executor.py -help.
    """
    args = parse_args()

    tests_root = os.path.abspath(args.root)

    #adds options to cucumber configuration
    add_config("--no-side-effects", dict(action="store_true", help = "Exclude side effects."))
    add_config("--database", dict(help = "Choose database(memgraph/neo4j)."))
    add_config("--database-password", dict(help = "Database password."))
    add_config("--database-username", dict(help = "Database username."))
    add_config("--database-uri", dict(help = "Database uri."))
    add_config("--graphs-root", dict(help = "Path to folder where graphs are given."))
    add_config("--output-folder", dict(help = "Folder where results of tests are written."))
    add_config("--root", dict(help = "Folder with test features."))


    #list with all options
    #options will be passed to the cucumber engine
    behave_options = [tests_root]
    if args.stop:
        behave_options.append("--stop")
    if args.no_side_effects:
        behave_options.append("--no-side-effects")
    behave_options.append("--database")
    behave_options.append(args.database)
    behave_options.append("--database-password")
    behave_options.append(args.database_password)
    behave_options.append("--database-username")
    behave_options.append(args.database_username)
    behave_options.append("--database-uri")
    behave_options.append(args.database_uri)
    behave_options.append("--graphs-root")
    behave_options.append(args.graphs_root)
    behave_options.append("--output-folder")
    behave_options.append(args.output_folder)
    behave_options.append("--root")
    behave_options.append(args.root)

    #runs tests with options
    behave_main(behave_options)


def add_config(option, dictionary):
    configuration.options.append(
        ((option,), dictionary)
    )

if __name__ == '__main__':
    main()
