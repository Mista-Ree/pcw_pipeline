import click

from pipeline.pipeline import ingest, run_pipeline, transform, validate, evaluate

@click.group()
def cli():
    pass

@cli.command(help="Runs the full pipeline")
@click.option("--print-transform-output", is_flag=True, default=False, help="Flag to specify whether the output of transforms should be printed to the console")
@click.option("--print-evaluation-output", is_flag=True, default=False, help="Flag to specify whether the output of model evaluation should be printed to the console")
@click.option("--input-csv", default="quotes.csv.gz", help="Path to input csv")
@click.option("--output-csv", default="clean_quotes.csv.gz", help="Path to output csv")
def run_full(
    print_transform_output: bool,
    print_evaluation_output: bool,
    input_csv: str,
    output_csv: str
):
    run_pipeline(
        input_csv=input_csv,
        output_csv=output_csv,
        print_transform_output=print_transform_output,
        print_evaluation_output=print_evaluation_output
    )

@cli.command(name="ingest")
@click.option("--input-csv", default="quotes.csv.gz", help="Path to input csv")
@click.option("--output-csv", default="clean_quotes.csv.gz", help="Path to output csv")
def run_ingest(input_csv: str, output_csv: str):
    ingest(raw_data=input_csv, output_csv_path=output_csv)

@cli.command(name="validate")
@click.option("--input-csv", default="clean_quotes.csv.gz", help="Path to input csv")
def run_validate(input_csv: str):
    validate(input_csv)

@cli.command(name="transform")
@click.option("--input-csv", default="clean_quotes.csv.gz", help="Path to input csv")
@click.option("--print-transform-output", is_flag=True, default=False, help="Flag to specify whether the output of transforms should be printed to the console")
def run_transform(input_csv: str, print_transform_output: bool):
    transform(input_csv, print_transform_output=print_transform_output)

@cli.command(name="evaluate")
@click.option("--input-csv", default="clean_quotes.csv.gz", help="Path to input csv")
@click.option("--print-evaluation-output", is_flag=True, default=False, help="Flag to specify whether the output of model evaluation should be printed to the console")
def run_evaluate(input_csv: str, print_evaluation_output: bool):
    evaluate(input_csv, print_evaluation_output=print_evaluation_output)

if __name__ == "__main__":
     cli()
