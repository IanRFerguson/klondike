import os
from uuid import uuid4

import click
import polars as pl
from faker import Faker
from tqdm import tqdm

#####

FAKE_CLIENT = Faker("en_us")


def generate_fake_data(num_records: int) -> pl.DataFrame:
    """
    Generates a Polars DataFrame of fake PII data for testing purposes.

    Args:
        num_records: The number of fake records to generate.

    Returns:
        A Polars DataFrame containing the generated fake data.
    """

    output = []
    for _ in tqdm(range(num_records), desc="Generating fake data", unit="record"):
        output.append(
            {
                "id": uuid4().hex,
                "name": FAKE_CLIENT.name(),
                "address": FAKE_CLIENT.address(),
                "city": FAKE_CLIENT.city(),
                "state": FAKE_CLIENT.state(),
                "email": FAKE_CLIENT.email(),
                "phone": FAKE_CLIENT.phone_number(),
            }
        )

    return pl.DataFrame(output)


@click.command()
@click.option(
    "--output-dir", default=".", help="Directory to save the generated CSV file."
)
@click.option(
    "--output-filename", default="fake_data.csv", help="Name of the generated CSV file."
)
@click.option(
    "--num-records",
    default=10_000_000,
    help="Number of fake records to generate (default: 10,000,000).",
)
def cli(output_dir: str, output_filename: str, num_records: int) -> None:
    """Generate a CSV file with fake data for testing purposes."""
    df = generate_fake_data(num_records)

    # Combine the output directory and filename to create the full output path
    output_path = os.path.join(output_dir, output_filename)

    # Save the output DataFrame to a CSV file
    df.write_csv(output_path)

    # Notify the end user
    click.echo(f"Generated {num_records} fake records and saved to {output_path}")


#####

if __name__ == "__main__":
    cli()
