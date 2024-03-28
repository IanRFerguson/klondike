import setuptools
import os
import pathlib

##########


def main():
    HERE = pathlib.Path(__file__).parent
    README = (HERE / "README.md").read_text()

    with open(os.path.join(HERE, "requirements.txt")) as requires_:
        INSTALL_REQUIRES = requires_.read().strip().split("\n")

    setuptools.setup(
        name="klondike",
        version="0.1.0",
        author="Ian Richard Ferguson",
        author_email="IRF229@nyu.edu",
        url="https://github.com/IanRFerguson/klondike",
        keywords=["API", "ETL", "BIGQUERY"],
        packages=setuptools.find_packages(),
        install_requires=INSTALL_REQUIRES,
        classifiers=[
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
        ],
        python_requires=">=3.8.0,<3.11.0",
        long_description=README,
        long_description_content_type="text/markdown",
    )


#####

if __name__ == "__main__":
    main()
