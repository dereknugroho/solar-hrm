PYTHON = python3

all: clean preprocess

clean:
	rm -r data/01_preprocessed/*.parquet

preprocess:
    $(PYTHON) src/data_preprocessing.py
