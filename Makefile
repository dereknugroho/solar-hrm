PYTHON = python3

all: clean preprocess

clean:
	rm -rf data/01_preprocessed
	rm -rf data/02_feature_engineered
	echo "Non-raw data directories removed successfully"

preprocess:
    $(PYTHON) src/data_preprocessing.py

feature_engineer:
	$(PYTHON) src/feature_engineering.py