PYTHON = python3

all: clean preprocess feature_engineering
run: preprocess feature_engineering

clean:
	rm -rf data/01_preprocessed
	rm -rf data/02_feature_engineered

preprocess:
	$(PYTHON) -m core.pipeline.data_preprocessing

feature_engineering:
	$(PYTHON) -m core.pipeline.feature_engineering
