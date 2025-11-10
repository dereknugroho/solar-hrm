PYTHON = python3

all: clean preprocess feature_engineer
run: preprocess feature_engineer

clean:
	rm -rf data/01_preprocessed
	rm -rf data/02_feature_engineered

preprocess:
	$(PYTHON) -m core.pipeline.data_preprocessing

feature_engineer:
	$(PYTHON) -m core.pipeline.feature_engineering
