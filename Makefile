PYTHON = python3

all: clean preprocess features
run: preprocess features

clean:
	rm -rf data/01_preprocessed
	rm -rf data/02_feature_engineered

preprocess:
	$(PYTHON) -m core.pipeline.data_preprocessing

features:
	$(PYTHON) -m core.pipeline.feature_engineering

eda:
	$(PYTHON) -m core.pipeline.exploratory_data_analysis
