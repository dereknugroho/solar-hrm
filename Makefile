PYTHON = python3
ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

pipeline: clean prep features eda
run: prep features eda

clean:
	rm -rf $(ROOT)data/01_preprocessed
	rm -rf $(ROOT)data/02_feature_engineered
	rm -rf $(ROOT)figures/

prep:
	$(PYTHON) src/core/pipeline/data_preprocessing.py

features:
	$(PYTHON) src/core/pipeline/feature_engineering.py

eda:
	$(PYTHON) src/core/pipeline/exploratory_data_analysis.py
