# ------------------------------
# Makefile for Solar HRM Project
# ------------------------------

# Variables
PYTHON = python3
DATA = data/
MODELS = models/

all: prepare train evaluate report

prepare:
    $(PYTHON) src/data_preprocessing.py

train:
    $(PYTHON) src/train_model.py

evaluate:
    $(PYTHON) src/evaluate_model.py

report:
    jupyter nbconvert --to pdf reports/analysis.ipynb

# Clean temporary files
clean:
    rm -f $(MODELS)*.pkl *.log
