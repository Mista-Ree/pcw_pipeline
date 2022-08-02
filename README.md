Install the dependencies specified in `requirements.txt`:

```python
pip install -r requirements.txt
```

To run the pipeline from the cli. This will show the help text with all the available commands.

```python
python -m pipeline.cli
```

Example command to run the full pipeline and to print out the output from transforms and model evaluation.

```python
python -m pipeline.cli run-full --print-transform-output --print-evaluation-output
```

n.b. The ingestion stage is expected to be run at least once prior to other stages as the latter stages depend on the output csv from the ingestion stage.
