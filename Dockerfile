FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

WORKDIR /template

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and setup files
COPY src/ /template/src/
COPY pipeline/ /template/pipeline/
COPY main.py /template/
COPY setup.py /template/
COPY pyproject.toml /template/

# Install the package itself so it's properly available to Beam workers
RUN pip install -e .

# Verify critical files and directories are present
RUN echo "Verifying container contents..." && \
    ls -la /template/src/wiki_pipeline/ && \
    test -f /template/src/wiki_pipeline/detection_patterns.json && \
    echo "✓ detection_patterns.json found" && \
    test -d /template/src/wiki_pipeline/rule_versions && \
    echo "✓ rule_versions/ directory found" && \
    ls -la /template/src/wiki_pipeline/rule_versions/ && \
    echo "✓ All critical files verified" || \
    (echo "ERROR: Missing critical files" && exit 1)

# Set Python path for imports
ENV PYTHONPATH="/template/src:${PYTHONPATH}"

# Set the main pipeline file for Dataflow Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
