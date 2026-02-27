FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
WORKDIR /code
# Add the virtual environment's bin directory to PATH so that the installed packages can be found and used.
ENV PATH="/code/.venv/bin:$PATH"   
## RUN pip install pandas pyarrow
COPY pyproject.toml .python-version uv.lock ./
# Install Python dependencies defined by uv. Using `uv sync` (without --locked)
# avoids failing the build if the local lockfile is out of date in this workspace.
RUN uv sync

## COPY pipeline.py .
COPY ingest_data_cli.py .


ENTRYPOINT [ "python", "ingest_data_cli.py" ]