FROM python:3.11-slim

# Built by the portable-data-stack-* repos straight from this directory, at the
# tag their DATAMART_REF points to. The build context is this directory, not
# the repository root.

WORKDIR /generator

RUN pip install uv

COPY requirements.txt .

RUN uv pip install --system -r requirements.txt

COPY generate.py .

CMD ["python3", "generate.py"]
