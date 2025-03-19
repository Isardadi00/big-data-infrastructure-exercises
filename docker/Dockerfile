# build it using the following command
# docker build --platform=linux/amd64 -t bdi-api -f docker/Dockerfile .
# And run it using
# docker run -p 8000:8000 bdi-api
# Then visit http://localhost:8000/docs to obtain the API documentation

FROM python:3.11

WORKDIR /usr/src/app
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="$PATH:/root/.local/bin"
ENV BDI_S3_BUCKET='aircraft-bdi-isar'
COPY . .

RUN poetry install

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "--", "bdi_api.app:app", "--host", "0.0.0.0", "--port", "8000"]