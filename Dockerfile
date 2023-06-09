#
FROM python:3.11-slim-buster

#
WORKDIR /code

#
COPY ./requirements.txt /code/requirements.txt

#
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

#
COPY ./app /code/app
COPY ./chain.pem /code/chain.pem

#
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--proxy-headers", "--port", "80"]