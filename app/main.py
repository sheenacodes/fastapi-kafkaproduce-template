from fastapi import FastAPI, Request
import os
import logging
import json
from kafka import KafkaProducer

app = FastAPI()

def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)

# env variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL')
SASL_MECHANISM = os.getenv('SASL_MECHANISM')
SSL_CAFILE = os.getenv('SSL_CAFILE')
SASL_PLAIN_USERNAME = os.getenv('SASL_PLAIN_USERNAME')
SASL_PLAIN_PASSWORD = os.getenv('SASL_PLAIN_PASSWORD')
LOG_LEVEL = os.getenv('LOG_LEVEL')

# initialize logger
logging.basicConfig(level=LOG_LEVEL)
logging.getLogger().setLevel(LOG_LEVEL)

#create kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    ssl_cafile=SSL_CAFILE,
    sasl_plain_username=SASL_PLAIN_USERNAME,
    sasl_plain_password=SASL_PLAIN_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("ascii"),
    key_serializer=lambda v: json.dumps(v).encode("ascii"),
)

@app.route("/", methods=["GET", "POST", "HEAD"])
async def root(request: Request):
    return {"message": "Test ok"}


# when the incoming data is xml and needs to be converted to json
# @app.post(os.getenv('API_REQUEST_PATH'))
# async def kafka_produce(request: Request):
#     post_data = await request.body()
#     logging.info(f"post observation: {post_data}")
#     data_dict = xmltodict.parse(post_data, xml_attribs=False)
#     producer.send(KAFKA_TOPIC,
#                   key={"key": ""},
#                   value={"message": data_dict}
#                   )
#     producer.flush()
#     return {"message": "Kafka Produce Done"}

@app.post(os.getenv('API_REQUEST_PATH'))
async def kafka_produce(request: Request):
    post_data = await request.json()
    logging.info(f"post observation: {post_data}")
    producer.send(KAFKA_TOPIC,
                  key={"key": ""},
                  value={"message": post_data}
                  )
    producer.flush()
    return {"message": "Kafka Produce Done"}