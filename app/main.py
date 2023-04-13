from fastapi import FastAPI, Request
import os
import logging
import json, xmltodict
from kafka import KafkaProducer


app = FastAPI()


def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)


# # env variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL')
SASL_MECHANISM = os.getenv('SASL_MECHANISM')
SSL_CAFILE = os.getenv('SSL_CAFILE')
SASL_PLAIN_USERNAME = os.getenv('SASL_PLAIN_USERNAME')
SASL_PLAIN_PASSWORD = os.getenv('SASL_PLAIN_PASSWORD')

# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)

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


@app.post("/peoplecounter/v1/")
async def kafka_produce(request: Request):
    post_data = await request.body()
    logging.debug(f"post observation: {post_data}")
    data_dict = xmltodict.parse(post_data, xml_attribs=False)
    print(data_dict)
    producer.send(KAFKA_TOPIC,
                  key={"key": ""},
                  value={"message": data_dict}
                  )
    producer.flush()
    return {"message": "Kafka Produce Done"}
