from fastapi import FastAPI, Request
from aiokafka import AIOKafkaProducer
import json
import os
from aiokafka.helpers import create_ssl_context
import asyncio


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

app = FastAPI()
loop = asyncio.get_event_loop()

context = create_ssl_context(
    cafile=SSL_CAFILE
)
producer = AIOKafkaProducer(loop=loop,   bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                            security_protocol=SECURITY_PROTOCOL,
                            sasl_mechanism=SASL_MECHANISM,
                            # ssl_cafile=SSL_CAFILE,
                            ssl_context=context,
                            sasl_plain_username=SASL_PLAIN_USERNAME,
                            sasl_plain_password=SASL_PLAIN_PASSWORD,
                            value_serializer=lambda v: json.dumps(
                                v).encode("ascii"),
                            key_serializer=lambda v: json.dumps(v).encode("ascii"),)


@app.post('/')
async def produce_message(request: Request):
    post_data = await request.json()
    print(post_data)
    await producer.send(KAFKA_TOPIC,
                        key={"key": ""},
                        value={"message": post_data})

    return {'status': 'success', 'message': post_data}


@app.on_event("startup")
async def startup():
    await producer.start()


@app.on_event("shutdown")
async def shutdown():
    await producer.stop()
