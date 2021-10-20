#!/usr/bin/env python
import pika
import json
import base64
import os
chunk_size = 200


class Produce:

    def __init__(self, file_name):
        user = 'guest'
        password = 'guest'
        credentials = pika.PlainCredentials(user, password)
        parameters = pika.ConnectionParameters('192.168.202.128',
                                               5672,
                                               '/',
                                               credentials)

        self.channel = None
        self.file_name = file_name
        self.connection = pika.SelectConnection(parameters=parameters, on_open_callback=self.on_open)
        self.run()

    def on_open(self, _conn):
        self.channel = self.connection.channel(on_open_callback=self.declare_exchange)

    def declare_exchange(self, _unused):
        self.channel.exchange_declare(exchange='exchange', exchange_type='topic', passive=True, callback=self.declare_queue)

    def declare_queue(self, _unused):
        self.channel.queue_declare(queue='queue', passive=True, callback=self.publish_messages)

    def publish_messages(self, _unused):
        try:
            f = open(self.file_name, mode='br')
        except Exception as e:
            print(f'*error* {e}')
        num_of_chunks = self.get_num_of_chunks()
        _flag = True
        _id = 0
        while _flag:
            temp = f.read(chunk_size)
            _dict = self.make_temporary_dict(temp, _id, num_of_chunks)
            self.channel.basic_publish(exchange='exchange', routing_key='s.msg', body=json.dumps(_dict))
            _id += 1
            if temp == b'':
                _flag = False
                print('stop sending')
        f.close()

    def get_num_of_chunks(self):
        length = os.path.getsize(self.file_name)
        num_of_chunks = int(length / chunk_size) + 1
        # send to the Receiver the number of the chunks for checking if all the messages were came.
        return num_of_chunks

    def make_temporary_dict(self, temp, _id, num_of_chunks):
        _dict = {}
        encoded = base64.b64encode(temp)
        _dict["id"] = _id
        # Decode the encoded bytes to str.
        _dict["chunk"] = encoded.decode()
        _dict["num_of_chunks"] = num_of_chunks
        return _dict

    def run(self):
        try:
            # Loop so we can communicate with RabbitMQ
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            # Gracefully close the connection
            self.connection.close()
            # Loop until we're fully closed, will stop on its own
            self.connection.ioloop.start()


b = Produce(file_name='picture.jpeg')
