#!/usr/bin/env python
import pika
import json
import base64


class Consume:

    def __init__(self, new_file_name):
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('192.168.202.128',
                                               5672,
                                               '/',
                                               credentials)
        self.channel = None
        self.new_file_name = new_file_name
        self._list = []
        self.connection = pika.SelectConnection(parameters, on_open_callback=self.on_open)
        self.run()

    def on_open(self, _conn):
        self.channel = self.connection.channel(on_open_callback=self.declare_exchange)

    def declare_exchange(self, _unused):
        self.channel.exchange_declare(exchange='exchange', exchange_type='topic', callback=self.declare_queue)

    def declare_queue(self, _unused):
        self.channel.queue_declare(queue='queue', callback=self.bind_queue)

    def bind_queue(self, _unused):
        self.channel.queue_bind(exchange='exchange', queue='queue', routing_key='s.*', callback=self.consume_messages)

    def consume_messages(self, _unused):
        self.channel.basic_consume(queue='queue', on_message_callback=self.callback, auto_ack=True)

    def callback(self, ch, method, properties, body):
        data = json.loads(body)
        _id, data_in_bytes, num_of_chunks = self.get_data_from_dict(data)
        self._list.insert(_id, data_in_bytes)
        if data_in_bytes == b'':
            self.write_in_order(num_of_chunks)

    def get_data_from_dict(self, _dict):
        _id = _dict["id"]
        data_in_str = _dict["chunk"]
        num_of_chunks = _dict["num_of_chunks"]
        data_in_bytes = base64.b64decode(data_in_str)
        return _id, data_in_bytes, num_of_chunks

    def write_in_order(self, num_of_chunks):
        file = open(self.new_file_name, mode='wb')
        for i in range(num_of_chunks):
            try:
                file.write(self._list[i])
            except Exception as e:
                print(f'*error* not all the data from the send arrived. {e}')
                file.seek(0)
                file.truncate(0)
                break
        file.close()
        print(f' a new file: "{self.new_file_name}" was arrived from the consumer.')

    def run(self):
        try:
            print(' [*] Waiting for messages. To exit press CTRL+C')
            self.connection.ioloop.start()
            # self.channel.start_consuming()
        except Exception as e:
            print(f"exception in publisher {e}")
            # self.connection.close()
            # self.connection.ioloop.start()


b = Consume(new_file_name='newPicture.jpeg')
