
import socket
import sys
from sqlalchemy import create_engine,text
import time
import threading
import json
import struct
import random

# Function to generate random data for YC (Analog) points
def generate_random_yc_data():
    yc_data = []
    for pnt_no in range(1, 11):
        value = random.uniform(0, 100)  # Generate a random float between 0 and 100
        status = random.randint(0, 1)  # Generate a random status (0 or 1)
        refresh_time = int(time.time())  # Current timestamp
        yc_data.append((pnt_no, value, status, refresh_time))
    return yc_data

# Function to generate random data for YX (Digital) points
def generate_random_yx_data():
    yx_data = []
    for pnt_no in range(1, 11):
        value = random.randint(0, 1)  # Generate a random value (0 or 1)
        status = random.randint(0, 1)  # Generate a random status (0 or 1)
        refresh_time = int(time.time())  # Current timestamp
        yx_data.append((pnt_no, value, status, refresh_time))
    return yx_data

# Function to periodically update YC and YX tables with random data
def refresh_ycyx_data(engine):
    while True:
        yc_data = generate_random_yc_data()
        yx_data = generate_random_yx_data()

        with engine.connect() as sqldb:

            # Clear existing data and insert the new random data
            sqldb.execute(text("DELETE FROM rtu_yc_info"))
            sqldb.execute(text("DELETE FROM rtu_yx_info"))
            sqldb.execute(text("VACUUM"))  # Clean up the database file

            # Insert the new random YC data
            sqldb.execute(text("INSERT INTO rtu_yc_info (id, name, value,  status, refresh_time) "
                               "VALUES (?, ?, ?, ?)"), yc_data)

            # Insert the new random YX data
            sqldb.execute(text("INSERT INTO rtu_yx_info (id, name, value, status, refresh_time) "
                               "VALUES (?, ?, ?, ?)"), yx_data)
        sqldb.commit()
        time.sleep(3)  # Sleep for 3 seconds before updating again

def recv_data(conn1, sqldb):
    head = sock.recv(4)
    if head:
        print("recv_data", head)
        head = conn1.recv(struct.unpack("i", head[0]))
        head_data = json.loads(head)
        body =  sqldb.recv(head_data['size'])
        body_data = json.loads(body)
        if head_data['type'] == 'return_yt':
            return recv_yt_data(body_data)
        if head_data['type'] == 'return_yk':
            return recv_yk_data(body_data)

def rtu_thread_recv_data(engine, conn1):
    with engine.connect() as sqldb:
        while True:
            recv_data(conn1, sqldb)

def recv_yt_data(sqldb, data):
    for result in data:
        info = f"update rtu_yt_info set value = {result[2]} where id = {rtu_id} "
        print(info)
        sqldb.execute(text(info))

    result = {"result": "ok"}
    send_data(conn1, {"type": "return_yt"}, result)


def recv_yk_data(sqldb, data):
    for result in data:
        info = f"update rtu_yk_info set value = {result[2]} where id = {rtu_id} "
        print(info)
        sqldb.execute(text(info))

    result = {"result": "ok"}
    send_data(conn1, {"type": "return_yk"}, result)

def send_data(conn1, head, data):
    print("send_data", head, data)
    json_data = json.dumps(data).encode("utf8")
    head['size'] = len(json_data)
    json_head = json.dumps(head).encode("utf8")
    head_len = len(json_head)
    data_head = struct.pack('i', head_len)
    conn1.send(data_head)
    conn1.send(json_head)
    conn1.send(json_data)

def rtu_thread_send_ycyx(engine, conn1):
    while True:
        update_yc_data(engine, conn1)
        update_yx_data(engine, conn1)
        time.sleep(1.0)



def update_yc_data(engine, conn1):
    with engine.connect() as sqldb:
        results = sqldb.execute(text("select * from rtu_yc_info"))
        data = []
        for result in results:
            data.append([result.id, result.value, result.status, result.refresh_time])
        return send_data(conn1, {"type": "update_yc"}, data)

def update_yx_data(engine, conn1):
    with engine.connect() as sqldb:
        results = sqldb.execute(text("select * from rtu_yx_info"))
        data = []
        for result in results:
            data.append([result.id, result.value, result.status, result.refresh_time])
        return send_data(conn1, {"type": "update_yx"}, data)




def process_command(sock, sqldb, rtu_id):
    head = sock.recv(4)
    if head:
        head_len = struct.unpack("i", head)[0]
        head = sock.recv(head_len)
        head_data = json.loads(head)
        body_len, body_type = head_data['size'], head_data['type']
        body = sock.recv(body_len)
        body_data = json.loads(body)

        # Check if the received message is a command from EMS
        if body_type == 'command':

            command_type = body_data.get("command_type") #get the command type
            if command_type == "send_data":
                rtu_thread_send_ycyx(conn1, rtu_id)
                rtu_thread_recv_data(conn1, rtu_id)



def auto_gen_tables(engine, rtu_id):
    with engine.connect() as sqldb:
        sqldb.execute(text("drop table if exists rtu_info"))
        sqldb.execute(text("create table rtu_info(id int, name text, address text, port int, status int, refresh_time int)"))

        # Create yc, yx, yk, and yt tables
        sqldb.execute(text("drop table if exists rtu_yc_info"))
        sqldb.execute(text("drop table if exists rtu_yx_info"))
        sqldb.execute(text("drop table if exists rtu_yk_info"))
        sqldb.execute(text("drop table if exists rtu_yt_info"))
        sqldb.execute(text("create table rtu_yc_info(id int, name text, value int, status int, refresh_time int)"))
        sqldb.execute(text("create table rtu_yx_info(id int, name text, value int, status int, refresh_time int)"))
        sqldb.execute(text("create table rtu_yk_info(id int, name text, value int,ctrl_time int, ret_code int)"))
        sqldb.execute(text("create table rtu_yt_info(id int, name text, value int,ctrl_time int, ret_code int)"))


        sqldb.execute(text(f"insert into rtu_info(id, name, address, port, status, refresh_time) "
                     f"values({rtu_id}, 'rtu_{rtu_id}', '127.0.0.1', {8800 + rtu_id}, 0, 0)"))

        sqldb.commit()

def get_ip_addr(engine, rtu_id):
    with engine.connect() as sqldb:
        results = sqldb.execute(text(f"select * from rtu_info "
                               f"where id = {rtu_id}"))

        for result in results:
            return (result.address, result.port)




if __name__ == "__main__":
    rtu_id = 1
    if len(sys.argv) > 1:
        rtu_id = int(sys.argv[1])
    engine = create_engine(f"sqlite:///./db/rtu_{rtu_id}.db")


    if len(sys.argv)>2 and sys.argv[2] == 'init':
        auto_gen_tables(engine,rtu_id)
        # exit(0)

    rtu_addr = get_ip_addr(engine, rtu_id)
    sock = socket.socket()
    print("bind ", rtu_addr)
    sock.bind(rtu_addr)
    # sock.bind(rtu_addr)
    sock.listen(10)



while True:
    conn1, status = sock.accept()

    thread = threading.Thread(target=rtu_thread_send_ycyx, args=(engine, conn1))
    thread.start()
    thread1 = threading.Thread(target=rtu_thread_recv_data, args=(engine, conn1))
    thread1.start()
    thread2 = threading.Thread(target=refresh_ycyx_data, args=(engine,))
    thread2.start()






