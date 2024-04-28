import json
import sys
import socket
import time
import threading
import struct
from sqlalchemy import create_engine,text
import random


def generate_random_ems_yk_data():
    ems_yk_data = []
    for pnt_no in range(1, 11):
        name = f"yk_{pnt_no}"
        value = random.randint(0, 1)  # Generate a random value (0 or 1)
        ret_code = random.randint(0, 1)  # Generate a random ret_code (0 or 1)
        ctrl_time = int(time.time())  # Current timestamp
        ems_yk_data.append((pnt_no, name, value, ret_code, ctrl_time))
    return ems_yk_data

# Function to generate random data for ems_yt_info
def generate_random_ems_yt_data():
    ems_yt_data = []
    for pnt_no in range(1, 11):
        name = f"yt_{pnt_no}"
        value = random.uniform(0, 100)  # Generate a random float between 0 and 100
        ret_code = random.randint(0, 1)  # Generate a random ret_code (0 or 1)
        ctrl_time = int(time.time())  # Current timestamp
        ems_yt_data.append((pnt_no, name, value, ret_code, ctrl_time))
    return ems_yt_data

# Function to periodically update YK and YT tables with random data
def update_ykyt_data(engine):
    while True:
        yk_data = generate_random_ems_yk_data()
        yt_data = generate_random_ems_yt_data()

        with engine.connect() as sqldb:
            # Clear existing data and insert the new random data
            sqldb.execute(text("DELETE FROM ems_yk_info"))
            sqldb.execute(text("DELETE FROM ems_yt_info"))
            sqldb.execute(text("VACUUM"))  # Clean up the database file

            # Insert the new random YC data
            sqldb.execute(text("INSERT INTO ems_yk_info (pnt_no, value, status, ctrl_time, ret_code) "
                               "VALUES (?, ?, ?, ?)"), yk_data)

            # Insert the new random YX data
            sqldb.execute(text("INSERT INTO ems_yt_info (pnt_no, value, status, ctrl_time, ret_code) "
                               "VALUES (?, ?, ?, ?)"), yt_data)
        sqldb.commit()
        time.sleep(3)  # Sleep for 3 seconds before updating again

def rtu_thread_update_ykyt(engine, sock, rtu):
    print("rtu_thread_update_ykyt....")
    with engine.connect() as sqldb:
        while True:
            print("update_yk_data....")
            update_yk_data(sqldb, sock, rtu_id)
            print("update_yt_data....")
            update_yt_data(sqldb, sock, rtu_id)
            time.sleep(1.0)

def send_data(sock, head,data):

    print("send_data", head, data)
    json_data = json.dumps(data).encode("utf8")
    head['size'] = len(json_data)
    json_head = json.dumps(head).encode("utf8")
    head_len = len(json_head)
    data_head = struct.pack('i', head_len)
    sqldb.send(data_head)
    sqldb.send(json_head)
    sqldb.send(json_data)


def update_yt_data(sqldb, sock, rtu_id):
    results = sqldb.execute(text(f"select * from ems_yt_info"
                           f" where rtu_id = {rtu_id}"))
    data = []
    for result in results:
        data.append([result.pnt_no, result.name, result.value, result.ret_code, result.ctrl_time])
    send_data(sock, {"type": "update_yt"}, data)


def update_yk_data(sqldb, sock, rtu_id):
    results = sqldb.execute(text(f"select * from ems_yk_info"
                           f" where rtu_id = {rtu_id}"))
    data = []
    for result in results:
        data.append([result.pnt_no, result.name, result.value, result.ret_code, result.ctrl_time])
    send_data(sock, {"type": "update_yk"}, data)
    time.sleep(3)

def recv_data(sock, sqldb, rtu_id):
    head = sock.recv(4)
    if head:
        head_len = struct.unpack("i", head)[0]
        head = sock.recv(head_len)
        head_data = json.loads(head)
        body_len, body_type = head_data['size'], head_data['type']
        body = sock.recv(body_len)
        body_data = json.loads(body)
        if body_type == 'update_yc':
            return recv_yc_data(sqldb, rtu_id, body_data)
        if body_type == 'update_yx':
            return recv_yx_data(rtu_id, body_data)


def rtu_thread_get_ycyx(engine, rtu):
    try:
        sock = socket.socket()
        print("sqldb", (rtu.address))
        sock.connect((rtu.address, rtu.port))
        while True:
            time.sleep(1.0)

    except Exception as e:
        print("sqldb error !!", (rtu.address, rtu.port),e)

def recv_yc_data(sqldb, rtu_id, data, sock):
    for result in data:
        info = f"update ems_yc_info set value = {result[2]} where rtu_id = {rtu_id} and pnt_no = {result[0]}"
        print(info)
        sqldb.execute(text(info))

    result = {"result": "ok"}
    send_data(sock,{"type": "return_yc" }, result)

def recv_yx_data(sqldb, rtu_id, data, sock):
    for result in data:
        info = f"update ems_yx_info set value = {result[2]} where rtu_id = {rtu_id} and pnt_no = {result[0]}"
        print(info)
        sqldb.execute(text(info))

    result = {"result": "ok"}
    send_data(sock, {"type": "return_yx"}, result)

def send_command(rtu_address, rtu_port, command):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((rtu_address, rtu_port))

            # Send the command to the RTU
            command_data = json.dumps(command).encode("utf8")
            command_head = {"type": "command", "size": len(command_data)}
            command_head_data = json.dumps(command_head).encode("utf8")
            command_head_len = len(command_head_data)

            sock.send(struct.pack('i', command_head_len))
            sock.send(command_head_data)
            sock.send(command_data)

            # Receive and process the RTU's response
            response_head_len = struct.unpack('i', sock.recv(4))[0]
            response_head_data = sock.recv(response_head_len)
            response_head = json.loads(response_head_data)

            response_size = response_head["size"]
            response_data = sock.recv(response_size)
            response = json.loads(response_data)

            rtu_thread_update_ykyt(sqldb, sock, rtu_id)
            return response  # This is the response from the RTU

    except Exception as e:
        print("Error sending command to RTU:", e)
        return None


def auto_gen_tables(engine):
    with engine.connect() as sqldb:
        sqldb.execute(text("drop table if exists ems_rtu_info"))
        sqldb.execute(text("create table ems_rtu_info(id int, name text, address varchar2(24), port int, status int, refresh_time int)"))

        # Create yc, yx, yk, and yt tables
        sqldb.execute(text("drop table if exists ems_yc_info"))
        sqldb.execute(text("drop table if exists ems_yx_info"))
        sqldb.execute(text("drop table if exists ems_yk_info"))
        sqldb.execute(text("drop table if exists ems_yt_info"))
        sqldb.execute(text("create table ems_yc_info(id int, pnt_no int, name text, value int, status int, "
                           "refresh_time int)"))
        sqldb.execute(text("create table ems_yx_info(id int, pnt_no int, name text, value int, status int, "
                           "refresh_time int)"))
        sqldb.execute(text("create table ems_yk_info(id int, pnt_no int, name text, value int,ctrl_time int, "
                           "ret_code int)"))
        sqldb.execute(text("create table ems_yt_info(id int, pnt_no int, name text, value int,ctrl_time int, "
                           "ret_code int)"))

        for rtu_id in range(1, 6):
            sqldb.execute(text(f"insert into ems_rtu_info(id, name, address, port, status, refresh_time) "
                              f"values({rtu_id}, 'rtu_{rtu_id}', '127.0.0.1', {8800 + rtu_id}, 0, 0)"))

        results = sqldb.execute(text("select * from ems_rtu_info"))

        for result in results:
            print(result.id, result.name, result.address, result.port, result.status, result.refresh_time)

        sqldb.commit()


if __name__ == "__main__":
    engine = create_engine(f"sqlite:///./db/ems.db")

    print(sys.argv)
    if len(sys.argv)> 1 and sys.argv[1] == 'init':
        auto_gen_tables(engine)

    with engine.connect() as sqldb:
        rtu_id = sqldb.execute(text("select id from ems_rtu_info"))
        for rtu in rtu_id:
            server_address = ('127.0.0.1', int(8800+ rtu[0]))
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(server_address)
            break
        rtus = sqldb.execute(text("select * from ems_rtu_info"))
        thread_list = []

        for rtu in rtus:
            print(rtu)
            thread = threading.Thread(target=rtu_thread_get_ycyx, args=(engine, rtu))
            thread.start()
            thread1 = threading.Thread(target=rtu_thread_update_ykyt, args=(engine,sock,rtu))
            thread1.start()

