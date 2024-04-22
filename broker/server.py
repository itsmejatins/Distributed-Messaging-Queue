from flask import Flask, request, jsonify
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe import election
from threading import Thread
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.recipe.lock import Lock
import threading
import sys
import uuid
import sqlite3
import json
import datetime
import colorama

# Initialize colorama
colorama.init()
app = Flask(__name__)
# node_id = str(uuid.uuid4())
HOST = '127.0.0.1'
# PORT = 5000
PORT = sys.argv[1]  # Getting the port of broker from the command line argument
node_id = HOST + str(PORT)
zkhost = 'localhost:2181'
zk = KazooClient(hosts=zkhost)  # zookeeper cliet object, connected to zkhost provided inside the constructor
zk.start()
delimiter = '##'
last_log_index = 0
is_leader = False

# ensure that below paths exists in the zookeeper (create them if they don't)

zk.ensure_path('/election')
zk.ensure_path('/leader')
zk.ensure_path('/message_queue')
zk.ensure_path('/logs')
zk.ensure_path('/consumers')
zk.ensure_path('/locks')
zk.ensure_path('/locks/topics')
election_node = zk.create('/election/node-' + node_id, ephemeral=True)

## This is registering the current broker ( port and address ) to the zookeeper , to be part of the election process
zk.set('/election/node-' + node_id, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").encode())

lock = threading.Lock()
publishLock = threading.Lock()
consumeLock = threading.Lock()


# con = sqlite3.connect(str(PORT) + ".sqlite")

def printlog(message):
    now = datetime.datetime.now()
    print(now, message)


def printCommand(message):
    now = datetime.datetime.now()
    print(now, message)


def printlog(str, mode="WHITE"):
    now = datetime.datetime.now()
    if mode == "GREEN":
        print(colorama.Fore.GREEN, end="")
    elif mode == "RED":
        print(colorama.Fore.RED, end="")
    elif mode == "YELLOW":
        print(colorama.Fore.YELLOW, end="")
    elif mode == "BLUE":
        print(colorama.Fore.BLUE, end="")
    print(now, str)
    print(colorama.Style.RESET_ALL, end="")


def printcommand(str, mode="GREEN"):
    now = datetime.datetime.now()
    if mode == "GREEN":
        print(colorama.Fore.GREEN, end="")
    elif mode == "RED":
        print(colorama.Fore.RED, end="")
    elif mode == "YELLOW":
        print(colorama.Fore.YELLOW, end="")

    print(now, "[EXECUTED]", str)
    print(colorama.Style.RESET_ALL, end="")




def get_db_connection():
    """
    Establishes a connection to the database with the specified PORT.
    """
    port = PORT  # Assuming PORT is defined elsewhere
    db_path = f"./db/{port}.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# Define a function to close the database connection
def close_db_connection(conn):
    conn.close()



def db_init():
    with get_db_connection() as con:
        cur = con.cursor()

        create_table_queries = [
            "CREATE TABLE IF NOT EXISTS consumer (id varchar(100) NOT NULL, PRIMARY KEY (id));",
            "CREATE TABLE IF NOT EXISTS producer (id varchar(100) NOT NULL, PRIMARY KEY (id));",
            "CREATE TABLE IF NOT EXISTS topic (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(100) NOT NULL UNIQUE, offset INTEGER NOT NULL, size INTEGER NOT NULL);",
            "CREATE TABLE IF NOT EXISTS message_queue (id INTEGER NOT NULL, seq_no INTEGER NOT NULL, message varchar(500), PRIMARY KEY (id, seq_no), FOREIGN KEY(id) REFERENCES topic(id));",
            "CREATE TABLE IF NOT EXISTS config_table (id varchar(100) NOT NULL, last_log_index INTEGER DEFAULT 0, PRIMARY KEY (id));"
        ]

        for query in create_table_queries:
            cur.execute(query)

        cur.execute("INSERT OR IGNORE into config_table(id, last_log_index) values('" + str(node_id) + "'," + str(
            0) + ");")

        con.commit()



def db_clear():
    with get_db_connection() as con:
        cur = con.cursor()

        drop_table_queries = [
            "DROP TABLE IF EXISTS consumer;",
            "DROP TABLE IF EXISTS producer;",
            "DROP TABLE IF EXISTS topic;",
            "DROP TABLE IF EXISTS message_queue;",
            "DROP TABLE IF EXISTS config_table;"
        ]

        for query in drop_table_queries:
            cur.execute(query)

        con.commit()



def set_leader_location():
    leader_loc = {'host': HOST, 'port': PORT}
    leader_loc_str = json.dumps(leader_loc)
    zk.set('/leader', leader_loc_str.encode())


def become_leader():
    set_leader_location()
    print("<Leader>")


# If current broker which we are in is the one with lowest sequence number , then it will be elected as the LEADER
def start_election():
    global is_leader
    # Create an ephemeral node with a unique sequential name
    # Get the list of all ephemeral nodes under /election
    election_nodes = zk.get_children('/election')
    node_path = '/election'
    election_nodes = sorted(election_nodes, key=lambda c: zk.get(f"{node_path}/{c}")[0].decode())
    # Sort the nodes in ascending order
    # If this node has the lowest sequence number, it becomes the leader
    if election_node == f'/election/{election_nodes[0]}':
        become_leader()
        is_leader = True
    else:
        is_leader = False


def election_watcher(event):
    lock.acquire()
    # If a node is deleted, start a new election
    start_election()
    lock.release()


@app.route('/')
def health():
    return 'Healthy'


@app.route('/db/clear', methods=['GET'])
def clear():
    db_clear()
    db_init()
    return 'Cleared DB'




def topic_lock(topic, consumer_id):
    try:
        lock_path, _ = zk.get('locks/topics/{}'.format(topic))
        lock_state = int(lock_path.decode())
    except:
        lock_state = 0  # Assume the lock is not held if it's not found

    if not lock_state:
        zk.set('locks/topics/{}'.format(topic, consumer_id), b'1')





def topic_unlock_all_consumers(topic):
    lock_path = 'locks/topics/{}'.format(topic)

    try:
        lock_state, _ = zk.get(lock_path)
        lock_state = int(lock_state.decode())
    except:
        # If the lock state is not found, assume it's unlocked
        lock_state = 0

    if lock_state == 1:
        zk.set(lock_path, b'0')


def SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id):
    return f"SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';"


def SELECT_ALL_FROM_TOPIC_QUERY(topicName):
    return f"SELECT * FROM topic WHERE name = '" + str(topicName) + "';"


def UPDATE_TOPIC_SIZE_QUERY(newSize, topicName):
    return f"UPDATE topic SET size = " + str(newSize) + " WHERE name = '" + str(topicName) + "';"


def INSERT_MESSAGE_QUEUE_QUERY(id, seqNo, message):
    return f"INSERT INTO message_queue VALUES(" + str(id) + ", " + str(seqNo) + ", '" + str(message) + "');"


def UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(lastLogIndex, nodeId):
    return f"UPDATE config_table SET last_log_index = " + str(lastLogIndex + 1) + " WHERE id = '" + str(nodeId) + "';"


def SELECT_ALL_FROM_MESSAGE_QUEUE(id, seqNo):
    return f"SELECT * FROM message_queue WHERE id = " + str(id) + " AND seq_no = " + str(seqNo + 1) + ";"


def UPDATE_OFFSET_IN_TOPIC_NAME(offset, topicName):
    return f"UPDATE topic SET offset = " + str(offset + 1) + " WHERE name = '" + str(topicName) + "';"


def INSERT_INTO_CONSUMER_QUERY(id):
    return f"INSERT INTO consumer VALUES('" + str(id) + "');"


def INSERT_INTO_PRODUCER_QUERY(id):
    return f"INSERT INTO producer VALUES('" + str(id) + "');"


def DELETE_PRODUCER_QUERY(producerId):
    return f"DELETE FROM producer WHERE id = '" + str(producerId) + "';"


def BEGIN_TRANSACTION():
    return "BEGIN"


def CREATE_TOPIC_QUERY(topicName):
    return f"INSERT INTO topic (name, offset, size) VALUES('" + str(topicName) + "', 0, 0)" + ";"


def DELETE_TOPIC_QUERY(topicName):
    return f"DELETE FROM topic WHERE name = '" + str(topicName) + "';"


## Producer will call it to publish the message
## Format will be name of topic and message to publish
@app.route('/publish', methods=['POST'])
def publish_message():
    consumeLock.acquire()
    name = request.json.get('name')
    message = request.json.get('message')

    con = get_db_connection()

    try:
        with con:
            cur = con.cursor()


            selectLastLogIndexQuery = SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id)
            cur.execute(selectLastLogIndexQuery)

            last_log_index = cur.fetchone()[0]
            printlog("publish message : log index > " + str(last_log_index))

            ########## BEGIN TRANSACTION ##########
            print(" publish message : begin transaction ")
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(beginTransactionQuery)

            # Reading id, size from topic table

            topicCommandQuery = SELECT_ALL_FROM_TOPIC_QUERY(name)
            cur.execute(topicCommandQuery)

            topicRecords = cur.fetchall()

            id = topicRecords[0][0]
            size = topicRecords[0][3]
            new_size = size + 1
            printlog("publish message : topic size " + str(size))

            # Update the size of the existing queue in topic table
            updateTopicSizeQuery = UPDATE_TOPIC_SIZE_QUERY(new_size, name)
            seq_no = size + 1

            pushMessageQuery = INSERT_MESSAGE_QUEUE_QUERY(id, seq_no, message)

            # Add an entry into logs in the Zookeeper
            log_entry = [updateTopicSizeQuery, pushMessageQuery]
            log_entry_str = delimiter.join(log_entry)
            zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
            printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }")

            # Commit entry into the database
            cur.execute(updateTopicSizeQuery)
            printcommand("executing : ", updateTopicSizeQuery)

            # Write in the message_queue table
            cur.execute(pushMessageQuery)
            printcommand("executing : ", pushMessageQuery)

            # update the last_log_index

            cur.execute(UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id))

            # Commit the transaction
            # con.execute("COMMIT")
            con.commit()
            print("publish message : commit ")
            topic_unlock_all_consumers(name)

            printlog("publish message : LOG INDEX  > " + str(last_log_index + 1))

        return jsonify({'message': 'message published in topic successfully'})
    except:
        con.rollback()
        print("publish message : rollback ")
        return jsonify({'message': 'Some error occured'}), 501
    finally:
        close_db_connection(con)
        consumeLock.release()


@app.route('/read', methods=['POST'])
def read_message():
    consumeLock.acquire()
    name = request.json.get('name')
    consumer_id = request.json.get('id')
    con = get_db_connection()

    try:
        with con:
            cur = con.cursor()
            # Start a transaction
            # con.execute("BEGIN")
            print(" read message : begin transaction ")
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(beginTransactionQuery)

            # Reading id, size from topic table
            # command = "SELECT * FROM topic WHERE name = '" + str(name) + "';"
            # command = SELECT_ALL_FROM_TOPIC_QUERY(name)
            selectAllFromTopicQuery = SELECT_ALL_FROM_TOPIC_QUERY(name)
            cur.execute(selectAllFromTopicQuery)

            records = cur.fetchall()

            id = records[0][0]
            offset = records[0][2]
            size = records[0][3]

            if (offset < size):
                # fetch_command = "SELECT * FROM message_queue WHERE id = " + str(id) + " AND seq_no = " + str(offset + 1) + ";"
                # fetch_command = SELECT_ALL_FROM_MESSAGE_QUEUE(id,offset)
                selectAllMessageFromQueueQuery = SELECT_ALL_FROM_MESSAGE_QUEUE(id, offset)

                cur.execute(selectAllMessageFromQueueQuery)
                print(" read message : ", selectAllFromTopicQuery)
                records = cur.fetchall()
                message = records[0][2]
                return jsonify({'message': message, 'offset': offset + 1})
            else:
                topic_lock(name, consumer_id)
                return jsonify({'message': ''}), 204
    except:
        # Rollback the transaction if there was an error
        con.rollback()
        printlog("read message : rollback")
        return jsonify({'message': 'Error. Unable to consume message'}), 501
    finally:
        # print("closing db connection")
        close_db_connection(con)
        consumeLock.release()


@app.route('/consume', methods=['POST'])
def consume_message():
    consumeLock.acquire()
    name = request.json.get('name')
    seq_no = request.json.get('offset')
    con = get_db_connection()

    try:
        with con:
            cur = con.cursor()
            # printlog("CONNECTING DB")

            # cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")
            cur.execute(SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id))

            last_log_index = cur.fetchone()[0]
            printlog("[LOG INDEX] : " + str(last_log_index))
            # Start a transaction
            # con.execute("BEGIN")
            print(" consume message : begin transaction ")
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(beginTransactionQuery)


            selectAllFromTopicQuery = SELECT_ALL_FROM_TOPIC_QUERY(name)
            # print(command)
            cur.execute(selectAllFromTopicQuery)
            print("executing : ", selectAllFromTopicQuery)
            records = cur.fetchall()

            id = records[0][0]
            offset = records[0][2]
            size = records[0][3]

            printlog("consume message : topic offset > " + str(offset))

            if not (seq_no <= size and seq_no == offset + 1):
                printlog("XXXXXXXXXXXX offset exceeds queue size XXXXXXXXXXXX")
                return jsonify({'message': 'Error. Offset exceeds queue size.'}), 204
            else:


                # update_command = UPDATE_OFFSET_IN_TOPIC_NAME(offset,name)
                updateOffsetQuery = UPDATE_OFFSET_IN_TOPIC_NAME(offset, name)

                # Add an entry into logs in the Zookeeper
                log_entry = [updateOffsetQuery]
                log_entry_str = delimiter.join(log_entry)
                zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
                printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }")

                # Update topic table, increase offset

                cur.execute(updateOffsetQuery)


                updateConfigTableQuery = UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id)
                cur.execute(updateConfigTableQuery)
                print("executing : ", updateConfigTableQuery)

                con.commit()
                printlog("[LOG INDEX] : " + str(last_log_index + 1))
                return jsonify({'message': 'Message Consumed. Log Index Updated'})


    except:
        con.rollback()
        printlog(" XXXXXXXXXXXX [ROLLBACKED] XXXXXXXXXXXX ")
        return jsonify({'message': 'Error. Unable to consume message'}), 501
    finally:
        close_db_connection(con)
        consumeLock.release()


@app.route("/consumer/create", methods=['POST'])
def create_consumer():
    # Get the message from the request body d
    id = request.json.get('id')

    # command = "INSERT INTO consumer VALUES('" + str(id) + "');"
    command = INSERT_INTO_CONSUMER_QUERY(id)

    # Add an entry into logs in the Zookeeper

    log_entry = [command]
    log_entry_str = delimiter.join(log_entry)
    zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
    printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }")

    # Commit entry into the database
    con = get_db_connection()
    # printlog("CONNECTING DB")

    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            cur = con.cursor()
            # cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")

            selectLastLogIndexQuery = SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id)
            cur.execute(selectLastLogIndexQuery)

            last_log_index = cur.fetchone()[0]
            printlog("[LOG INDEX] : " + str(last_log_index))

            # command = "INSERT INTO consumer VALUES('" + str(id) + "');"
            command = INSERT_INTO_CONSUMER_QUERY(id)

            cur.execute(command)
            printcommand(command)

            # update the last_log_index

            # cur.execute("UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(node_id) + "';")
            updateConfigTableQuery = UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id)
            cur.execute(updateConfigTableQuery)

            # Commit the transaction

            con.commit()
            printlog("OOOOOOOOOOOO [COMMITTED] OOOOOOOOOOOO ")
            printlog("[LOG INDEX] : " + str(last_log_index + 1))
        return jsonify({'message': 'Consumer created.'})
    except:
        # Rollback the transaction if there was an error
        # con.execute("ROLLBACK")
        con.rollback()
        printlog(" XXXXXXXXXXXX [ROLLBACKED] XXXXXXXXXXXX ")
        return jsonify({'message': 'Error. Unable to create consumer.'}), 501
    finally:
        # printlog("CLOSING DB")
        close_db_connection(con)


# TESTED
@app.route("/consumer/delete", methods=['POST'])
def delete_consumer():
    # Get the message from the request body
    id = request.json.get('id')

    exists_command = "SELECT * FROM consumer WHERE id = '" + str(id) + "';"

    # DELETE FROM artists_backup WHERE artistid = 1;

    command = "DELETE FROM consumer WHERE id = '" + str(id) + "';"
    con = get_db_connection()
    cur = con.cursor()
    cur.execute(exists_command)
    rows = cur.fetchall()
    if (len(rows) >= 1):
        # Add an entry into logs in the Zookeeper

        log_entry = [command]
        log_entry_str = delimiter.join(log_entry)
        zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
        printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }", "YELLOW")

        # Commit entry into the database

        # con = get_db_connection()
        # printlog("CONNECTING DB")

        # DELETE FROM artists_backup WHERE artistid = 1;
        try:
            with con:
                # Start a transaction
                con.execute("BEGIN")
                command = "DELETE FROM consumer WHERE id = '" + str(id) + "';"
                cur = con.cursor()
                cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")
                last_log_index = cur.fetchone()[0]
                printlog("[LOG INDEX] : " + str(last_log_index), "YELLOW")
                cur.execute(command)
                printcommand(command)
                cur.execute(
                    "UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(
                        node_id) + "';")
                # Commit the transaction
                # con.execute("COMMIT")
                con.commit()
                printlog("[COMMITTED]", "GREEN")
                printlog("[LOG INDEX] : " + str(last_log_index + 1), "YELLOW")
            return jsonify({'message': 'consumer deleted successfully'})
        except:
            # Rollback the transaction if there was an error
            # con.execute("ROLLBACK")
            con.rollback()
            printlog("[ROLLBACKED]")
            return jsonify({'message': 'consumer deleted unsuccessful!!'}), 501
        finally:
            # printlog("CLOSING DB")
            close_db_connection(con)
    else:
        printlog("[Consumer NOT EXISTS]")
        return jsonify({'message': 'consumer not exists!!'}), 501


# TESTED
@app.route("/producer/create", methods=['POST'])
def create_producer():
    # Get the message from the request body
    id = request.json.get('id')
    print(f"Received id {id}")

    # command = "INSERT INTO producer VALUES('" + str(id) + "');"

    insertIntoProducerQuery = INSERT_INTO_PRODUCER_QUERY(id)

    # Add an entry into logs in the Zookeeper

    log_entry = [insertIntoProducerQuery]
    log_entry_str = delimiter.join(log_entry)

    zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
    printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }", "YELLOW")

    # Commit entry into the database

    con = get_db_connection()
    # printlog("CONNECTING DB")
    try:
        with con:
            # Start a transaction
            # con.execute("BEGIN")
            ########## BEGIN TRANSACTION ##########
            print(" create producer :  begin transaction ")
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(beginTransactionQuery)

            # command = "INSERT INTO producer VALUES('" + str(id) + "');"
            createProducerQuery = INSERT_INTO_PRODUCER_QUERY(id)

            cur = con.cursor()
            # cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")

            selectLastLogIndexQuery = SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id)
            cur.execute(selectLastLogIndexQuery)

            last_log_index = cur.fetchone()[0]
            printlog("[LOG INDEX] : " + str(last_log_index))
            cur.execute(createProducerQuery)
            printcommand(createProducerQuery)

            # cur.execute("UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(node_id) + "';")

            updateConfigTableQuery = UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id)
            cur.execute(updateConfigTableQuery)

            # Commit the transaction
            # con.execute("COMMIT")

            con.commit()
            printlog("create producer : commit")
            printlog("[LOG INDEX] : " + str(last_log_index + 1))

        return jsonify({'message': 'Producer created.'})
    except:
        # Rollback the transaction if there was an error
        con.rollback()
        # con.execute("ROLLBACK")
        printlog(" create producer : rollback ")
        return jsonify({'message': 'Error. Unable to create Producer.'}), 501
    finally:
        # printlog("CLOSING DB")
        close_db_connection(con)


# TESTED
@app.route("/producer/delete", methods=['POST'])
def delete_producer():
    # Get the message from the request body
    id = request.json.get('id')

    # command = "DELETE FROM producer WHERE id = '" + str(id) + "';"
    deleteProducerCommand = DELETE_PRODUCER_QUERY(id)

    # Add an entry into logs in the Zookeeper
    log_entry = [deleteProducerCommand]

    log_entry_str = delimiter.join(log_entry)
    zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
    printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }", "YELLOW")

    # Commit entry into the database
    con = get_db_connection()
    # printlog("CONNECTING DB")
    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            # command = "DELETE FROM producer WHERE id = '" + str(id) + "';"
            deleteProducerQuery = DELETE_PRODUCER_QUERY(id)

            cur = con.cursor()
            cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")
            last_log_index = cur.fetchone()[0]
            printlog("[LOG INDEX] : " + str(last_log_index))

            cur.execute(deleteProducerQuery)
            printcommand(deleteProducerQuery)
            # cur.execute("UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(node_id) + "';")

            updateLastLogIndexQuery = UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id)
            cur.execute(updateLastLogIndexQuery)

            # Commit the transaction
            # con.execute("COMMIT")
            con.commit()
            printlog("[COMMITTED]")
            printlog("[LOG INDEX] : " + str(last_log_index + 1), "YELLOW")
        return jsonify({'message': 'Producer Created'})
    except:
        # Rollback the transaction if there was an error
        # con.execute("ROLLBACK")
        con.rollback()
        printlog("XXXXXXXXXXXX [ROLLBACKED] XXXXXXXXXXXX ")
        return jsonify({'message': 'Error. Unable to delete producer'}), 501
    finally:
        # printlog("CLOSING DB")
        close_db_connection(con)


# TESTED
@app.route("/topic/exists", methods=['POST'])
def exists_topic():
    # Get the message from the request body
    name = request.json.get('name')

    # command = "SELECT * FROM topic WHERE name = '" + str(name) + "';"
    selectAllFromTopicQuery = SELECT_ALL_FROM_TOPIC_QUERY(name)

    # Add an entry into logs in the Zookeeper



    # Commit entry into the database

    con = get_db_connection()
    # printlog("CONNECTING DB")
    try:
        with con:
            # Start a transaction
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(beginTransactionQuery)

            cur = con.cursor()


            # command = "SELECT * FROM topic WHERE name = '" + str(name) + "';"
            selectAllFromTopicQuery = SELECT_ALL_FROM_TOPIC_QUERY(name)

            cur.execute(selectAllFromTopicQuery)
            printcommand(selectAllFromTopicQuery)

            rows = cur.fetchall()
            print("len: " + str(len(rows)))

            # Commit the transaction
            # con.execute("COMMIT")
            con.commit()
        if (len(rows) > 0):
            printlog("OOOOOOOOOOOO topic found OOOOOOOOOOOO")
            return jsonify({'message': True})
        else:
            printlog("XXXXXXXXXXXX topic not found XXXXXXXXXXXX")
            return jsonify({'message': False})
    except:
        # Rollback the transaction if there was an error
        # con.execute("ROLLBACK")
        con.rollback()
        return jsonify({'message': 'Error. Unable to check for topic '}), 501
    finally:
        # printlog("CLOSING DB")
        close_db_connection(con)


@app.route("/topic/create", methods=['POST'])
def create_topic():
    # Get the message from the request body
    name = request.json.get('name')

    # command = "INSERT INTO topic (name, offset, size) VALUES('" + str(name) + "', 0, 0)" + ";"
    createTopicQuery = CREATE_TOPIC_QUERY(name)

    print(createTopicQuery)

    # Add an entry into logs in the Zookeeper

    log_entry = [createTopicQuery]
    log_entry_str = delimiter.join(log_entry)
    zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
    printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }", "YELLOW")
    # Commit entry into the database

    con = get_db_connection()
    # printlog("CONNECTING DB")
    try:
        with con:
            # Start a transaction

            # con.execute("BEGIN")
            print(" create topic : begin transaction ")
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(beginTransactionQuery)

            # command = "INSERT INTO topic (name, offset, size) VALUES('" + str(name) + "', 0, 0)" + ";"
            createTopicQuery = CREATE_TOPIC_QUERY(name)

            # printcommand(createTopicQuery)
            cur = con.cursor()

            # cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")
            selectLastLogIndexQuery = SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id)
            cur.execute(selectLastLogIndexQuery)
            print("executing : ", selectLastLogIndexQuery)

            last_log_index = cur.fetchone()[0]
            printlog("[LOG INDEX] : " + str(last_log_index), "YELLOW")
            print("executing : ", createTopicQuery)
            cur.execute(createTopicQuery)

            # cur.execute("UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(node_id) + "';")

            updateLastLogIndexQuery = UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id)
            cur.execute(updateLastLogIndexQuery)
            print("executing : ", updateLastLogIndexQuery)

            # Commit the transaction
            # con.execute("COMMIT")
            con.commit()
            print("create topic : commit transaction")
            printlog(" OOOOOOOOOOOO commit OOOOOOOOOOOO ")
            printlog("[LOG INDEX] : " + str(last_log_index + 1), "YELLOW")
            zk.create('locks/topics/{}'.format(name), b'0')
        return jsonify({'message': 'Topic created.'})
    except:
        # Rollback the transaction if there was an error
        # con.execute("ROLLBACK")
        con.rollback()
        printlog(" XXXXXXXXXXXX rollback XXXXXXXXXXXX ")
        return jsonify({'message': 'Error. Unable to create topic.'}), 501
    finally:
        # printlog("CLOSING DB")
        close_db_connection(con)


# TESTED
@app.route("/topic/delete", methods=['POST'])
def delete_topic():
    # Get the message from the request body
    name = request.json.get('name')

    # DELETE FROM artists_backup WHERE artistid = 1;

    # command = "DELETE FROM topic WHERE name = '" + str(name) + "';"
    deleteTopicQuery = DELETE_TOPIC_QUERY(name)

    # Add an entry into logs in the Zookeeper

    log_entry = [deleteTopicQuery]
    log_entry_str = delimiter.join(log_entry)
    zk.create('/logs/log_', value=log_entry_str.encode(), sequence=True)
    printlog("[NEW LOG PUBLISHED] { " + log_entry_str + " }")
    # Commit entry into the database

    con = get_db_connection()
    # printlog("CONNECTING DB")
    # DELETE FROM artists_backup WHERE artistid = 1;

    try:
        with con:
            # Start a transaction

            # con.execute("BEGIN")
            beginTransactionQuery = BEGIN_TRANSACTION()
            con.execute(BEGIN_TRANSACTION)
            print("delete topic : begin transaction")

            # command = "DELETE FROM topic WHERE name = '" + str(name) + "';"
            deleteTopicQuery = DELETE_TOPIC_QUERY(name)

            cur = con.cursor()


            selectLastLogIndexQuery = SELECT_LAST_LOG_INDEX_FROM_CONFIG_QUERY(node_id)
            cur.execute(selectLastLogIndexQuery)

            last_log_index = cur.fetchone()[0]
            printlog("[LOG INDEX] : " + str(last_log_index), "YELLOW")

            cur.execute(deleteTopicQuery)
            printcommand(deleteTopicQuery)



            updateLastLogIndexQuery = UPDATE_LAST_LOG_INDEX_IN_CONFIG_TABLE_QUERY(last_log_index, node_id)
            cur.execute(updateLastLogIndexQuery)

            # Commit the transaction
            # con.execute("COMMIT")
            con.commit()
            print("delete topic : commit transaction ")
            try:
                clock = zk.Lock('locks/topics/{}'.format(name))
                clock.cancel()
                zk.delete('locks/topics/{}'.format(name))
            except NoNodeError:
                pass
            printlog("OOOOOOOOOOOO commit OOOOOOOOOOOO ")
            printlog("[LOG INDEX] : " + str(last_log_index + 1))
        return jsonify({'message': 'Topic deleted.'})
    except:
        # Rollback the transaction if there was an error
        # con.execute("ROLLBACK")
        con.rollback()
        printlog("XXXXXXXXXXXX rollback XXXXXXXXXXXX")
        return jsonify({'message': 'Error. Unable to delete topic.'}), 501
    finally:
        # printlog("CLOSING DB")
        close_db_connection(con)


def log_init():
    log_nodes = zk.get_children('/logs')
    execute_from_log(log_nodes)


def execute_from_log(event):
    event.sort()
    lock.acquire()

    if is_leader:
        return

    printlog("[READING LOG]", "BLUE")

    con = get_db_connection()

    try:
        with con:
            cur = con.cursor()

            cur.execute("SELECT last_log_index FROM config_table WHERE id = '" + str(node_id) + "';")
            last_log_index = cur.fetchone()[0]

            printlog("[LOG INDEX] : " + str(last_log_index), "YELLOW")

            # print(event[last_log_index:])

            for log in event[last_log_index:]:
                curr_log = zk.get('/logs/' + log)
                # print(curr_log)
                curr_log = curr_log[0].decode()
                commands = curr_log.split(delimiter)

                # Start a transaction
                con.execute("BEGIN")
                for command in commands:
                    # Commit entry into the database
                    cur.execute(command)
                    printcommand(command)
                cur.execute(
                    "UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(
                        node_id) + "';")
                printcommand(
                    "UPDATE config_table SET last_log_index = " + str(last_log_index + 1) + " WHERE id = '" + str(
                        node_id) + "';")
                # con.execute("COMMIT")
                con.commit()
                printlog("[COMMITTED]", "GREEN")
                last_log_index = last_log_index + 1
                printlog("[LOG INDEX] : " + str(last_log_index), "YELLOW")
    except Exception as e:

        print(e)
        con.rollback()
        printlog("[ROLLBACKED]", "RED")
    finally:

        close_db_connection(con)
    lock.release()


if __name__ == '__main__':
    db_init()
    print("############## Calling Log Initialisation ##############")
    log_init()
    ## It will monitor changes to the sub nodes(childrens) of /logs path. When children are added , deleted or moidified
    ## in this path then execute_from_log will be invoked to handle such changes
    watcher = ChildrenWatch(client=zk, path="/logs", func=execute_from_log)

    ## Same as above , it will monitor changes to the /election path
    ## Whenever we add a node then this function will be called and then new leader will be elected
    electionWatcher = ChildrenWatch(client=zk, path="/election", func=election_watcher)

    app.run(host="0.0.0.0", port=PORT, debug=True, use_debugger=False,
            use_reloader=False, passthrough_errors=True)

