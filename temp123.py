# программа для управления клиентами
import psycopg2


def create_db(conn):
    """Функция, создающая структуру БД (таблицы)"""

    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS customers(
        client_id INTEGER UNIQUE PRIMARY KEY,
        first_name VARCHAR(40),
        last_name VARCHAR(60),
        email VARCHAR(60)
        );""")
    cur.execute("""CREATE TABLE IF NOT EXISTS phones(
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES customers(client_id),
        phone VARCHAR(12)
        );""")
    conn.commit()  # фиксируем в БД


def add_client(conn, client_id, first_name, last_name, email, phones=None):
    """Функция, позволяющая добавить нового клиента"""

    cur = conn.cursor()
    cur.execute("""
    INSERT INTO customers(client_id, first_name, last_name, email) VALUES(%s, %s, %s, %s);
    """, (client_id, first_name, last_name, email))
    conn.commit()
    cur.execute("""
    SELECT * FROM customers;
    """)
    print(cur.fetchall())
    cur.execute("""
    INSERT INTO phones(client_id, phone) VALUES(%s, %s);
    """, (client_id, phones))
    conn.commit()
    cur.execute("""
    SELECT * FROM phones;
    """)
    print(cur.fetchall())


def add_phone(conn, client_id, phone):
    """Функция, позволяющая добавить телефон для существующего клиента"""

    cur = conn.cursor()
    cur.execute("""
    UPDATE phones SET phone=%s WHERE client_id=%s;
    """, (phone, client_id))
    conn.commit()  # фиксируем в БД


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    """Функция, позволяющая изменить данные о клиенте"""

    cur = conn.cursor()
    cur.execute("""
    UPDATE customers SET first_name=%s, last_name=%s, email=%s WHERE client_id=%s;
    """, (first_name, last_name, email, client_id))
    cur.execute("""
    SELECT * FROM customers;
    """)
    print(cur.fetchall())
    cur.execute("""
    UPDATE phones SET phone=%s WHERE client_id=%s;
    """, (phones, client_id))
    cur.execute("""
    SELECT * FROM phones;
    """)
    print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения


def delete_phone(conn, client_id):
    """Функция, позволяющая удалить телефон для существующего клиента"""

    cur = conn.cursor()
    cur.execute("""
    UPDATE phones SET phone=%s WHERE client_id=%s;
    """, ('Null', client_id))
    cur.execute("""
    SELECT * FROM phones;
    """)
    print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения


def delete_client(conn, client_id):
    """Функция, позволяющая удалить существующего клиента"""

    cur = conn.cursor()
    cur.execute("""
    DELETE FROM phones WHERE client_id=%s;
    """, (client_id,))
    cur.execute("""
    SELECT * FROM phones;
    """)
    print(cur.fetchall())
    cur = conn.cursor()
    cur.execute("""
    DELETE FROM customers WHERE client_id=%s;
    """, (client_id,))
    cur.execute("""
    SELECT * FROM customers;
    """)
    print(cur.fetchall())  # запрос данных автоматически зафиксирует изменения


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    """Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)"""

    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM customers c JOIN phones p ON c.client_id = p.client_id WHERE first_name=%s OR last_name=%s 
    OR email=%s OR p.phone=%s;
    """, (first_name, last_name, email, phone))
    print(cur.fetchall())


with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
    create_db(conn)
    add_client(conn, 1, 'Anton', 'Naumov', 'anton.naumov@inbox.ru', '+79633045137')
    add_client(conn, 2, 'Maksim', 'Naumov', 'maksim.naumov@inbox.ru', '+79668485723')
    add_client(conn, 3, 'Maksim', 'Naumov', 'maksim.naumov@google.com')
    add_client(conn, 4, 'Sergey', 'Rodygin', 'rodygin.sergey@mail.ru')
    add_client(conn, 'Ivan', 'Ivanov', 'ivan.ivanov@mail.ru', '+7965555555')
    add_phone(conn, 3, '+79650449020')
    add_phone(conn, 4, '+79650449022')
    change_client(conn, 1, 'Artem', 'Naumov', 'anton.naumov@inbox.ru', '+79633045137')
    change_client(conn, 4, first_name='Ivan', last_name='Petrov', email='ivan.petrov@gmail.com', phones='+79650449022')
    delete_phone(conn, 1)
    delete_client(conn, 4)
    find_client(conn, first_name='Artem')
    find_client(conn, last_name='Naumov')
    find_client(conn, email='anton.naumov@inbox.ru')
    find_client(conn, phone='+79650449020')