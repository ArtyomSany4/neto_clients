# Создайте программу для управления клиентами на Python.

# Требуется хранить персональную информацию о клиентах:

# имя,
# фамилия,
# email,
# телефон.
# Сложность в том, что телефон у клиента может быть не один, а два, три и даже больше. А может и вообще не быть телефона, например, он не захотел его оставлять.

# Вам необходимо разработать структуру БД для хранения информации и несколько функций на Python для управления данными.

# Функция, создающая структуру БД (таблицы). Done
# Функция, позволяющая добавить нового клиента. Done
# Функция, позволяющая добавить телефон для существующего клиента. Done
# Функция, позволяющая изменить данные о клиенте.
# Функция, позволяющая удалить телефон для существующего клиента.
# Функция, позволяющая удалить существующего клиента.
# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
# Функции выше являются обязательными, но это не значит, что должны быть только они. При необходимости можете создавать дополнительные функции и классы.

# Также предоставьте код, демонстрирующий работу всех написанных функций.

# Результатом работы будет .py файл.

# Каркас кода
# import psycopg2

# def create_db(conn):
#     pass

# def add_client(conn, name, surname, email, phones=None):
#     pass

# def add_phone(conn, client_id, phone):
#     pass

# def change_client(conn, client_id, name=None, surname=None, email=None, phones=None):
#     pass

# def delete_phone(conn, client_id, phone):
#     pass

# def delete_client(conn, client_id):
#     pass

# def find_client(conn, name=None, surname=None, email=None, phone=None):
#     pass


# with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
#     pass  # вызывайте функции здесь

# conn.close()


import psycopg2
# from psycopg2 import sql
# from psycopg2.sql import Identifier
from psycopg2.sql import SQL, Identifier

# from sqlalchemy import update

conn = psycopg2.connect(
        database = 'Python_DB',
        user = 'postgres',
        password = 'PG_password'    
            )
conn.autocommit = True
        
# 0.1 Функция, создающая курсор. 
def cursor(SQL_query, params = None):
    with conn.cursor() as cur:
        cur.execute(SQL_query, params)
    return 

# 0.1.1 Курсор с фетчом. 
def cursor_f(SQL_query, params = None):
    with conn.cursor() as cur:
        cur.execute(SQL_query, params)
        result = cur.fetchone()[0]
        print('Return =', result)
    return 

# 0.2 Функция, дропающая БД .
def drop_db():
    SQL_query = """
TRUNCATE TABLE phone_numbers, clients;
DROP TABLE phone_numbers, clients; 
"""
    return cursor(SQL_query)

# Селект для отладки
def select_function(conn, name):
    SQL_query = ("""
        SELECT client_id FROM clients
        WHERE name=%s
    """)
    params = (name, )
    result = cursor_f(SQL_query, params)
    return result

# 1. Функция, создающая структуру БД (таблицы).
def create_table():
    SQL_query = """
    CREATE TABLE IF NOT EXISTS clients(
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        surname VARCHAR(255),
        email VARCHAR(255) UNIQUE
        );
    CREATE TABLE IF NOT EXISTS phone_numbers(
        phone_id SERIAL PRIMARY KEY,
        phone_number CHAR(11) UNIQUE,
        client_id INTEGER REFERENCES clients(client_id)
        ON DELETE CASCADE
        );       
    """
    cursor(SQL_query)
    return print('create_table отработала\n')


# 2. Функция, позволяющая добавить нового клиента.
def add_client(name, surname, email):
    SQL_query = """
        INSERT INTO clients(name, surname, email)   
        VALUES(%s, %s, %s)
    """
    params = (name, surname, email)
    cursor(SQL_query, params)
    return print('add_client отработала\n')

  
# 3. Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(client_id, phone_number):
    SQL_query = """
        INSERT INTO phone_numbers (client_id, phone_number)   
        VALUES(%s, %s)
    """
    params = (client_id, phone_number)
    cursor(SQL_query, params) 
    return print('add_phone отработала\n')


# 4. Функция, позволяющая изменить данные о клиенте. (По рекомендациям Булыгина)
def change_client(client_id, name=None, surname=None, email=None, phone_number=None):
    # arg_list собираем все входные данные, в т.ч. None 
    arg_list = {'name': name, 
                'surname': surname, 
                'email': email, 
                'phone_number': phone_number}

    # Бежим по arg_list
    for key, arg in arg_list.items():
        # выбираем только то, что передали при вызове функции
        if arg:
            # развилка на то, какую табличку апдейтить
            if key == 'phone_number':
                with conn.cursor() as cursor_tmp:
                    cursor_tmp.execute(SQL("UPDATE phone_numbers SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))
                conn.close
            else:
                with conn.cursor() as cursor_tmp:
                    cursor_tmp.execute(SQL("UPDATE Clients SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))
                conn.close                
    print('change_client отработала\n')


# 5. Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone_number(client_id, phone_number):
    SQL_query = """
    DELETE from phone_numbers 
    WHERE client_id=%s
    AND phone_number=%s;
    """
    params = (client_id, phone_number)
    cursor(SQL_query, params)
    return print('delete_phone_number отработала\n')


# 6. Функция, позволяющая удалить существующего клиента.
"""в create_table добавлен настройка ON DELETE CASCADE, 
чтобы при удалении не париться с форейн кей"""
def delete_client(client_id):
    SQL_query = """
    DELETE from clients
    WHERE client_id=%s;
    """
    params = (client_id, )
    cursor(SQL_query, params)
    return print('delete_client отработала\n')

# 7. Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
def find_client(name=None, surname=None, email=None, phone_number=None):
    # arg_list собираем все входные данные, в т.ч. None 
    # arg_list = {'name': name, 
    #             'surname': surname, 
    #             'email': email, 
    #             'phone_number': phone_number}

    # SQL_query = """
    # SELECT *
    # FROM clients c
    # JOIN phone_numbers pn 
    # ON c.client_id = pn.client_id
    # WHERE (name = %(name)s OR %(name)s IS NULL)
    # AND (surname = %(surname)s OR %(surname)s IS NULL)
    # AND (email = %(email)s OR %(email)s IS NULL)
    # AND (phone_number = %(phone_number)s OR %(phone_number)s IS NULL);
    # """
    with conn.cursor() as cursor_tmp:
        cursor_tmp.execute(
            """
            SELECT *
            FROM clients c
            JOIN phone_numbers pn 
            ON c.client_id = pn.client_id
            WHERE (c.name = %(name)s OR c.name IS NULL)
            AND (c.surname = %(surname)s OR c.surname IS NULL)
            AND (c.email = %(email)s OR c.email IS NULL)
            AND (pn.phone_number = %(phone_number)s OR pn.phone_number IS NULL);
            """, {'name': name, 
                    'surname': surname, 
                    'email': email, 
                    'phone_number': phone_number})
        result = cursor_tmp.fetchall()
        print(result)
    conn.close

    # print(cursor_f(SQL_query, arg_list))
    
    print('find_client отработала')
    return







# # Дропаем базу
# drop_db()

# # Проверяем все функции по порядку
# create_table()  

# add_client('John', 'Daw', '123@daw.com')
# add_client('John', 'SecondDaw', 'ksdjng@find.com')
# add_client('Second', 'Surname2', '222@daw.com')
# add_client('Third', 'Surname3', '333@daw.com')
# add_phone(4, '891111114')
# delete_phone_number(2, '891111112')
# delete_client(2)
# # find_client(conn, surname = 'John')
# change_client(3, 
#                 surname='Rasrhijvb',
#                 name='Change_Name3',
#                 phone_number='891111114')

find_client(name='Change_Name3', surname='Rasrhijvb', email='333@daw.com', phone_number='978132567  ')