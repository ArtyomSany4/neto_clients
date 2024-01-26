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
# from psycopg2 import SQL
# from psycopg2.sql import Identifier

from sqlalchemy import update

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
    return print('create_table отработала')


# 2. Функция, позволяющая добавить нового клиента.
def add_client(name, surname, email):
    SQL_query = """
        INSERT INTO clients(name, surname, email)   
        VALUES(%s, %s, %s)
    """
    params = (name, surname, email)
    cursor(SQL_query, params)
    return print('add_client отработала')

  
# 3. Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(client_id, phone_number):
    SQL_query = """
        INSERT INTO phone_numbers (client_id, phone_number)   
        VALUES(%s, %s)
    """
    params = (client_id, phone_number)
    cursor(SQL_query, params) 
    return print('add_phone отработала')


# 4. Функция, позволяющая изменить данные о клиенте. (Меняем концепцию)
def change_client(conn, client_id, name=None, surname=None, email=None, phone_number=None):
    """Сначала апдейтим таблицу clients"""
    SQL_query = """
    UPDATE clients SET 
    name=%s, 
    surname=%s, 
    email=%s 
    WHERE client_id=%s;
    """
    params = (name, surname, email, client_id)
    cursor(SQL_query, params)
    print('change_client, отработал апдейт clients')
    """Теперь апдейтим таблицу phone_numbers"""
    SQL_query = """
    UPDATE phone_numbers SET 
    phone_number=%s
    WHERE client_id=%s;
    """
    params = (phone_number, client_id)
    cursor(SQL_query, params)
    print('change_client, отработал апдейт phone_numbers')
    return print('change_client отработала')

def change_client2(client_id, name=None, surname=None, email=None, phone_number=None):
    # arg_list собираем все входные данные, в т.ч. None 
    arg_list = {'name': name, 
                'surname': surname, 
                'email': email, 
                'phone_number': phone_number}
    # values_to_upd - заготовка для сбора изменяемых данных
    values_to_upd = {}
    # Бежим по arg_list, выбираем только то, что передали при вызове функции
    for key, arg in arg_list.items():
        if arg:
            values_to_upd[key] = arg
    print(values_to_upd)
    
    from sqlalchemy import create_engine
    engine = create_engine('', future, kwargs)

    
    upd_test = update(clients).where(
        clients.c.client_id == 1).values(
            surname='Отлаживатель', 
            phone_number='260120241534',)
    print(upd_test, 'upd_test отработала')
change_client2(1, 
               surname='Отлаживатель', 
               phone_number='260120241534')



# 5. Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone_number(client_id, phone_number):
    SQL_query = """
    DELETE from phone_numbers 
    WHERE client_id=%s
    AND phone_number=%s;
    """
    params = (client_id, phone_number)
    cursor(SQL_query, params)
    return print('delete_phone_number отработала')


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
    return print('delete_client отработала')

# # 7. Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
# def find_client(cur, name=None, surname=None, email=None, phone_number=None):
#     arg_list = {'name': name, 
#                 'surname': surname, 
#                 'email': email, 
#                 'phone_number': phone_number}
#     for_ident_1 = []
#     for_ident_2 = []
#     arg_not_none = {}
#     for key, arg in arg_list.items():
#         if arg:
#             arg_not_none[key] = arg
#             for_ident_1.append(key)
#             print('for_ident_1', for_ident_1)
#             for_ident_2.append(arg)
#             print('for_ident_2', for_ident_2)
#     # print(arg_not_none)
#     # print(tuple(arg_not_none))  
#     SQL_query = sql.SQL("""
#                         SELECT * FROM clients c
#                         JOIN phone_numbers pn
#                         ON c.client_id = pn.client_id 
#                         WHERE {} 
#                         """).format(
#             sql.SQL(' = ').join(map(sql.Identifier, arg_not_none))
#             # sql.SQL(' = ').join(map(sql.Identifier, arg_not_none)), sql.Identifier('clients')
#         )
#     print(SQL_query)
#     a = cur.execute(SQL_query)
#     print(a)





# # Дропаем базу
# drop_db()
# # Проверяем все функции по порядку
# create_table()  

# add_client('John', 'Daw', '123@daw.com')
# add_client('Second', 'Surname2', '222@daw.com')
# add_client('Third', 'Surname3', '333@daw.com')
# add_phone(2, '891111112')
# change_client(conn, 1, name = 'Измененный3', phone_number = '24012023017')
# select_function(conn, 'Измененный2')
# delete_phone_number(2, '891111112')
# delete_client(2)
# find_client(conn, surname = 'John')