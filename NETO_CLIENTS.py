import psycopg2
from psycopg2.sql import SQL, Identifier

conn = psycopg2.connect(
        database = 'Python_DB',
        user = 'postgres',
        password = 'PG_password'    
            )
conn.autocommit = True #стоит отказаться от этого, но долго все переписывать
        
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


# 4. Функция, позволяющая изменить данные о клиенте.
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
    arg_list = {'name': name, 
                'surname': surname, 
                'email': email, 
                'phone_number': phone_number}
    with conn.cursor() as cursor_tmp:
        cursor_tmp.execute(
            """
            SELECT *
            FROM clients c
            LEFT JOIN phone_numbers pn 
            ON c.client_id = pn.client_id
            WHERE (c.name = %(name)s OR c.name IS NULL)
            AND (c.surname = %(surname)s OR c.surname IS NULL)
            AND (c.email = %(email)s OR c.email IS NULL)
            AND (pn.phone_number = %(phone_number)s OR pn.phone_number IS NULL);
            """, arg_list)
        result = cursor_tmp.fetchall()
        print(result)
    conn.close   
    print('find_client отработала\n')
    return



# # Дропаем базу
# drop_db()

# # Проверяем все функции по порядку
create_table()  

add_client('Иван', 'Первый', '111@daw.com')
add_client('Петр', 'Второй', '222@find.com')
add_client('Сидор', 'Третий', '333@daw.com')
add_client('Игорь', 'Четвертый', '444@daw.com')
add_client('Пять', 'Пятый', '555@daw.com')

add_phone(1, '891111111')
add_phone(2, '891111112')
add_phone(3, '891111113')
add_phone(4, '891111114')

delete_phone_number(1, '891111111')

delete_client(2)

change_client(3, 
                surname='Измененный',
                name='Карл',
                phone_number='891111133')

find_client(name='Иван', 
            surname='Первый', 
            email='111@daw.com')
find_client(name='Карл', 
            surname='Измененный', 
            email='333@daw.com', 
            phone_number='891111133')

