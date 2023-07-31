import psycopg2
from typing import Optional


def create_table():
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS Client_phone;
                DROP TABLE IF EXISTS Client;
                """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS Client(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(40),
                last_name VARCHAR(40),
                email VARCHAR(255)
                );
                """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Client_phone(
                id SERIAL PRIMARY KEY,
                phone_number VARCHAR(20),
                client_id INTEGER NOT NULL REFERENCES Client(id)
                );
                """)
    print('DB created.')


def insert_client(first_name: str, last_name: str, email: str) -> None:
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO Client(first_name, last_name, email)
            VALUES(%s, %s, %s) RETURNING id, first_name, last_name, email;
            """, (first_name, last_name, email))
            print(f'Добавили клиента: {cur.fetchone()}')
    return


def insert_client_phone(phone_number: str, client_id: int) -> None:
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""            
                INSERT INTO client_phone(phone_number, client_id)
                VALUES(%s, %s) RETURNING id, phone_number;
            """, (phone_number, client_id))
            print(f'Добавили телефон: {cur.fetchone()}')
    return

# Далее отстутствует пользовательский интерфейс. Я бы сделал так: через find (он ниже) нашел бы ID по ФИО и передал сюда
# Если найдено больше 1 значения в списке, спросил бы какое именно выбрать.


def update_client(client_id: int,
                  first_name: Optional[str] = None, last_name: Optional[str] = None, email: Optional[str] = None):
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            if first_name is not None:
                cur.execute("""
                UPDATE Client
                SET first_name = %s
                WHERE id = %s;""", (first_name, client_id, ))
            if last_name is not None:
                cur.execute("""
                UPDATE Client    
                SET last_name = %s
                WHERE id = %s;""", (last_name, client_id,))
            if email is not None:
                cur.execute("""
                UPDATE Client
                SET email = %s
                WHERE id = %s;
                """, (email, client_id, ))
            cur.execute("""
                SELECT * FROM Client
                WHERE id = %s;
                    """, (client_id, ))
            print(f'UPDATED! Новые данные: {cur.fetchall()}')
    return


def update_client_phone(client_id: int, phone_number: str):
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE Client_Phone
                SET phone_number = %s
                WHERE id = %s;
                """, (phone_number, client_id))
            cur.execute("""
                SELECT * FROM Client_phone
                WHERE id = %s;
                    """, (client_id, ))
            print(f'UPDATED! Новые данные: {cur.fetchall()}')
    return


def delete_all_client_phones(client_id):
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM Client_Phone
                WHERE client_id = %s;
                """, (client_id, ))
            print(f'DELETED! And a silence will be answer.')
    return


def delete_exact_phone(phone_number):
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM Client_Phone
                WHERE phone_number = %s
                RETURNING client_id;
                """, (phone_number, ))
            client_id_temp = cur.fetchone()
            cur.execute("""
                SELECT * FROM Client_phone
                WHERE client_id = %s;
                    """, (client_id_temp[0], ))
            print(f'DELETED! Новые данные: {cur.fetchall()}')
    return


def delete_client(client_id):
    delete_all_client_phones(client_id)
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM Client
                WHERE id = %s;
                """, (client_id, ))
            print(f'Client was DELETED! Need a cleaner?')
    return


def find_client(first_name: str = "%", last_name: str = "%", email: str = "%", phone_number: str = "%") -> None:
    with psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT DISTINCT cl.id FROM Client AS cl
            LEFT JOIN Client_phone AS cp ON CP.client_id = cl.id
            WHERE (cl.first_name LIKE %s OR cl.first_name IS NULL)
            AND (cl.last_name LIKE %s OR cl.last_name IS NULL)
            AND (cl.email LIKE %s OR cl.email IS NULL)
            AND (cp.phone_number LIKE %s OR cp.phone_number IS NULL)
            """, (first_name, last_name, email, phone_number))
            row = [item[0] for item in cur.fetchall()]
            row.sort()
            print(f'Найдены ID: {row}')
    return


def check_homework():
    create_table()
    insert_client('John', 'Doe', '1324134134@sdfsd')
    insert_client_phone("123123123", 1)
    update_client(1, 'John', 'Doe', '1324134134@sdfsd')
    update_client_phone(1, '+654687564654')
    delete_all_client_phones(1)
    insert_client_phone('123123123', 1)
    delete_exact_phone('123123123')
    delete_client(1)
    insert_client('John', 'Doe', '1324134134@sdfsd')
    insert_client('Jane', 'Doe', '1323134134@sdfsd')
    insert_client_phone("123123123", 2)
    find_client(last_name='Doe')
    print("Окончание ДЗ")


if __name__ == "__main__":
    check_homework()
