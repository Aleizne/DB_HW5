import psycopg2


def create_table():
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
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
        conn.commit()
    print('DB created.')
    conn.close()


def insert_client(first_name: str, last_name: str, email: str) -> None:
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO Client(first_name, last_name, email)
        VALUES(%s, %s, %s) RETURNING id, first_name, last_name, email;
        """, (first_name, last_name, email))
        print(f'Добавили клиента: {cur.fetchone()}')
        conn.commit()
    conn.close()
    return


def insert_client_phone(phone_number: str, client_id: int) -> None:
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
    with conn.cursor() as cur:
        cur.execute("""            
            INSERT INTO client_phone(phone_number, client_id)
            VALUES(%s, %s) RETURNING id, phone_number;
        """, (phone_number, client_id))
        print(f'Добавили телефон: {cur.fetchone()}')
        conn.commit()
    conn.close()
    return


def update_client(client_id: int, first_name: str, last_name: str, email: str):
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE Client
            SET first_name = %s,
            last_name = %s,
            email = %s
            WHERE id = %s;
            """, (first_name, last_name, email, client_id))
        cur.execute("""
            SELECT * FROM Client
            WHERE id = %s;
                """, (client_id, ))
        print(f'UPDATED! Новые данные: {cur.fetchall()}')
        conn.commit()
    conn.close()
    return


def update_client_phone(client_id: int, phone_number: str):
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
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
        conn.commit()
    conn.close()
    return


def delete_all_client_phones(client_id):
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM Client_Phone
            WHERE client_id = %s;
            """, (client_id, ))
        print(f'DELETED! And a silence will be answer.')
        conn.commit()
    conn.close()
    return


def delete_exact_phone(phone_number):
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
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
        conn.commit()
    conn.close()
    return


def delete_client(client_id):
    delete_all_client_phones(client_id)
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM Client
            WHERE id = %s;
            """, (client_id, ))
        print(f'Client was DELETED! Need a cleaner?')
        conn.commit()
    conn.close()
    return


def find_client(first_name: str = "", last_name: str = "", email: str = "", phone_number: str = "") -> None:
    conn = psycopg2.connect(database="netology_db", user="postgres", password="LaVerdaderaDestreza")
    with conn.cursor() as cur:
        cur.execute("""
        SELECT DISTINCT cl.id FROM Client AS cl
        LEFT JOIN Client_phone AS cp ON CP.client_id = cl.id
        WHERE cl.first_name = %s
        OR cl.last_name = %s
        OR cl.email = %s
        OR cp.phone_number = %s
        """, (first_name, last_name, email, phone_number))
        row = [item[0] for item in cur.fetchall()]
        row.sort()
        print(f'Найдены ID: {row}')

    conn.close()
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
