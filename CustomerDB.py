
import os
import dotenv
import pg8000 


class CustomerDB:
    def __init__(self, path:str = None):
        if path is None:
            dotenv_path = os.path.dirname(__file__)
        else:
            dotenv_path = path
        dotenv.load_dotenv(dotenv_path)
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.database = os.getenv('DATABASE')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.default_schema = os.getenv('DEFAULT_SCHEMA')
        self.__connect()
        self.set_schema()

    def __connect(self):
        try:
            self.conn = pg8000.connect(
                host=self.host, 
                port=self.port, 
                database=self.database, 
                user=self.user, 
                password=self.password
            )
            self.cur = self.conn.cursor()
            print('Connection established')
        except pg8000.Error as e:
            if self.conn:
                self.conn.rollback()
            print('Connection error', e)
    
    def __commit(self):
        try:
            self.conn.commit()
        except pg8000 as e:
            if self.conn:
                self.conn.rollback()
            self.close()
            print('Error occured', e)
    
    def __update_cursor(self):
        if self.cur:
            self.cur.close()
        self.cur = self.conn.cursor()

    def create_tables(self):
        self.cur.execute("""
        DROP TABLE IF EXISTS phone_number CASCADE;
        """)
        self.conn.commit()

        self.cur.execute("""
        DROP TABLE IF EXISTS customer CASCADE;
        """)
        self.conn.commit()

        self.cur.execute("""
        CREATE TABLE customer(
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NULL,
            email VARCHAR(50) NOT NULL);
        """)
        self.conn.commit()

        self.cur.execute("""
        CREATE TABLE phone_number(
            phone_number_id SERIAL PRIMARY KEY,
            customer_id INT NOT NULL REFERENCES customer(customer_id),
            phone_number VARCHAR(16) NOT NULL);
        """)
        self.conn.commit()

    def add_customer(self, first_name: str, last_name: str, email: str, phones: list = []) -> int:
        self.__update_cursor()
        self.cur.execute("""
        INSERT INTO CUSTOMER(FIRST_NAME, LAST_NAME, EMAIL)
        VALUES (%s, %s, %s) RETURNING customer_id;
        """, (first_name, last_name, email,))
        customer_id = self.cur.fetchone()[0]
        self.__commit()
        for phone in phones:
            self.add_phone_number(customer_id, phone,)
        return customer_id

    def add_phone_number(self, customer_id: int, phone: str) -> int:
        self.cur.execute("""
        INSERT INTO PHONE_NUMBER(CUSTOMER_ID, PHONE_NUMBER)
        VALUES (%s, %s) RETURNING PHONE_NUMBER_ID;
        """, (customer_id, phone,))
        phone_number_id = self.cur.fetchone()[0]
        self.__commit()
        return phone_number_id
    
    def add_many_phone_numbers(self, customer_id: int, phones: list):
        for phone in phones:
            self.add_many_phone_numbers(customer_id, phone)
    
    def update_customer(self, customer_id: int, first_name:str = None, last_name:str = None, email:str = None, phones:list = None):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT first_name,last_name, email
            FROM customer
            WHERE customer_id = %s
            """, (customer_id,))
            old_first_name, old_last_name, old_email = cur.fetchone()[0:3]
            self.__commit()

            if first_name is None:
                first_name = old_first_name
            if last_name is None:
                last_name = old_last_name
            if email is None:
                email = old_email
            cur.execute("""
            UPDATE CUSTOMER
            SET FIRST_NAME = %s,
            LAST_NAME = %s,
            EMAIL = %s
            WHERE CUSTOMER_ID = %s;
            """, (first_name, last_name, email, customer_id,))
            self.__commit()

        if phones:
            self.remove_all_customer_phones(customer_id)
            for phone in phones:
                self.add_phone_number(customer_id, phone)

    def remove_all_customer_phones(self, customer_id: int):
        self.cur.execute("""
        DELETE FROM phone_number
        WHERE customer_id = %s
        """, (customer_id,))
        self.__commit()

    def remove_customer_phone(self, phone_number_id: int):
        self.cur.execute("""
        DELETE FROM phone_number
        WHERE phone_number_id = %s
        """, (phone_number_id,))
        self.__commit()

    def get_phone_number_id(self, customer_id: int, phone: str) -> int:
        self.cur.execute("""
        SELECT phone_number_id
        FROM phone_number
        WHERE customer_id = %s
        AND phone_number = %s
        """, (customer_id, phone,))
        phone_number_id = self.cur.fetchone[0]
        return phone_number_id
    
    def remove_customer(self, customer_id: int):
        self.remove_all_customer_phones(customer_id)
        self.cur.execute("""
        DELETE FROM customer
        WHERE customer_id = %s
        """, (customer_id,))
        self.__commit()

    def search_customer_id(self, first_name:str = None, last_name:str = None, email:str = None, phones:list = []) -> set:
        customers = []
        with self.conn.cursor() as cur:
            for phone in phones:
                cur.execute("""
                SELECT customer_id
                FROM phone_number
                WHERE phone_number = %s
                """, (phone,))
                result = cur.fetchall()
                customers.extend([row[0] for row in result])

            where_clauses = []
            values = []
            if first_name:
                where_clauses.append(f'first_name = %s')
                values.append(first_name)
            if last_name:
                where_clauses.append(f'last_name = %s')
                values.append(last_name)
            if email:
                where_clauses.append(f'email = %s')
                values.append(email)

            if len(where_clauses) > 0:
                query = """
                SELECT customer_id
                FROM CUSTOMER
                WHERE 
                """
                query += "AND".join(where_clauses)
                cur.execute(query, values)
                result = cur.fetchmany()
                customers.extend([row[0] for row in result])
        return set(customers)

    def set_schema(self, schema:str = None):
        if schema is None:
            schema = self.default_schema

        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = %s;
            """, (schema,))
            if cur.rowcount > 0:
                query = f'SET search_path TO {schema};'
                cur.execute(query)
                self.__commit
            else:
                raise NameError(f'No schema {schema} in the database')

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print('Connection closed')
