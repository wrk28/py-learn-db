from CustomerDB import CustomerDB

if __name__ == '__main__':
    try:
        db = CustomerDB()

        # Set another schema if necessary
        db.set_schema() 

        db.create_tables()
        customer_a = db.add_customer('Adam', 'Adams', 'adam@mail.com', ['555-55-55', '111-11-11'])
        customer_b = db.add_customer('Bill', 'Williams', 'bill@mail.com', ['555-22-22'])
        phone_number_id = db.add_phone_number(customer_b, '222-55-55')
        db.update_customer(customer_a, email='adam2@mail.com', phones=['222-11-11'])
        db.update_customer(customer_a, email='adam3@mail.com')
        db.remove_customer_phone(phone_number_id)
        customers = db.search_customer_id(email='adam3@mail.com', phones=['555-22-22'])
        print(f"Result customer_id: {', '.join([str(customer) for customer in customers])}")
        db.remove_customer(customer_a)
        db.remove_customer(customer_b)
    except NameError as e:
        print('Error:', e)
        raise
    finally:
        db.close()