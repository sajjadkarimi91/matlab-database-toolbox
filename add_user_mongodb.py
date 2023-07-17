import pymongo

def add_user(admin_user, admin_password, host, port, new_user, new_user_password):
    client = pymongo.MongoClient(f'mongodb://{admin_user}:{admin_password}@{host}:{port}/')

    databases = client.list_database_names()
    for db in databases:
        if db in ['admin', 'config', 'local']:
            continue
        client_db = client.get_database(db)
        roles = [{'role': 'readWrite', 'db': db}]
        client_db.command('createUser', new_user, pwd=new_user_password, roles=roles)

if __name__ == "__main__":
    admin_user = 'root'
    admin_password = "alg123"
    host = "10.20.32.57"
    port = "27100"
    new_user = "milad_final"
    new_user_password = "123"
    add_user(admin_user, admin_password, host, port, new_user, new_user_password)