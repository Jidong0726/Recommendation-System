import mysql.connector
import pandas as pd



class load_table_from_MySQL(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.cnx = mysql.connector.connect(user = self.username, password = self.password, 
                                  host = '146.66.69.53', database = 'grmds054_drup881')
    
    def load_data(self, table_name):
        db_cursor = self.cnx.cursor()
        db_cursor.execute('SHOW columns FROM '+table_name)
        column_names = [column[0] for column in db_cursor.fetchall()]
        db_cursor.execute('SELECT * FROM '+table_name)
        wholetable = pd.DataFrame(columns = column_names)
        for tuples in db_cursor:
            dic = dict(zip(column_names, list(tuples)))
            wholetable = wholetable.append(dic,ignore_index = True)
        
        return wholetable
        
            
        
        
        
if __name__ == "__main__":
    username = input('Enter your username: ')
    password = input('Enter your password: ')
    x = load_table_from_MySQL(username,password)
    print ('connection complete')
    table_name = input('Enter your table name: ')
    z = x.load_data(table_name)
    z.to_csv(table_name+'.csv')