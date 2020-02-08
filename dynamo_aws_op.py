"""This file will automate following dynamo db operaions
1. create  tables  in dynamodb
2. populate the data within it
3. retrieve data from it . make CLI that where user can search for data.output format will be json
4. delete the tables created."""
from __future__ import print_function
from time import sleep
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

region_name = "us-east-1"
# access_key = ""
# secret_key = ""


# create connection with dynamodb
dynamodb = boto3.resource('dynamodb', region_name=region_name)


class DynamoOpration:

    def create_table(self):
        """Create a table in dynamo DB.You can mention the scheme below."""
        try:
            # table_name = input('insert the name of sensor table')
            table_name = "Sensor"
            self.table_1 = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'Sensor',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'SensorDescription',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Sensor',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'SensorDescription',
                        'AttributeType': 'S'
                    },

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
            print("Table status:", self.table_1.table_status)
            sleep(5)
        except Exception as e:
            print(e)

        try:
            # table_name_2 = input('insert the name of Course table')
            table_name_2 = "Courses"
            self.table_2 = dynamodb.create_table(
                TableName=table_name_2,
                KeySchema=[
                    {
                        'AttributeName': 'CourseID',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'Subject',
                        'KeyType': 'RANGE'
                    },

                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Subject',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'CourseID',
                        'AttributeType': 'N'
                    },

                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
            print("Table status:", self.table_2.table_status)
        except Exception as e:
            print(e)

    def insert_into_table(self):
        """Populate table with the json file"""
        self.table_1 = dynamodb.Table('Sensor')
        # self.table_1.put_item(
        #     Item = {"ImageFile": "/test/desc/img1.jpg",
        #     "SensorDescription": "test_desc",
        #     "SampleRate": "10", "Sensor": "121test",
        #     "Locations": "{wa,ca}"}

        #     )
        with open('file_table_1.json', 'r') as f:
            for line in f.readlines():
                details = (json.loads(line))
                details = {key: value for key, value in details.items() if value}

                self.table_1.put_item(
                    Item=details
                )

        self.table_2 = dynamodb.Table('Courses')
        with open('file_table_2.json', 'r') as f:
            for line in f.readlines():
                details = (json.loads(line))
                details = {key: value for key, value in details.items() if value}
                self.table_2.put_item(
                    Item=details
                )

    def retrieve_data(self):
        """Retrive records from the table"""
        self.table_1 = dynamodb.Table('Sensor')
        self.table_2 = dynamodb.Table('Courses')
        is_in = True
        while is_in:
            try:
                Subject = ''
                CatalogNbr = ''
                while not Subject:
                    Subject = raw_input("Enter the Subject:")
                while not CatalogNbr:
                    CatalogNbr = raw_input("Enter the CatalogNbr:")

                response = self.table_2.scan(

                    FilterExpression=Attr('CatalogNbr').eq(str(CatalogNbr)) & Attr('Subject').eq(str(Subject))

                )

                item = response['Items']
                if item:
                    item = item[0]

                if item:
                    result = "The title of {} {} is {}.".format(Subject, CatalogNbr, item['Title'])
                    print(result)
                else:
                    result = "No result for {} {} .".format(Subject, CatalogNbr)
                    print(result)

                choice = raw_input("Would you like to search for another title? (Y or N)")
                if choice.strip() == 'N':
                    is_in = False
                    break
            except Exception as e:
                print(e)

    def delete_table(self):
        """Delete table from the dynamo database"""
        self.table_1 = dynamodb.Table('Sensor')
        self.table_1.delete()
        print("Table status : sensor : ", self.table_1.table_status)
        self.table_2 = dynamodb.Table('Courses')
        self.table_2.delete()
        print("Table status course : ", self.table_2.table_status)


d1 = DynamoOpration()
# create a table
# d1.create_table()
# retrive from table
d1.retrieve_data()
# popuate table
# d1.insert_into_table()
# delete table
# d1.delete_table()
