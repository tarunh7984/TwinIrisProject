from fastapi import FastAPI
import pymongo
from dotenv import dotenv_values
from pymongo import MongoClient
import pandas as pd
from pymongo.server_api import ServerApi

config = dotenv_values(".env")

app = FastAPI()

@app.on_event("startup")
async def startup_db_client():
    app.mongodb_client = MongoClient(config["URI"])
    app.database = app.mongodb_client[config["DB_NAME"]]
    print("Connected to the MongoDB database!!")

    # Send a ping to confirm a successful connection
    try:
        app.mongodb_client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(f"Error pinging MongoDB: {e}")

    try:
        # Read data from an Excel file into a pandas DataFrame
        data = pd.read_excel("C:/sourcecode/New folder/sample.xlsx", sheet_name="Prices")
        
        # Convert DataFrame to a list of dictionaries (records)
        json_result = data.to_dict(orient='records')
        
        # Database and collection details
        # Creating empty database and collection
        myDb = app.mongodb_client["PlantInfo"]
        myCol = myDb["PlantPrices"]
        
        # Iterate over each item in the json_result list
        for item in json_result:
            # Extract relevant fields from the item dictionary
            commodity = item.pop("Commodity")
            size = item.pop("Size")
            currency = item.pop("Currency")
            unit = item.pop("Unit")
            yearly_average = item.pop("Yearly Average")
            
            # Create a dictionary for monthly prices, excluding other fields
            monthly_prices = {month: item[month] for month in item} 
            
            # Construct the document to be inserted into MongoDB
            priceEntry = {
                commodity: {
                    "Unit": unit,
                    "Size": size,
                    "Currency": currency,
                    "MonthlyPrices": monthly_prices
                }
            }
            
            # Insert a single document into the MongoDB collection
            myCol.insert_one(priceEntry)
    except Exception as e:
        print(f"Error processing Excel data: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    app.mongodb_client.close()
