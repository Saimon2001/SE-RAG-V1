import requests
import json

res = requests.get("http://localhost:8080/v1/schema")
schema = res.json()

#Print all class (collections) names
for cls in schema.get("classes", []):
    print(cls["class"])
