import os
import requests
from dotenv import load_dotenv

load_dotenv()  # remove this line in production if env vars are provided by your platform

url = "https://app.loxo.co/api/projectus-consulting-ltd/people"

token = os.environ["LOXO_API_TOKEN"]

headers = {
    "accept": "application/json",
    "authorization": f"Bearer {token}",
}

data = {
    "person[name]": "Scott Dialo",
    "person[location]": "Austin, Texas",
    "person[address]": "Congress Ave",
    "person[city]": "Austin",
    "person[state]": "Texas",
    "person[country]": "USA",
    "person[company]": "Projectus Consulting",
    "person[title]": "Data Automation ",
    "person[phone]": "123456789",
    "person[linkedin_url]": "https://www.linkedin.com/in/scottdialo/",
    "person[email]": "scott@projectusconsulting.com",
}

response = requests.post(url, headers=headers, data=data, timeout=30)
response.raise_for_status()
print(response.json())